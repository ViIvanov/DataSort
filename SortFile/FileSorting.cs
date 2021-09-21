using System.Diagnostics;
using System.Text;
using System.Threading.Channels;

using Microsoft.Extensions.Logging;

namespace DataSort.SortFile;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

using Common;

using Microsoft.VisualBasic;

internal sealed class FileSorting
{
  public FileSorting(string filePath, Encoding encoding, int sortChunkSize, string? workingDirectory = null) {
    if(sortChunkSize <= 0) {
      throw new ArgumentNullException(nameof(sortChunkSize));
    }//if

    SourceFileName = filePath;
    Encoding = encoding ?? throw new ArgumentNullException(nameof(encoding));
    SortChunkSize = sortChunkSize;

    MaxLineBufferSizeInBytes = Encoding.GetByteCount(DataDescription.MaxString);

    FileNameGeneration = FileNameGeneration.Create(SourceFileName, workingDirectory);
    if(!String.IsNullOrEmpty(FileNameGeneration.DirectoryPath) && !Directory.Exists(FileNameGeneration.DirectoryPath)) {
      Directory.CreateDirectory(FileNameGeneration.DirectoryPath);
    }//if
  }

  public string SourceFileName { get; }
  public Encoding Encoding { get; }
  public int SortChunkSize { get; }
  private int MaxLineBufferSizeInBytes { get; }
  private FileNameGeneration FileNameGeneration { get; }

  public async Task<string> SortFileAsync(ILogger logger, CancellationToken cancellationToken = default) {
    var pairingChannel = Channel.CreateUnbounded<(string FilePath, int Iteration)>(new UnboundedChannelOptions {
      SingleReader = true,
      SingleWriter = false,
    });

    var mergeChannel = Channel.CreateUnbounded<(string LeftFilePath, string RightFilePath, int Iteration)>(new UnboundedChannelOptions {
      SingleReader = false,
      SingleWriter = true,
    });

    var deleteFilesChannel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions {
      SingleReader = true,
      SingleWriter = false,
    });

    var mergeTask = Tasks.WhenAll(count: 16, async _ => {
      await foreach(var (leftFile, rightFile, iteration) in mergeChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false)) {
        var stopwatch = Stopwatch.StartNew();
        var filePath = await MergeFilesAsync(leftFile, rightFile, cancellationToken);
        stopwatch.Stop();

        ReportMerge(leftFile, rightFile, filePath, iteration, stopwatch.Elapsed);

        await pairingChannel.Writer.WriteAsync((filePath, iteration + 1), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        await deleteFilesChannel.Writer.WriteAsync(leftFile, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        await deleteFilesChannel.Writer.WriteAsync(rightFile, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
      }//for

      void ReportMerge(string left, string right, string result, int iteration, TimeSpan elapsed) {
        var leftInfo = new FileInfo(left);
        var rightInfo = new FileInfo(right);
        var resultInfo = new FileInfo(result);

        logger.LogInformation($"Merge {GetFileName(leftInfo)} [{leftInfo.Length:N0}] and {GetFileName(rightInfo)} [{rightInfo.Length:N0}] => [{iteration}] {GetFileName(resultInfo)} [{resultInfo.Length:N0}] in {elapsed}");

        static string? GetFileName(FileInfo info) => Path.GetFileNameWithoutExtension(info?.Name);
      }
    }, cancellationToken);

    var pairingTask = Task.Run(async () => {
      var pairing = new FilePairing();
      await foreach(var (filePath, iteration) in pairingChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false)) {
        if(String.IsNullOrEmpty(filePath)) {
          // Last file from a "split" operation was added to the channel and now we are know how many files should be processed on each iteration.
          // "iteration" contains number of files after the "split" operation.
          if(pairing.SetItemCountOnFirstIteration(iteration, out var pair)) {
            await mergeChannel.Writer.WriteAsync(pair, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
          }//if
        } else {
          if(pairing.TryFindPair(iteration, filePath, out var leftFile)) {
            await mergeChannel.Writer.WriteAsync((leftFile, filePath, iteration), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
          }//if
        }//if
      }//for

      mergeChannel.Writer.Complete();
      return pairing.GetSingleFile();
    }, cancellationToken);

    var totalProcessingFiles = 0;
    var deleteFilesTask = Task.Run(async () => {
      var deletedFiles = 0;
      await foreach(var filePath in deleteFilesChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false)) {
        deletedFiles++;
        try {
          File.Delete(filePath);
        } catch(IOException ex) {
          logger.LogError(ex, $"Error deleting file \"{filePath}\".");
        }//try

        var processingFiles = Volatile.Read(ref totalProcessingFiles);

        // The result, sorted file, stored in the last file. This file will not be deleted.
        if(processingFiles > 0 && deletedFiles == processingFiles - 1) {
          pairingChannel.Writer.Complete();
        }//if
      }//for
    }, cancellationToken);

    var stopwatch = Stopwatch.StartNew();
    var (fileCount, lineCount) = await SplitFileAsync(logger, pairingChannel.Writer, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    stopwatch.Stop();

    await pairingChannel.Writer.WriteAsync((FilePath: String.Empty, Iteration: fileCount), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    //pairingChannel.Writer.Complete();

    //Volatile.Write(ref firstIterationFiles, value: fileCount);
    // We are merging two files at the time, so during sorting we will process (2 * N - 1) files where N is initial file count after splitting source file
    Volatile.Write(ref totalProcessingFiles, value: fileCount * 2 - 1);

    logger.LogInformation($"Read {fileCount:N0} files / {lineCount:N0} lines in {stopwatch.Elapsed}");

    //if(fileCount == 1) {
    //  pairingChannel.Writer.Complete();
    //}//if

    var resultFilePath = await pairingTask.ConfigureAwait(continueOnCapturedContext: false);
    await mergeTask.ConfigureAwait(continueOnCapturedContext: false);

    deleteFilesChannel.Writer.Complete();
    await deleteFilesTask.ConfigureAwait(continueOnCapturedContext: false);

    return resultFilePath;
  }

  private async Task<(int FileCount, int LineCount)> SplitFileAsync(ILogger logger, ChannelWriter<(string FilePath, int Iteration)> writer, CancellationToken cancellationToken = default) {
    if(writer is null) {
      throw new ArgumentNullException(nameof(writer));
    }//if

    var (fileCount, lineCount) = (0, 0);

    await using var reading = new DataReading(SourceFileName, Encoding);
    await foreach(var lines in reading.ReadLinesAsync(SortChunkSize, cancellationToken).ConfigureAwait(continueOnCapturedContext: false)) {
      lines.Sort((left, right) => DataComparer.Compare(left, right));

      var path = FileNameGeneration.GetNewFilePath();
      await using(var saving = new FileSaving(path, MaxLineBufferSizeInBytes, Encoding, streamBufferSize: 0x_10_0000)) {
        foreach(var line in lines) {
          await saving.WriteDataAsync(line.AsMemory(), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        }//for
      }//using

      logger.LogInformation($"Saved file {Path.GetFileNameWithoutExtension(path)}");
      await writer.WriteAsync((path, Iteration: 0), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

      fileCount++;
      lineCount += lines.Count;
    }//for

    return (fileCount, lineCount);
  }

  private async Task<string> MergeFilesAsync(string leftFile, string rightFile, CancellationToken cancellationToken = default) {
    const int ReadBufferSize = 0x_100_0000;
    const int WriteBufferSize = 0x_10_0000;

    await using var leftStream = new FileStream(leftFile, FileMode.Open, FileAccess.Read, FileShare.None, ReadBufferSize, FileOptions.SequentialScan | FileOptions.Asynchronous);
    using var leftReader = new StreamReader(leftStream, Encoding);

    await using var rightStream = new FileStream(rightFile, FileMode.Open, FileAccess.Read, FileShare.None, ReadBufferSize, FileOptions.SequentialScan | FileOptions.Asynchronous);
    using var rightReader = new StreamReader(rightStream, Encoding);

    var outputFileName = FileNameGeneration.GetNewFilePath();
    await using var saving = new FileSaving(outputFileName, MaxLineBufferSizeInBytes, Encoding, WriteBufferSize);

    var leftLine = await leftReader.ReadLineAsync();
    var rightLine = await rightReader.ReadLineAsync();

    while(leftLine is not null && rightLine is not null) {
      if(DataComparer.Compare(leftLine, rightLine) <= 0) {
        await saving.WriteDataAsync(leftLine.AsMemory(), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        leftLine = await leftReader.ReadLineAsync().ConfigureAwait(continueOnCapturedContext: false);
      } else {
        await saving.WriteDataAsync(rightLine.AsMemory(), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        rightLine = await rightReader.ReadLineAsync().ConfigureAwait(continueOnCapturedContext: false);
      }//if
    }//while

    while(leftLine is not null) {
      await saving.WriteDataAsync(leftLine.AsMemory(), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
      leftLine = await leftReader.ReadLineAsync().ConfigureAwait(continueOnCapturedContext: false);
    }//try

    while(rightLine is not null) {
      await saving.WriteDataAsync(rightLine.AsMemory(), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
      rightLine = await rightReader.ReadLineAsync().ConfigureAwait(continueOnCapturedContext: false);
    }//try

    return outputFileName;
  }

  private sealed class FilePairing
  {
    private SortedDictionary<int, List<string>> Items { get; } = new();
    private int ItemCountOnFirstIteration { get; set; } = 0;

    public bool TryFindPair(int iteration, string value, [MaybeNullWhen(returnValue: false)] out string otherValue) {
      if(Items.TryGetValue(iteration, out var items) && items.Any()) {
        otherValue = RetriveExistingItem(items);
        return true;
      } else if(ItemCountOnFirstIteration > 0) {
        // Try to join new value with existing value on other iteration
        foreach(var key in Items.Keys) {
          var list = Items[key];
          if(list.Any()) {
            otherValue = RetriveExistingItem(list);
            return true;
          }//if
        }//for
      }//if

      if(items is null) {
        items = new List<string>();
        Items.Add(iteration, items);
      }//if

      items.Add(value);

      otherValue = default;
      return false;

      static string RetriveExistingItem(List<string> list) {
        var existingItem = list[0];
        list.RemoveAt(0);
        return existingItem;
      }
    }

    public bool SetItemCountOnFirstIteration(int value, out (string, string, int) pair) {
      if(value <= 0) {
        throw new ArgumentOutOfRangeException(nameof(value), value, message: null);
      } else if(ItemCountOnFirstIteration > 0) {
        throw new InvalidOperationException($"{nameof(ItemCountOnFirstIteration)} already initialized.");
      }//if

      ItemCountOnFirstIteration = value;

      var left = default(string);
      foreach(var key in Items.Keys) {
        var list = Items[key];
        if(list.Any()) {
          if(left is null) {
            left = list[0];
            list.RemoveAt(0);
          } else {
            pair = (left, list[0], key);
            list.RemoveAt(0);
            return true;
          }//if
        }//if
      }//for

      pair = default;
      return false;
    }

    public string GetSingleFile() => Items.Single(item => item.Value.Any()).Value.Single();
  }
}
