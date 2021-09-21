using System.Diagnostics;
using System.Text;
using System.Threading.Channels;

using Microsoft.Extensions.Logging;

namespace DataSort.SortFile;

using Common;

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
      var fileByIteration = new Dictionary<int, List<string>>();
      await foreach(var (filePath, iteration) in pairingChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false)) {
        if(fileByIteration.TryGetValue(iteration, out var files) && files.Any()) {
          var lastFile = files.Last();
          files.RemoveAt(files.Count - 1);
          await mergeChannel.Writer.WriteAsync((lastFile, filePath, iteration), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

          if(!files.Any() && fileByIteration.TryGetValue(iteration - 1, out var previousIteration)) {
            files.AddRange(previousIteration);
          }//if
        } else {
          if(files is null) {
            files = new List<string>();
            fileByIteration.Add(iteration, files);
          }//if

          files.Add(filePath);
        }//if
      }//for

      mergeChannel.Writer.Complete();
      return fileByIteration.Single(item => item.Value.Any()).Value.Single();
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

    // We are merging two files at the time, so during sorting we will process (2 * N - 1) files where N is initial file count after splitting source file
    Volatile.Write(ref totalProcessingFiles, value: fileCount * 2 - 1);

    logger.LogInformation($"Read {fileCount:N0} files / {lineCount:N0} lines in {stopwatch.Elapsed}");

    if(fileCount == 1) {
      pairingChannel.Writer.Complete();
    }//if

    var resultFilePath = await pairingTask.ConfigureAwait(continueOnCapturedContext: false);
    await mergeTask.ConfigureAwait(continueOnCapturedContext: false);

    deleteFilesChannel.Writer.Complete();
    await deleteFilesTask.ConfigureAwait(continueOnCapturedContext: false);

    return resultFilePath;
  }

  private async Task<(int FileCount, int LineCount)> SplitFileAsync(ILogger logger, ChannelWriter<(string FilePath, int Iteration)> writer, CancellationToken cancellationToken = default) {
    ArgumentNullException.ThrowIfNull(writer);

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
}
