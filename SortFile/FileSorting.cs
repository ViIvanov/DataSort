using System.Diagnostics;
using System.Text;
using System.Threading.Channels;

namespace DataSort.SortFile;

using Common;

internal sealed class FileSorting
{
  public FileSorting(SortFileOptions configuration) {
    ArgumentNullException.ThrowIfNull(configuration);

    Configuration = configuration.File;
    SourceFilePath = configuration.FilePath;
    Encoding = Encoding.GetEncoding(configuration.File.EncodingName);
    SortChunkSize = configuration.MaxReadLines;

    MaxLineBufferSizeInBytes = Encoding.GetByteCount(DataDescription.MaxString);

    FileNameGeneration = FileNameGeneration.Create(SourceFilePath, Configuration.WorkingDirectory);
  }

  public SortFileConfigurationOptions Configuration { get; }
  public string SourceFilePath { get; }
  public Encoding Encoding { get; }
  public int SortChunkSize { get; }
  private int MaxLineBufferSizeInBytes { get; }
  private FileNameGeneration FileNameGeneration { get; }

  private static long GetFileLength(string filePath) => new FileInfo(filePath).Length;

  public async Task<string> SortFileAsync(CancellationToken cancellationToken = default) {
    var deleteFilesChannel = Channel.CreateUnbounded<string>(new() { SingleReader = true, SingleWriter = false, });

    var deleteFilesTask = Task.Run(async () => {
      await foreach(var filePath in deleteFilesChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false)) {
        try {
          File.Delete(filePath);
        } catch(SystemException ex) {
          // Not a critical error
          Console.WriteLine($"Error deleting file \"{filePath}\": {ex}.");
        }//try
      }//for
    }, cancellationToken);

    var stopwatchSplit = Stopwatch.StartNew();
    var (files, lineCount) = await SplitFileAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    stopwatchSplit.Stop();
    Console.WriteLine($"Split to {files.Count:N0} files / {lineCount:N0} lines in {stopwatchSplit.Elapsed}");

    var stopwatchMerge = Stopwatch.StartNew();
    var filePath = await MergeFilesAsync(files, deleteFilesChannel, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    stopwatchMerge.Stop();
    Console.WriteLine($"Merge {files.Count:N0} files in {stopwatchMerge.Elapsed}");

    deleteFilesChannel.Writer.Complete();
    await deleteFilesTask.ConfigureAwait(continueOnCapturedContext: false);

    return filePath;
  }

  private async Task<(IReadOnlyList<string> Files, int LineCount)> SplitFileAsync(CancellationToken cancellationToken = default) {
    var files = new List<string>();
    var lineCount = 0;

    var sourceFileLength = GetFileLength(SourceFilePath);
    var currentLength = 0L;

    await using var reading = new DataReading(SourceFilePath, Encoding, Configuration);
    var savingTasks = new List<Task>();
    await foreach(var lines in reading.ReadLinesAsync(SortChunkSize, cancellationToken).ConfigureAwait(continueOnCapturedContext: false)) {
      // Will sort and save temporary file while read the next portion of data.
      savingTasks.Add(Task.Run(() => SaveDataAsync(lines), cancellationToken));
      savingTasks.RemoveAll(item => item.IsCompleted);
    }//for

    if(savingTasks.Any()) {
      savingTasks.RemoveAll(item => item.IsCompleted);
      await Task.WhenAll(savingTasks).ConfigureAwait(continueOnCapturedContext: false);
    }//if

    async Task SaveDataAsync(List<string> lines) {
      lines.Sort((left, right) => DataComparer.Compare(left, right));

      var filePath = FileNameGeneration.GetNewFilePath();
      await using(var saving = new FileSaving(filePath, MaxLineBufferSizeInBytes, Encoding, streamBufferSize: Configuration.SavingFileBufferSizeKiB * 1024)) {
        foreach(var line in lines) {
          await saving.WriteDataAsync(line.AsMemory(), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        }//for
      }//using

      Interlocked.Add(ref lineCount, lines.Count);
      await reading.ReturnBufferAsync(lines, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

      files.Add(filePath);

      var fileLength = GetFileLength(filePath);
      currentLength += fileLength;
      var currentProgress = currentLength < sourceFileLength ? (int)((double)currentLength / sourceFileLength * 100) : 100;
      Console.WriteLine($"Saved [{currentProgress,3:N0}%] \"{Path.GetFileNameWithoutExtension(filePath)}\" [{fileLength:N0}].");
    }

    return (files, lineCount);
  }

  private async Task<string> MergeFilesAsync(IReadOnlyCollection<string> files, ChannelWriter<string> deleteFilesChannelWriter, CancellationToken cancellationToken = default) {
    ArgumentNullException.ThrowIfNull(files);
    ArgumentNullException.ThrowIfNull(deleteFilesChannelWriter);

    var items = new List<MergeFileItem>(files.Count);
    try {
      var bufferSize = Configuration.MergeFileReadBufferSizeKiB * 1024;
      foreach(var filePath in files) {
        var item = await MergeFileItem.OpenAsync(filePath, Encoding, bufferSize).ConfigureAwait(continueOnCapturedContext: false);
        if(item is not null) {
          items.Add(item);
        }//if
      }//for

      // Sorting items in reversed order, to have minimal (first) item at the tail of the list.
      // After that, the last item will be removed instead of first item.
      // Hope, it can reduce movements of items in the list.
      items.Sort(MergeFileItem.ReverseComparer);

      var outputFileLength = GetFileLength(SourceFilePath); // Preallocate space for output file.
      var outputFileName = FileNameGeneration.GetNewFilePath();
      var writeBufferSize = Configuration.MergeFileWriteBufferSizeKiB * 1024;
      await using var saving = new FileSaving(outputFileName, MaxLineBufferSizeInBytes, Encoding, writeBufferSize, requiredLength: outputFileLength);

      var (currentLength, progress) = (0L, 0);
      while(items.Count > 0) {
        var lastItemIndex = items.Count - 1;
        var item = items[lastItemIndex];
        items.RemoveAt(lastItemIndex);
        currentLength += await saving.WriteDataAsync(item.CurrentLine.AsMemory(), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        PrintProgress(currentLength, outputFileLength, ref progress);

        if(await item.ReadNext().ConfigureAwait(continueOnCapturedContext: false)) {
          var index = items.BinarySearch(item, MergeFileItem.ReverseComparer);
          items.Insert(index: index >= 0 ? index : ~index, item);
        } else {
          await item.DisposeAsync().ConfigureAwait(continueOnCapturedContext: false);
          await deleteFilesChannelWriter.WriteAsync(item.FilePath, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        }//if
      }//while

      return outputFileName;
    } finally {
      foreach(var item in items) {
        await item.DisposeAsync().ConfigureAwait(continueOnCapturedContext: false);
      }//for
    }//try

    static bool PrintProgress(long currentValue, long maxValue, ref int progress) {
      var currentProgress = currentValue < maxValue ? (int)((double)currentValue / maxValue * 100) : 100;
      if(currentProgress > progress) {
        var text = currentProgress % 10 is 0 ? $"{currentProgress,3}%{Environment.NewLine}" : ".";
        Console.Write(text);
        progress = currentProgress;
        return true;
      } else {
        return false;
      }//if
    }
  }

  private sealed class MergeFileItem : IDisposable, IAsyncDisposable
  {
    private MergeFileItem(string filePath, Encoding encoding, int bufferSize) {
      FilePath = filePath;

      try {
        Stream = new(FilePath, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize, FileOptions.SequentialScan | FileOptions.Asynchronous);
        Reader = new(Stream, encoding);
      } catch {
        Dispose();
        throw;
      }//try
    }

    public static Comparer<MergeFileItem> ReverseComparer { get; } = new MergeFileItemReverseComparer();

    public string? CurrentLine { get; private set; }

    public string FilePath { get; }

    private FileStream Stream { get; }
    private StreamReader Reader { get; }

    public static async Task<MergeFileItem?> OpenAsync(string filePath, Encoding encoding, int bufferSize) {
      var item = new MergeFileItem(filePath, encoding, bufferSize);
      try {
        var hasValue = await item.ReadNext().ConfigureAwait(continueOnCapturedContext: false);
        if(!hasValue) {
          await item.DisposeAsync().ConfigureAwait(continueOnCapturedContext: false);
          return null;
        } else {
          return item;
        }//if
      } catch {
        await item.DisposeAsync().ConfigureAwait(continueOnCapturedContext: false);
        throw;
      }//try
    }

    public override string ToString() => $"[{Path.GetFileNameWithoutExtension(FilePath)}]: {CurrentLine}";

    public async Task<bool> ReadNext() => (CurrentLine = await Reader.ReadLineAsync().ConfigureAwait(continueOnCapturedContext: false)) is not null;

    public ValueTask DisposeAsync() {
      Reader?.Dispose();
      return Stream?.DisposeAsync() ?? default;
    }

    public void Dispose() {
      Reader?.Dispose();
      Stream?.Dispose();
    }

    private sealed class MergeFileItemReverseComparer : Comparer<MergeFileItem>
    {
      public override int Compare(MergeFileItem? x, MergeFileItem? y) {
        if(x is null) {
          return y is null ? 0 : 1;
        } else if(y is null) {
          return -1;
        } else {
          return DataComparer.Compare(y.CurrentLine, x.CurrentLine);
        }//if
      }
    }
  }
}
