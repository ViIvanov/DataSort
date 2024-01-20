using System.Diagnostics;
using System.Text;
using System.Threading.Channels;

using System.Collections.Concurrent;

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
    var (mergeOptions, lineCount) = await SplitFileAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    stopwatchSplit.Stop();
    Console.WriteLine($"Split to {mergeOptions.Files.Count:N0} files / {lineCount:N0} lines in {stopwatchSplit.Elapsed}");

    var stopwatchMerge = Stopwatch.StartNew();
    var filePath = await MergeFilesAsync(mergeOptions, deleteFilesChannel, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    stopwatchMerge.Stop();
    Console.WriteLine($"Merge {mergeOptions.Files.Count:N0} files in {stopwatchMerge.Elapsed}");

    deleteFilesChannel.Writer.Complete();
    await deleteFilesTask.ConfigureAwait(continueOnCapturedContext: false);

    return filePath;
  }

  private async Task<(MergeOptions Options, int LineCount)> SplitFileAsync(CancellationToken cancellationToken = default) {
    var files = new List<string>();
    var lineCount = 0;

    var sourceFileLength = GetFileLength(SourceFilePath);
    var currentLength = 0L;

    var stopwatchReadLines = new Stopwatch();
    using var stopwatchSort = new ThreadLocal<Stopwatch>(() => new Stopwatch(), trackAllValues: true);
    using var stopwatchSaving = new ThreadLocal<Stopwatch>(() => new Stopwatch(), trackAllValues: true);
    using var stopwatchReturnBuffer = new ThreadLocal<Stopwatch>(() => new Stopwatch(), trackAllValues: true);

    await using var reading = new DataReading(SourceFilePath, Encoding, Configuration);
    var savingTasks = new List<Task>();

    stopwatchReadLines.Start();
    await foreach(var lines in reading.ReadLinesAsync(SortChunkSize, cancellationToken).ConfigureAwait(continueOnCapturedContext: false)) {
      stopwatchReadLines.Stop();

      // Will sort and save temporary file while read the next portion of data.
      //savingTasks.Add(Task.Run(() => SaveDataAsync(lines, cancellationToken), cancellationToken));
      savingTasks.Add(SaveDataAsync(lines, cancellationToken));
      savingTasks.RemoveAll(item => item.IsCompleted);

      stopwatchReadLines.Start();
    }//for
    stopwatchReadLines.Stop();

    var stopwatchWaitTasks = new Stopwatch();
    if(savingTasks.Any()) {
      savingTasks.RemoveAll(item => item.IsCompleted);

      stopwatchWaitTasks.Start();
      await Task.WhenAll(savingTasks).ConfigureAwait(continueOnCapturedContext: false);
      stopwatchWaitTasks.Stop();
    }//if

    async Task SaveDataAsync(List<string> lines, CancellationToken cancellationToken = default) {
      stopwatchSort.Start();
      lines.Sort((left, right) => DataComparer.Compare(left, right));
      stopwatchSort.Stop();

      var filePath = FileNameGeneration.GetNewFilePath();
      //var fileLength = 0;
      stopwatchSaving.Start();
      await using(var saving = new FileSaving(filePath, MaxLineBufferSizeInBytes, Encoding, streamBufferSize: Configuration.SavingFileBufferSizeKiB * 1024)) {
        foreach(var line in lines) {
          //fileLength += Encoding.GetByteCount(line);
          await saving.WriteDataAsync(line.AsMemory(), cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        }//for
      }//using
      stopwatchSaving.Stop();

      Interlocked.Add(ref lineCount, lines.Count);
      stopwatchReturnBuffer.Start();
      await reading.ReturnBufferAsync(lines, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
      stopwatchReturnBuffer.Stop();

      files.Add(filePath);

      var fileLength = GetFileLength(filePath);
      currentLength += fileLength;
      var currentProgress = currentLength < sourceFileLength ? (int)((double)currentLength / sourceFileLength * 100) : 100;
      Console.WriteLine($"Saved [{currentProgress,3:N0}%] \"{Path.GetFileNameWithoutExtension(filePath)}\" [{fileLength:N0}].");
    }

    Console.WriteLine($"Read lines:\t{stopwatchReadLines.Elapsed}");
    Console.WriteLine($"Sort file:\t{stopwatchSort.Elapsed()} (avg: {new TimeSpan(stopwatchSort.ElapsedTicks() / files.Count)})");
    Console.WriteLine($"Write data:\t{stopwatchSaving.Elapsed()} (avg: {new TimeSpan(stopwatchSaving.ElapsedTicks() / files.Count)})");
    Console.WriteLine($"Return buffer:\t{stopwatchReturnBuffer.Elapsed()} (avg: {new TimeSpan(stopwatchReturnBuffer.ElapsedTicks() / files.Count)})");
    Console.WriteLine($"Wait tasks:\t{stopwatchWaitTasks.Elapsed}");

    return (new(files, reading.HasEncodingPreamble ?? false, reading.HasFinalNewLine ?? false), lineCount);
  }

  private async Task<string> MergeFilesAsync(MergeOptions mergeOptions, ChannelWriter<string> deleteFilesChannelWriter, CancellationToken cancellationToken = default) {
    ArgumentNullException.ThrowIfNull(deleteFilesChannelWriter);

    //var stopwatchGC = new Stopwatch();
    var itemBag = await OpenFilesAsync(mergeOptions.Files, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    try {
      var items = itemBag.ToList();

      // Sorting items in reversed order, to have minimal (first) item at the tail of the list.
      // After that, the last item will be removed instead of first item.
      // Hope, it can reduce movements of items in the list.
      items.Sort(MergeFileItem.ReverseComparer);

      var outputFileLength = GetFileLength(SourceFilePath); // Preallocate space for output file.
      var outputFileName = FileNameGeneration.GetNewFilePath();
      var writeBufferSize = Configuration.MergeFileWriteBufferSizeKiB * 1024;
      await using var saving = new FileSaving(outputFileName, MaxLineBufferSizeInBytes, Encoding, mergeOptions.WriteEncodingPreamble, requiredLength: outputFileLength);

      var stopwatchWriteStart = new Stopwatch();
      var stopwatchWriteAwait = new Stopwatch();
      var stopwatchReadNext = new Stopwatch();
      var stopwatchSearchInsert = new Stopwatch();
      var stopwatchDelete = new Stopwatch();

      var (currentLength, progress) = (0L, 0);
      while(items.Count > 0) {
        var lastItemIndex = items.Count - 1;
        var item = items[lastItemIndex];
        items.RemoveAt(lastItemIndex);

        stopwatchWriteStart.Start();
        var currentLine = item.CurrentLine.AsMemory();
        //var writeDataTask = Task.Run(() => saving.WriteDataAsync(currentLine, cancellationToken), cancellationToken);
        var writeDataTask = saving.WriteDataAsync(currentLine, cancellationToken);
        stopwatchWriteStart.Stop();

        stopwatchReadNext.Start();
        var found = await item.ReadNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        stopwatchReadNext.Stop();
        if(found) {
          stopwatchSearchInsert.Start();
          var index = items.BinarySearch(item, MergeFileItem.ReverseComparer);
          items.Insert(index: index >= 0 ? index : ~index, item);
          stopwatchSearchInsert.Stop();

          await WriteDataAsync(writeDataTask).ConfigureAwait(continueOnCapturedContext: false);
        } else {
          await WriteDataAsync(writeDataTask).ConfigureAwait(continueOnCapturedContext: false);

          await item.DisposeAsync().ConfigureAwait(continueOnCapturedContext: false);
          await deleteFilesChannelWriter.WriteAsync(item.FilePath, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        }//if
      }//while

      if(mergeOptions.WriteFinalNewLine) {
        currentLength += await saving.WriteNewLineAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        PrintProgress(currentLength, outputFileLength, ref progress);
      }//if

      async Task WriteDataAsync(Task<long> task) {
        stopwatchWriteAwait.Start();
        currentLength += await task.ConfigureAwait(continueOnCapturedContext: false);
        if(PrintProgress(currentLength, outputFileLength, ref progress)) {
          //RunGC(stopwatchGC);
        }//if
        stopwatchWriteAwait.Stop();
      }

      Console.WriteLine($"Write Start:\t{stopwatchWriteStart.Elapsed}");
      Console.WriteLine($"Write Await:\t{stopwatchWriteAwait.Elapsed}");
      Console.WriteLine($"Read Next:\t{stopwatchReadNext.Elapsed}");
      Console.WriteLine($"Search/Insert:\t{stopwatchSearchInsert.Elapsed}");
      //Console.WriteLine($"GC takes:\t{stopwatchGC.Elapsed}");

      return outputFileName;
    } finally {
      foreach(var item in itemBag) {
        await item.DisposeAsync().ConfigureAwait(continueOnCapturedContext: false);
      }//for
    }//try

    //static void RunGC(Stopwatch? stopwatch = default) {
    //  stopwatch?.Start();
    //  GC.Collect();
    //  GC.WaitForPendingFinalizers();
    //  GC.Collect();
    //  stopwatch?.Stop();
    //}

    async Task<IReadOnlyCollection<MergeFileItem>> OpenFilesAsync(IReadOnlyCollection<string> files, CancellationToken cancellationToken = default) {
      var stopwatch = Stopwatch.StartNew();
      var items = new ConcurrentBag<MergeFileItem>();

      var bufferSize = Configuration.MergeFileReadBufferSizeKiB * 1024;
      await Parallel.ForEachAsync(files, cancellationToken, async (filePath, token) => {
        var item = await MergeFileItem.OpenAsync(filePath, Encoding, bufferSize, token).ConfigureAwait(continueOnCapturedContext: false);
        if(item is not null) {
          items.Add(item);
          //RunGC(stopwatchGC);
        }//if
      }).ConfigureAwait(continueOnCapturedContext: false);

      stopwatch.Stop();
      Console.WriteLine($"Open files:\t{stopwatch.Elapsed} ({items.Count:N0})");
      return items;
    }

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

  private readonly struct MergeOptions(IReadOnlyList<string> files, bool writeEncodingPreamble, bool writeFinalNewLine)
  {
    public IReadOnlyList<string> Files { get; } = files ?? [];
    public bool WriteEncodingPreamble => writeEncodingPreamble;
    public bool WriteFinalNewLine => writeFinalNewLine;
  }

  private sealed class MergeFileItem : IDisposable, IAsyncDisposable
  {
    private MergeFileItem(string filePath, Encoding encoding, int bufferSize) {
      FilePath = filePath;

      try {
        Stream = new(FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.SequentialScan | FileOptions.Asynchronous);
        Reader = new(Stream, encoding, leaveOpen: true);
      } catch {
        Dispose();
        throw;
      }//try
    }

    public static Comparer<MergeFileItem> ReverseComparer { get; } = new MergeFileItemReverseComparer();

    private int TextDelimeterIndex { get; set; }
    public string? CurrentLine { get; private set; }

    public string FilePath { get; }

    private FileStream Stream { get; }
    private StreamReader Reader { get; }

    public static async Task<MergeFileItem?> OpenAsync(string filePath, Encoding encoding, int bufferSize, CancellationToken cancellationToken = default) {
      var item = new MergeFileItem(filePath, encoding, bufferSize);
      try {
        var hasValue = await item.ReadNextAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
        if(!hasValue /* || String.IsNullOrEmpty(item.CurrentLine) */) {
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

    public async Task<bool> ReadNextAsync(CancellationToken cancellationToken = default) {
      if((CurrentLine = await Reader.ReadLineAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false)) is not null) {
        TextDelimeterIndex = DataComparer.FindDelimiterIndex(CurrentLine.AsSpan());
        return true;
      } else {
        TextDelimeterIndex = -1;
        return false;
      }//if
    }

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
          return DataComparer.Compare(y.CurrentLine, y.TextDelimeterIndex, x.CurrentLine, x.TextDelimeterIndex);
        }//if
      }
    }
  }
}
