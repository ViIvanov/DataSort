using System;
using System.Linq;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Reflection.PortableExecutable;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Reflection;

namespace DataSort.GenerateFile;

internal static class App
{
  private static async Task<int> Main(string[] args) {
    //await using var stream = new FileStream(@"d:\Develop\Temp\FileSort.txt", FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 0x_100_000, FileOptions.SequentialScan | FileOptions.Asynchronous);

    //var totalItems = 0;
    var stopwatch = Stopwatch.StartNew();

    //Test();

    var deleteFilesChannel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions {
      SingleReader = true,
      SingleWriter = false,
    });

    var deleteFilesTask = Task.Run(async () => {
      await foreach(var item in deleteFilesChannel.Reader.ReadAllAsync()) {
        try {
          File.Delete(item);
        } catch(IOException ex) {
          Console.WriteLine(ex.Message);
        }//try
      }//for
    });

    //await using var reader = new RowReading(@"d:\Develop\Temp\FileSort-10G.txt", Encoding.UTF8);
    var context = new ProcessingContext {
      SourceFilePath = @"d:\Develop\Temp\FileSort.txt",
      //SourceFilePath = @"d:\Develop\Temp\FileSort-10G-release.txt",
      WorkingDirectoryPath = @"d:\Develop\Temp\Sorting\",
    };
    context.EnsureWorkingDirectory();
    Console.WriteLine($"Started operation {context.OperationId}");

    var files = await SplitFileAsync(context);

    stopwatch.Stop();
    Console.WriteLine($"Created {files.Count:N0} files at {stopwatch.Elapsed}");
    stopwatch.Start();

    var iteration = 0;
    while(files.Count > 1) {
      var stopwatchIteration = Stopwatch.StartNew();

      var mergedFiles = new List<string>();
      foreach(var chunk in files.Chunk(size: 2)) {
        if(chunk.Length is 2) {
          var mergedFile = await MergeFilesAsync(context, chunk[0], chunk[1], iteration);
          mergedFiles.Add(mergedFile);
          await deleteFilesChannel.Writer.WriteAsync(chunk[0]);
          await deleteFilesChannel.Writer.WriteAsync(chunk[1]);
        } else {
          mergedFiles.Add(chunk[0]);
        }//if
      }//for

      files = mergedFiles;

      stopwatchIteration.Stop();
      Console.WriteLine($"Iteration {iteration:N0}: {files.Count:N0} files at {stopwatchIteration.Elapsed}");
      iteration++;
    }//while

    deleteFilesChannel.Writer.Complete();
    await deleteFilesTask;

    stopwatch.Stop();
    Console.WriteLine($"Completed at {stopwatch.Elapsed}, file name is \"{files.SingleOrDefault()}\"");

    return 0;
  }

  private static async Task<string> MergeFilesAsync(ProcessingContext context, string leftFile, string rightFile, int iteration) {
    ArgumentNullException.ThrowIfNull(context);

    await using var leftStream = new FileStream(leftFile, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: 0x_100_000, FileOptions.SequentialScan | FileOptions.Asynchronous);
    using var leftReader = new StreamReader(leftStream, context.Encoding);

    await using var rightStream = new FileStream(rightFile, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: 0x_100_000, FileOptions.SequentialScan | FileOptions.Asynchronous);
    using var rightReader = new StreamReader(rightStream, context.Encoding);

    var outputFileName = context.GetNewFileName(iteration);
    await using var outputStream = new FileStream(outputFileName, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize: 0x_100_000, FileOptions.Asynchronous);
    using var outputWriter = new StreamWriter(outputStream, context.Encoding);

    var leftLine = await leftReader.ReadLineAsync();
    var rightLine = await rightReader.ReadLineAsync();

    while(leftLine is not null && rightLine is not null) {
      if(RowComparer.Compare(leftLine, rightLine) <= 0) {
        await outputWriter.WriteLineAsync(leftLine);
        leftLine = await leftReader.ReadLineAsync();
      } else {
        await outputWriter.WriteLineAsync(rightLine);
        rightLine = await rightReader.ReadLineAsync();
      }//if
    }//while

    while(leftLine is not null) {
      await outputWriter.WriteLineAsync(leftLine);
      leftLine = await leftReader.ReadLineAsync();
    }//try

    while(rightLine is not null) {
      await outputWriter.WriteLineAsync(rightLine);
      rightLine = await rightReader.ReadLineAsync();
    }//try

    return outputFileName;
  }

  private static async Task<IReadOnlyCollection<string>> SplitFileAsync(ProcessingContext context) {
    ArgumentNullException.ThrowIfNull(context);

    var data = new List<string>(100_000);
    var files = new List<string>();

    using var stream = new FileStream(context.SourceFilePath, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: 0x_100_000, FileOptions.SequentialScan | FileOptions.Asynchronous);
    using var reader = new StreamReader(stream, context.Encoding);
    while(await reader.ReadLineAsync() is { } line) {
      data.Add(line);
      if(data.Count == data.Capacity) {
        var path = context.GetNewFileName();
        files.Add(path);
        await ProcessBufferAsync(context, data, path).ConfigureAwait(continueOnCapturedContext: false);
      }//if
    }//while

    if(data.Any()) {
      var path = context.GetNewFileName();
      files.Add(path);
      await ProcessBufferAsync(context, data, path).ConfigureAwait(continueOnCapturedContext: false);
    }//if

    return files;

    static async Task ProcessBufferAsync(ProcessingContext context, List<string> data, string filePath) {
      data.Sort((left, right) => RowComparer.Compare(left, right));
      await File.WriteAllLinesAsync(filePath, data, context.Encoding, context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
      data.Clear();
    }
  }

  //static void Test() {
  //  var chars = new char[1000];
  //  var items = new List<ReadOnlyMemory<char>>();
  //  var random = Random.Shared;

  //  var index = 0;
  //  const int MinBlockSize = 3;
  //  const int MaxBlockSize = 10;
  //  while(index + MinBlockSize < chars.Length) {
  //    var length = index < chars.Length - MaxBlockSize ? random.Next(MinBlockSize, MaxBlockSize + 1) : chars.Length - index;
  //    var memory = chars.AsMemory()[index..(index + length)];
  //    var span = memory.Span;
  //    for(var charIndex = 0; charIndex < length; charIndex++) {
  //      span[charIndex] = (char)('0' + charIndex);
  //    }//for

  //    MemoryPool<char>
  //    items.Add(memory);
  //    index += length;
  //  }//while

  //  items.Sort((left, right) => left.Span.CompareTo(right.Span, StringComparison.Ordinal));
  //}
}

internal sealed class ProcessingContext
{
  public Guid OperationId { get; } = Guid.NewGuid();
  public string WorkingDirectoryPath { get; set; } = String.Empty;
  public string SourceFilePath { get; set; } = String.Empty;
  public Encoding Encoding { get; set; } = Encoding.UTF8;
  public CancellationToken CancellationToken { get; set; }

  private FileNameGeneration FileNameGeneration { get; } = new();

  public void EnsureWorkingDirectory() {
    var path = Path.GetDirectoryName(SourceFilePath);
    var workingDirectoryPath = Path.Combine(WorkingDirectoryPath ?? path!, OperationId.ToString("N"));
    Directory.CreateDirectory(workingDirectoryPath);
  }

  public string GetNewFileName(int? iteration = null) {
    var path = Path.GetDirectoryName(SourceFilePath);
    var name = Path.GetFileNameWithoutExtension(SourceFilePath);
    var extension = Path.GetExtension(SourceFilePath);
    var index = FileNameGeneration.NewFileIndex(iteration);
    return Path.Combine(WorkingDirectoryPath ?? path!, OperationId.ToString("N"), $"{name}-{index}{extension}");
  }
}

internal sealed class FileNameGeneration
{
  private ulong index;
  public string NewFileIndex(int? iteration = null) {
    var value = Interlocked.Increment(ref index);
    if(iteration is null) {
      return value.ToString();
    } else {
      return $"{iteration}-{value}";
    }//if
  }
}

//internal interface IRowProcessing
//{
//  bool Add(ReadOnlyMemory<char> item);
//  Task ProcessAsync(CancellationToken cancellationToken = default);
//  void Compeleted();
//}

//internal sealed class RowReading : IDisposable, IAsyncDisposable
//{
//  public RowReading(string fileName, Encoding encoding) {
//    Encoding = encoding ?? throw new ArgumentNullException(nameof(encoding));
//    Delimeter = Encoding.GetBytes(Environment.NewLine);
//    Stream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 0x_100_000, FileOptions.SequentialScan | FileOptions.Asynchronous);

//    var maxRowBytes = Encoding.GetByteCount(Row.MaxString);
//    Reader = PipeReader.Create(Stream, new StreamPipeReaderOptions(bufferSize: 64 * maxRowBytes, minimumReadSize: 16 * maxRowBytes));
//  }

//  public Encoding Encoding { get; }
//  private byte[] Delimeter { get; }
//  private FileStream Stream { get; }
//  private PipeReader Reader { get; }

//  public async Task ReadRows(IRowProcessing processing, CancellationToken cancellationToken = default) {
//    ArgumentNullException.ThrowIfNull(processing);

//    while(true) {
//      var result = await Reader.ReadAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
//      var buffer = result.Buffer;
//      //var position = ReadRows(buffer, result.IsCompleted, processing);

//      if(result.IsCompleted) {
//        await Reader.CompleteAsync().ConfigureAwait(continueOnCapturedContext: true);
//        break;
//      }//if

//      //Reader.AdvanceTo(position, buffer.End);
//    }//while

//    //await processing.CompeletedAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
//  }

//  private SequencePosition ReadRows(in ReadOnlySequence<byte> sequence, bool isCompleted, IRowProcessing processing) {
//    var reader = new SequenceReader<byte>(sequence);
//    while(!reader.End)
//    {
//      if(reader.TryReadTo(out ReadOnlySpan<byte> bytes, Delimeter, advancePastDelimiter: true)) {
//        var buffer = ArrayPool<char>.Shared.Rent(Row.MaxStringLength);
//        var charsWritten = Encoding.GetChars(bytes, buffer);
//        var row = new Row(buffer.AsMemory()[..charsWritten]);
//        //await processing.CompeletedAsync(buffer.AsMemory()[..charsWritten], cancellationToken);
//      } else if(isCompleted) {
//        var buffer = ArrayPool<char>.Shared.Rent(Row.MaxStringLength);
//        //RowsArrays.Add(buffer);
//        var charsWritten = Encoding.UTF8.GetChars(sequence, buffer);
//        //var row = new Row(buffer.AsMemory()[..charsWritten]);
//        //RowsData.Add(buffer.AsMemory()[..charsWritten]);
//        //itemsReturned++;
//        reader.Advance(sequence.Length);
//      } else {
//        break;
//      }//if
//    }//while

//    return reader.Position;
//  }

//  private void CleanUp() {
//    Reader.Complete();
//    //RowsArrays.ForEach(item => ArrayPool<char>.Shared.Return(item));
//    //RowsArrays.Clear();
//    //RowsData.Clear();
//  }

//  public void Dispose() {
//    CleanUp();
//    Stream.Dispose();
//  }

//  public ValueTask DisposeAsync() {
//    CleanUp();
//    return Stream.DisposeAsync();
//  }
//}
