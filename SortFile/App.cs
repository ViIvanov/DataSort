using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Text;
using DataSort.Common;

using Microsoft.Extensions.Configuration;

using Microsoft.Extensions.Logging;

namespace DataSort.SortFile;

internal static class App
{
  private const int SuccessExitCode = 0;
  private const int InvalidArgsExitCode = -1;
  private const int InvalidConfigurationExitCode = -2;
  private const int FailedExitCode = -3;

  private static void Usage() => Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(Environment.ProcessPath)} --{nameof(SortFileOptions.FilePath)} \"<input file path>\" --{nameof(SortFileOptions.MaxReadLines)} <max read lines>");

  private static async Task<int> Main(string[] args) {
    using var builder = new AppBuilder(typeof(App), args);

    var validateOptionsResult = builder.GetOptions(out var options);
    if(validateOptionsResult is not SuccessExitCode) {
      if(validateOptionsResult is InvalidArgsExitCode) {
        Usage();
      }//if

      return validateOptionsResult;
    }//if

    try {
      var operationId = Guid.NewGuid();
      var encoding = Encoding.GetEncoding(options.File.EncodingName);

      var workingDirectoryStart = String.IsNullOrEmpty(options.File.WorkingDirectory) ? Path.GetDirectoryName(options.FilePath) : options.File.WorkingDirectory;
      var workingDirectory = Path.Combine(workingDirectoryStart ?? String.Empty, operationId.ToString("N"));
      Console.WriteLine($"Sort file \"{Path.GetFileName(options.FilePath)}\" in directory \"{workingDirectory}\".");

      var stopwatch = Stopwatch.StartNew();

      var sorting = new FileSorting(options.FilePath, encoding, options.MaxReadLines, workingDirectory);
      var filePath = await sorting.SortFileAsync(builder.Logger).ConfigureAwait(continueOnCapturedContext: false);

      stopwatch.Stop();
      Console.WriteLine($"File \"{Path.GetFileName(options.FilePath)}\" sorted as \"{Path.GetFileName(filePath)}\" in {stopwatch.Elapsed}.");
    } catch(Exception ex) {
      builder.Logger.LogError(ex, "Unhandled exception");
      return FailedExitCode;
    }//try

    return SuccessExitCode;
  }

  private static int GetOptions(this AppBuilder builder, out SortFileOptions options) {
    options = builder.Configuration.Get<SortFileOptions>();

    var validationResults = new List<ValidationResult>();

    if(!Validator.TryValidateObject(options, new ValidationContext(options), validationResults, validateAllProperties: true)) {
      builder.ReportErrors("Command Line", validationResults);
      return InvalidArgsExitCode;
    } else if(!Validator.TryValidateObject(options.File, new ValidationContext(options.File), validationResults, validateAllProperties: true)) {
      builder.ReportErrors(nameof(options.File), validationResults);
      return InvalidConfigurationExitCode;
    }//if

    return SuccessExitCode;
  }

  //private static async Task<string> MergeFilesAsync(ProcessingContext context, string leftFile, string rightFile) {
  //  ArgumentNullException.ThrowIfNull(context);

  //  await using var leftStream = new FileStream(leftFile, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: 0x_100_0000, FileOptions.SequentialScan | FileOptions.Asynchronous);
  //  using var leftReader = new StreamReader(leftStream, context.Encoding);

  //  await using var rightStream = new FileStream(rightFile, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: 0x_100_0000, FileOptions.SequentialScan | FileOptions.Asynchronous);
  //  using var rightReader = new StreamReader(rightStream, context.Encoding);

  //  var outputFileName = context.GetNewFileName();
  //  //await using var outputStream = new FileStream(outputFileName, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize: 0x_10_0000, FileOptions.Asynchronous);
  //  //using var outputWriter = new StreamWriter(outputStream, context.Encoding);
  //  await using var saving = new FileSaving(outputFileName, context.MaxRowBufferSizeInBytes, context.Encoding, streamBufferSize: 0x_10_0000);
  //  await saving.WritePreambleAsync(context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);

  //  var leftLine = await leftReader.ReadLineAsync();
  //  var rightLine = await rightReader.ReadLineAsync();

  //  while(leftLine is not null && rightLine is not null) {
  //    if(DataComparer.Compare(leftLine, rightLine) <= 0) {
  //      await saving.WriteDataAsync(leftLine.AsMemory(), context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //      //      await outputWriter.WriteLineAsync(leftLine);
  //      leftLine = await leftReader.ReadLineAsync();
  //    } else {
  //      //await outputWriter.WriteLineAsync(rightLine);
  //      await saving.WriteDataAsync(rightLine.AsMemory(), context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //      rightLine = await rightReader.ReadLineAsync();
  //    }//if
  //  }//while

  //  while(leftLine is not null) {
  //    //await outputWriter.WriteLineAsync(leftLine);
  //    await saving.WriteDataAsync(leftLine.AsMemory(), context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //    leftLine = await leftReader.ReadLineAsync();
  //  }//try

  //  while(rightLine is not null) {
  //    //await outputWriter.WriteLineAsync(rightLine);
  //    await saving.WriteDataAsync(rightLine.AsMemory(), context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //    rightLine = await rightReader.ReadLineAsync();
  //  }//try

  //  return outputFileName;
  //}

  //private static async Task<string> MergeFilesAsync(ProcessingContext context, string leftFile, string rightFile) {
  //  ArgumentNullException.ThrowIfNull(context);

  //  await using var leftReading = new RowReading(leftFile, context.Encoding, readByOne: true);
  //  var leftEnumerable = leftReading.ReadByRow(context.CancellationToken);
  //  var leftEnumerator = leftEnumerable.GetAsyncEnumerator(context.CancellationToken);

  //  await using var rightReading = new RowReading(rightFile, context.Encoding, readByOne: true);
  //  var rightEnumerable = rightReading.ReadByRow(context.CancellationToken);
  //  var rightEnumerator = rightEnumerable.GetAsyncEnumerator(context.CancellationToken);

  //  var outputFileName = context.GetNewFileName();
  //  await using var saving = new FileSaving(outputFileName, context.MaxRowBufferSizeInBytes, context.Encoding, streamBufferSize: 0x_100_000);
  //  await saving.WritePreambleAsync(context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);

  //  var hasLeft = await leftEnumerator.MoveNextAsync().ConfigureAwait(continueOnCapturedContext: false);
  //  var hasRight = await rightEnumerator.MoveNextAsync().ConfigureAwait(continueOnCapturedContext: false);

  //  while(hasLeft && hasRight) {
  //    if(RowComparer.Compare(leftEnumerator.Current, rightEnumerator.Current) <= 0) {
  //      await saving.WriteDataAsync(leftEnumerator.Current.AsMemory(), context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //      hasLeft = await leftEnumerator.MoveNextAsync().ConfigureAwait(continueOnCapturedContext: false);
  //    } else {
  //      await saving.WriteDataAsync(rightEnumerator.Current.AsMemory(), context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //      hasRight = await rightEnumerator.MoveNextAsync().ConfigureAwait(continueOnCapturedContext: false);
  //    }//if
  //  }//while

  //  if(hasLeft) {
  //    do {
  //      await saving.WriteDataAsync(leftEnumerator.Current.AsMemory(), context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //    } while(await leftEnumerator.MoveNextAsync().ConfigureAwait(continueOnCapturedContext: false));
  //  }//if

  //  if(hasRight) {
  //    do {
  //      await saving.WriteDataAsync(rightEnumerator.Current.AsMemory(), context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //    } while(await rightEnumerator.MoveNextAsync().ConfigureAwait(continueOnCapturedContext: false));
  //  }//if

  //  return outputFileName;
  //}

  //private static async Task<string> MergeFilesAsync(ProcessingContext context, string leftFile, string rightFile) {
  //  ArgumentNullException.ThrowIfNull(context);

  //  await using var leftStream = new FileStream(leftFile, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: 0x_100_0000, FileOptions.SequentialScan | FileOptions.Asynchronous);
  //  using var leftReader = new StreamReader(leftStream, context.Encoding);

  //  await using var rightStream = new FileStream(rightFile, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: 0x_100_0000, FileOptions.SequentialScan | FileOptions.Asynchronous);
  //  using var rightReader = new StreamReader(rightStream, context.Encoding);

  //  var outputFileName = context.GetNewFileName();
  //  await using var outputStream = new FileStream(outputFileName, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize: 0x_10_0000, FileOptions.Asynchronous);
  //  using var outputWriter = new StreamWriter(outputStream, context.Encoding);

  //  var leftLine = await leftReader.ReadLineAsync();
  //  var rightLine = await rightReader.ReadLineAsync();

  //  while(leftLine is not null && rightLine is not null) {
  //    if(RowComparer.Compare(leftLine, rightLine) <= 0) {
  //      await outputWriter.WriteLineAsync(leftLine);
  //      leftLine = await leftReader.ReadLineAsync();
  //    } else {
  //      await outputWriter.WriteLineAsync(rightLine);
  //      rightLine = await rightReader.ReadLineAsync();
  //    }//if
  //  }//while

  //  while(leftLine is not null) {
  //    await outputWriter.WriteLineAsync(leftLine);
  //    leftLine = await leftReader.ReadLineAsync();
  //  }//try

  //  while(rightLine is not null) {
  //    await outputWriter.WriteLineAsync(rightLine);
  //    rightLine = await rightReader.ReadLineAsync();
  //  }//try

  //  return outputFileName;
  //}

  ///////////////////////////////////////////////
  //private static async Task<(int FileCount, int RowCount)> SplitFileAsync(ProcessingContext context, int chunkSize, ChannelWriter<string> writer) {
  //  ArgumentNullException.ThrowIfNull(context);
  //  ArgumentNullException.ThrowIfNull(writer);

  //  var maxRowBufferSizeInBytes = context.Encoding.GetByteCount(DataDescription.MaxString);
  //  var (fileCount, rowCount) = (0, 0);

  //  await using var reading = new DataReading(context.SourceFilePath, context.Encoding);
  //  await foreach(var item in reading.ReadLinesAsync(chunkSize, context.CancellationToken)) {
  //    item.Sort((left, right) => DataComparer.Compare(left, right));

  //    var path = context.GetNewFileName();
  //    await using(var saving = new FileSaving(path, maxRowBufferSizeInBytes, context.Encoding, streamBufferSize: 0x_10_0000)) {
  //      await saving.WritePreambleAsync(context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //      foreach(var row in item) {
  //        await saving.WriteDataAsync(row.AsMemory(), context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //      }//for
  //    }//using

  //    Console.WriteLine($"Saved file {Path.GetFileNameWithoutExtension(path)}");
  //    await writer.WriteAsync(path, context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);

  //    fileCount++;
  //    rowCount += item.Count;
  //  }//for

  //  return (fileCount, rowCount);
  //}
  ///////////////////////////////////////////////

  //private static async Task<(int FileCount, int RowCount)> SplitFileAsync(ProcessingContext context, ChannelWriter<string> writer) {
  //  ArgumentNullException.ThrowIfNull(context);
  //  ArgumentNullException.ThrowIfNull(writer);

  //  var data = new List<string>(1_000_000);
  //  var (fileCount, rowCount) = (0, 0);

  //  using var stream = new FileStream(context.SourceFilePath, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: 0x_100_000, FileOptions.SequentialScan | FileOptions.Asynchronous);
  //  using var reader = new StreamReader(stream, context.Encoding);
  //  while(await reader.ReadLineAsync() is { } line) {
  //    //data.Add(line);
  //    //if(data.Count == data.Capacity) {
  //    //  //(fileCount, rowCount) = (fileCount + 1, rowCount + await SaveDataAsync(context, data, writer));
  //    //  await SaveData2Async();
  //    //}//if
  //  }//while

  //  //if(data.Any()) {
  //  //  //(fileCount, rowCount) = (fileCount + 1, rowCount + await SaveDataAsync(context, data, writer));
  //  //  await SaveData2Async();
  //  //}//if

  //  return (fileCount, rowCount);

  //  //async Task SaveData2Async() {
  //  //  data.Sort((left, right) => RowComparer.Compare(left, right));

  //  //  var path = context.GetNewFileName();
  //  //  await File.WriteAllLinesAsync(path, data, context.Encoding, context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //  //  await writer.WriteAsync(path, context.CancellationToken);

  //  //  fileCount++;
  //  //  rowCount += data.Count;
  //  //  data.Clear();
  //  //}

  //  //async Task SaveData2Async() {
  //  //  data.Sort((left, right) => RowComparer.Compare(left, right));

  //  //  var path = context.GetNewFileName();
  //  //  await File.WriteAllLinesAsync(path, data, context.Encoding, context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //  //  await writer.WriteAsync(path, context.CancellationToken);

  //  //  fileCount++;
  //  //  rowCount += data.Count;
  //  //  data.Clear();
  //  //}

  //  //static async Task<int> SaveDataAsync(ProcessingContext context, List<string> data, ChannelWriter<string> writer) {
  //  //  data.Sort((left, right) => RowComparer.Compare(left, right));

  //  //  var path = context.GetNewFileName();
  //  //  await File.WriteAllLinesAsync(path, data, context.Encoding, context.CancellationToken).ConfigureAwait(continueOnCapturedContext: false);
  //  //  await writer.WriteAsync(path, context.CancellationToken);

  //  //  var rowCount = data.Count;
  //  //  data.Clear();
  //  //  return rowCount;
  //  //}
  //}

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

//internal sealed class ProcessingContext
//{
//  private ulong fileNameIndex;

//  public Guid OperationId { get; } = Guid.NewGuid();
//  public string WorkingDirectoryPath { get; set; } = String.Empty;
//  public string SourceFilePath { get; set; } = String.Empty;
//  public Encoding Encoding { get; set; } = Encoding.UTF8;
//  public int MaxRowBufferSizeInBytes { get; set; } = Encoding.UTF8.GetByteCount(Row.MaxString);
//  public CancellationToken CancellationToken { get; set; }

//  public void EnsureWorkingDirectory() {
//    var path = Path.GetDirectoryName(SourceFilePath);
//    var workingDirectoryPath = Path.Combine(WorkingDirectoryPath ?? path!, OperationId.ToString("N"));
//    Directory.CreateDirectory(workingDirectoryPath);
//  }

//  public string GetNewFileName() {
//    var path = Path.GetDirectoryName(SourceFilePath);
//    var name = Path.GetFileNameWithoutExtension(SourceFilePath);
//    var extension = Path.GetExtension(SourceFilePath);
//    var index = Interlocked.Increment(ref fileNameIndex);
//    return Path.Combine(WorkingDirectoryPath ?? path!, OperationId.ToString("N"), $"{name}-{index}{extension}");
//  }
//}
