using System.Buffers;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Text;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataSort.GenerateFile;

using Common;

internal static class App
{
  private const int SuccessExitCode = 0;
  private const int InvalidArgsExitCode = -1;
  private const int InvalidConfigurationExitCode = -2;
  private const int FailedExitCode = -3;
  
  private static void Usage() => Console.WriteLine($"Usage: {AppDomain.CurrentDomain.FriendlyName} --{nameof(GenerateFileOptions.FilePath)} \"<output file path>\" --{nameof(GenerateFileOptions.RequiredLengthGiB)} <required length in GiB>");

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
      var directoryPath = Path.GetDirectoryName(options.FilePath);
      if(!String.IsNullOrEmpty(directoryPath) && !Directory.Exists(directoryPath)) {
        Directory.CreateDirectory(directoryPath);
      }//if

      await GenerateFileAsync(builder.Logger, options).ConfigureAwait(continueOnCapturedContext: false);
    } catch(Exception ex) {
      builder.Logger.LogError(ex, "Unhandled exception");
      return FailedExitCode;
    }//try

    return SuccessExitCode;
  }

  private static int GetOptions(this AppBuilder builder, out GenerateFileOptions options) {
    options = builder.Configuration.Get<GenerateFileOptions>();

    var validationResults = new List<ValidationResult>();

    if(!Validator.TryValidateObject(options, new ValidationContext(options), validationResults, validateAllProperties: true)) {
      builder.ReportErrors("Command Line", validationResults);
      return InvalidArgsExitCode;
    } else if(!Validator.TryValidateObject(options.GenerationChannel, new ValidationContext(options.GenerationChannel), validationResults, validateAllProperties: true)) {
      builder.ReportErrors(nameof(options.GenerationChannel), validationResults);
      return InvalidConfigurationExitCode;
    } else if(!Validator.TryValidateObject(options.FileSaving, new ValidationContext(options.FileSaving), validationResults, validateAllProperties: true)) {
      builder.ReportErrors(nameof(options.FileSaving), validationResults);
      return InvalidConfigurationExitCode;
    }//if

    return SuccessExitCode;
  }

  private static async Task GenerateFileAsync(ILogger logger, GenerateFileOptions options) {
    ArgumentNullException.ThrowIfNull(options);

    var encoding = Encoding.GetEncoding(options.FileSaving.EncodingName);
    var maxBufferSize = encoding.GetByteCount(DataDescription.MaxString);
    var requiredLength = (long)(options.RequiredLengthGiB * 1024 * 1024 * 1024);
    var streamBufferSize = options.FileSaving.StreamBufferSizeMiB > 0 ? options.FileSaving.StreamBufferSizeMiB * 1024 * 1024 : default(int?);

    logger.LogInformation($"Writing {requiredLength:N0} bytes to \"{options.FilePath}\".");
    var stopwatch = Stopwatch.StartNew();

    var generation = new DataGenerationChannel(options.GenerationChannel.Capacity, options.GenerationChannel.ConcurrentGenerators);
    await using var saving = new FileSaving(options.FilePath, maxBufferSize, encoding, streamBufferSize, requiredLength);
    var bytesWritten = 0L;

    // Save some data to add into file sometimes, to be able handle the case with sorting the same data
    var (savedData, savedDataLength) = (ArrayPool<char>.Shared.Rent(maxBufferSize), 0);
    try {
      var progress = 0;
      while(bytesWritten < requiredLength) {
        var data = await generation.NextAsync().ConfigureAwait(continueOnCapturedContext: false);

#if DEBUG
        Debug.Assert(data.Span[^1] is DataDescription.LastTextCharacter, $"Latest character is not \"{DataDescription.LastTextCharacter}\"", $"Data is {data}");
        Debug.Assert(data.Span.IndexOf(DataDescription.LastTextCharacter) is var index && index == data.Span.Length - 1,
          $"{nameof(DataDescription.LastTextCharacter)} should be latest character in the string, but occured at index {index}.", $"Data is {data}");
#endif // DEBUG

        bytesWritten += await saving.WriteDataAsync(data).ConfigureAwait(continueOnCapturedContext: false);
        if(PrintProgress(bytesWritten, requiredLength, ref progress)) {
          // Add saved data to the file when progress value changed
          if(savedDataLength > 0) {
            bytesWritten += await saving.WriteDataAsync(savedData.AsMemory()[..savedDataLength]).ConfigureAwait(continueOnCapturedContext: false);
          }//if

          data.CopyTo(savedData);
          savedDataLength = data.Length;
        }//if
      }//while
    } finally {
      ArrayPool<char>.Shared.Return(savedData);
    }//try

    await saving.DisposeAsync().ConfigureAwait(continueOnCapturedContext: false);
    await generation.CompleteAsync().ConfigureAwait(continueOnCapturedContext: true);

    stopwatch.Stop();
    logger.LogInformation($"{bytesWritten:N0} bytes written in {stopwatch.Elapsed}.");
  }

  private static bool PrintProgress(long currentValue, long maxValue, ref int progress) {
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
