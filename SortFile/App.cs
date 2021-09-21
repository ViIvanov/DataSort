using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Text;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataSort.SortFile;

using Common;

internal static class App
{
  private const int SuccessExitCode = 0;
  private const int InvalidArgsExitCode = -1;
  private const int InvalidConfigurationExitCode = -2;
  private const int FailedExitCode = -3;

  private static void Usage() => Console.WriteLine($"Usage: {Path.GetFileNameWithoutExtension(AppDomain.CurrentDomain.FriendlyName)} --{nameof(SortFileOptions.FilePath)} \"<input file path>\" --{nameof(SortFileOptions.MaxReadLines)} <max read lines>");

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
}
