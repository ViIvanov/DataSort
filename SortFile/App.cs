using System.ComponentModel.DataAnnotations;
using System.Diagnostics;

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

      var workingDirectory = String.IsNullOrEmpty(options.File.WorkingDirectory) ? Path.GetDirectoryName(options.FilePath) : options.File.WorkingDirectory;
      options.File.WorkingDirectory = Path.Combine(workingDirectory ?? String.Empty, operationId.ToString("N"));
      if(!Directory.Exists(options.File.WorkingDirectory)) {
        Directory.CreateDirectory(options.File.WorkingDirectory);
      }//if

      Console.WriteLine($"Sort file \"{Path.GetFileName(options.FilePath)}\" in directory \"{options.File.WorkingDirectory}\".");
      if(!Debugger.IsAttached) {
        Console.Write($"Start memory monitor for process #{Environment.ProcessId} and press <Enter>");
        Console.ReadLine();
      }//if

      var stopwatch = Stopwatch.StartNew();

      var sorting = new FileSorting(options);
      var filePath = await sorting.SortFileAsync().ConfigureAwait(continueOnCapturedContext: false);

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

    if(!Validator.TryValidateObject(options, new(options), validationResults, validateAllProperties: true)) {
      builder.ReportErrors("Command Line", validationResults);
      return InvalidArgsExitCode;
    } else if(!Validator.TryValidateObject(options.File, new(options.File), validationResults, validateAllProperties: true)) {
      builder.ReportErrors(nameof(options.File), validationResults);
      return InvalidConfigurationExitCode;
    }//if

    return SuccessExitCode;
  }
}
