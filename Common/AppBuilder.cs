using System.ComponentModel.DataAnnotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataSort.Common;

public sealed class AppBuilder : IDisposable
{
  public AppBuilder(Type appType, string[] args) {
    ArgumentNullException.ThrowIfNull(appType);

    Configuration = GetConfiguration(args);
    LogFactory = CreateLoggerFactory(Configuration);
    Logger = LogFactory.CreateLogger(appType);
  }

  public IConfiguration Configuration { get; }
  public ILoggerFactory LogFactory { get; }
  public ILogger Logger { get; }

  private static ILoggerFactory CreateLoggerFactory(IConfiguration? configuration) => LoggerFactory.Create(builder =>
    builder
      .SetMinimumLevel(LogLevel.Trace)
      .AddConfiguration(configuration)
      .AddSimpleConsole(options => options.SingleLine = true)
  );

  private static IConfiguration GetConfiguration(string[] args) => new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("AppSettings.json", optional: false, reloadOnChange: false)
    .AddCommandLine(args)
    .Build();

  public void ReportErrors(string section, IEnumerable<ValidationResult> validationResults) {
    foreach(var item in validationResults ?? Array.Empty<ValidationResult>()) {
      Logger.LogError($"Validation error [{section}]: {{Error}}", item);
    }//for
  }

  public void Dispose() => LogFactory.Dispose();
}
