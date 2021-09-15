using System.ComponentModel.DataAnnotations;

namespace DataSort.GenerateFile;

internal sealed class GenerateFileOptions
{
  public const string MinValueVallidationErrorMessage = "The field {0} must be greater than {1}.";

  [Required, MinLength(1)]
  public string FilePath { get; set; } = String.Empty;

  [Required, Range(0, UInt64.MaxValue, ErrorMessage = MinValueVallidationErrorMessage)]
  public double RequiredLengthGiB { get; set; }

  public GenerationChannelOptions GenerationChannel { get; } = new();

  public FileSavingOptions FileSaving { get; } = new();
}

internal sealed class GenerationChannelOptions
{
  [Required, Range(1, Int32.MaxValue, ErrorMessage = GenerateFileOptions.MinValueVallidationErrorMessage)]
  public int Capacity { get; set; } = 128;

  [Required, Range(0, Int32.MaxValue, ErrorMessage = GenerateFileOptions.MinValueVallidationErrorMessage)]
  public int ConcurrentGenerators { get; set; } = Environment.ProcessorCount;
}

internal sealed class FileSavingOptions
{
  public string EncodingName { get; set; } = String.Empty;

  [Range(0, Int32.MaxValue, ErrorMessage = GenerateFileOptions.MinValueVallidationErrorMessage)]
  public int StreamBufferSizeMiB { get; set; }
}
