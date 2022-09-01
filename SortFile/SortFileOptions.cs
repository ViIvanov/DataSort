using System.ComponentModel.DataAnnotations;

namespace DataSort.SortFile;

internal sealed class SortFileOptions
{
  public const string MinValueVallidationErrorMessage = "The field {0} must be greater than {1}.";

  [Required, MinLength(1)]
  public string FilePath { get; set; } = String.Empty;

  [Required, Range(0, Int32.MaxValue, ErrorMessage = MinValueVallidationErrorMessage)]
  public int MaxReadLines { get; set; }

  public SortFileConfigurationOptions File { get; } = new();
}

internal sealed class SortFileConfigurationOptions
{
  public string WorkingDirectory { get; set; } = String.Empty;

  [Required]
  public string EncodingName { get; set; } = String.Empty;

  [Range(0, Int32.MaxValue, ErrorMessage = SortFileOptions.MinValueVallidationErrorMessage)]
  public int ReadingStreamBufferSizeKiB { get; set; }

  [Range(1, Int32.MaxValue, ErrorMessage = SortFileOptions.MinValueVallidationErrorMessage)]
  public int ReadingPipeBufferSizeFactor { get; set; }

  [Range(1, Int32.MaxValue, ErrorMessage = SortFileOptions.MinValueVallidationErrorMessage)]
  public int ReadingPipeMinimumReadSizeFactor { get; set; }

  [Range(0, Int32.MaxValue, ErrorMessage = SortFileOptions.MinValueVallidationErrorMessage)]
  public int ReadingBufferCount { get; set; }

  [Range(0, Int32.MaxValue, ErrorMessage = SortFileOptions.MinValueVallidationErrorMessage)]
  public int SavingFileBufferSizeKiB { get; set; }

  [Range(0, Int32.MaxValue, ErrorMessage = SortFileOptions.MinValueVallidationErrorMessage)]
  public int MergeFileReadBufferSizeKiB { get; set; }

  [Range(0, Int32.MaxValue, ErrorMessage = SortFileOptions.MinValueVallidationErrorMessage)]
  public int MergeFileWriteBufferSizeKiB { get; set; }
}
