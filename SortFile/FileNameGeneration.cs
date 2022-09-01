namespace DataSort.SortFile;

internal sealed class FileNameGeneration
{
  private ulong index;

  public FileNameGeneration(string directoryPath, string sourceFileName, string extension) {
    DirectoryPath = directoryPath ?? throw new ArgumentNullException(nameof(directoryPath));
    SourceFileName = sourceFileName ?? throw new ArgumentNullException(nameof(sourceFileName));
    Extension = extension ?? throw new ArgumentNullException(nameof(extension));
  }

  public string DirectoryPath { get; }
  public string SourceFileName { get; }
  public string Extension { get; }

  public static FileNameGeneration Create(string sourceFilePath, string? workingDirectory = null) {
    var directoryPath = String.IsNullOrEmpty(workingDirectory) ? Path.GetDirectoryName(sourceFilePath) : workingDirectory;
    var sourceFileName = Path.GetFileNameWithoutExtension(sourceFilePath);
    var extension = Path.GetExtension(sourceFilePath);
    return new(directoryPath ?? String.Empty, sourceFileName, extension);
  }

  public string GetNewFilePath() {
    var fileNameIndex = Interlocked.Increment(ref index);
    var fileName = $"{SourceFileName}-{fileNameIndex}{Extension}";
    return Path.Combine(DirectoryPath, fileName);
  }
}
