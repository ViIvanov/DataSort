#if DEBUG
using System.Diagnostics;
#endif // DEBUG

namespace DataSort.Common;

public static class DataDescription
{
  public const string TextCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  public const string TextSeparator = ". "; // Dot and Space

  public const char TextSeparatorFirstCharacter = '.'; // Dot
  public const char TextSeparatorSecondCharacter = ' '; // Space
  public const int TextSeparatorLength = 2; // ". ": Dot and Space

  // Limining the Text part
#if DEBUG
  public const int TextMinLength = 4;
  public const int TextMaxLength = 16;
# else
  public const int TextMinLength = 16;
  public const int TextMaxLength = 1024;
#endif // DEBUG

  private static readonly string MaxStringCore = $"{UInt64.MaxValue}. " + new string('Z', count: TextMaxLength);

#if DEBUG
  public const char LastTextCharacter = '|'; // This character should not present in AllTextCharacters
  public static readonly string MaxString = MaxStringCore + LastTextCharacter;
#else
  public static readonly string MaxString = MaxStringCore;
#endif // DEBUG

#if DEBUG
  static DataDescription() {
    Debug.Assert(!TextCharacters.Contains(LastTextCharacter), $"{nameof(TextCharacters)} should not contain {nameof(LastTextCharacter)}.");
  }
#endif // DEBUG
}
