namespace DataSort.SortFile;

using Common;

internal sealed class DataComparer : Comparer<ReadOnlyMemory<char>>
{
  public static new DataComparer Default { get; } = new DataComparer();

  public override int Compare(ReadOnlyMemory<char> x, ReadOnlyMemory<char> y) => Compare(x.Span, y.Span);

  public static int Compare(ReadOnlySpan<char> x, ReadOnlySpan<char> y) {
    var xtext = GetText(x, out var xdelimeter);
    var ytext = GetText(y, out var ydelimeter);
    var compareText = xtext.CompareTo(ytext, StringComparison.Ordinal);
    if(compareText is not 0) {
      return compareText;
    }//if

    var (xnumber, ynumber) = (GetNumber(x, xdelimeter), GetNumber(y, ydelimeter));
    return xnumber.CompareTo(ynumber);
  }

  private static ReadOnlySpan<char> GetText(ReadOnlySpan<char> value, out int delimeter) {
    delimeter = value.IndexOf(DataDescription.TextSeparator, StringComparison.Ordinal);
    if(delimeter <= 0) {
      throw new ArgumentException($"Number separator \"{DataDescription.TextSeparator}\" not found in \"{value}\".", nameof(value));
    }//if

    return value[(delimeter + DataDescription.TextSeparator.Length)..];
  }

  private static ulong GetNumber(ReadOnlySpan<char> value, int delimeter) => UInt64.Parse(value[..delimeter]);
}
