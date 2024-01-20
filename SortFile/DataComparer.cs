namespace DataSort.SortFile;

using Common;

internal sealed class DataComparer : Comparer<ReadOnlyMemory<char>>
{
  private const StringComparison StringComparisonValue = StringComparison.Ordinal;

  public static new DataComparer Default { get; } = new();

  public override int Compare(ReadOnlyMemory<char> x, ReadOnlyMemory<char> y) => Compare(x.Span, y.Span);

  public static int Compare(in ReadOnlySpan<char> x, in ReadOnlySpan<char> y) {
    var (xindex, yindex) = (FindDelimiterIndex(x), FindDelimiterIndex(y));
    return Compare(x, xindex, y, yindex);
  }

  public static int Compare(in ReadOnlySpan<char> x, int xDelimiterIndex, in ReadOnlySpan<char> y, int yDelimiterIndex) {
    var xtext = GetText(x, xDelimiterIndex);
    var ytext = GetText(y, yDelimiterIndex);
    var compareText = xtext.CompareTo(ytext, StringComparisonValue);
    if(compareText is not 0) {
      return compareText;
    }//if

    var (xnumber, ynumber) = (GetNumber(x, xDelimiterIndex), GetNumber(y, yDelimiterIndex));
    return xnumber.CompareTo(ynumber);
  }

  public static int FindDelimiterIndex(in ReadOnlySpan<char> value) {
    var index = value.IndexOf(DataDescription.TextSeparator, StringComparisonValue);
    if(index <= 0) {
      throw new ArgumentException($"Number separator \"{DataDescription.TextSeparator}\" not found in \"{value}\".", nameof(value));
    }//if
    return index;
  }

  private static ulong GetNumber(in ReadOnlySpan<char> value, int delimeter) => UInt64.Parse(value[..delimeter]);
  private static ReadOnlySpan<char> GetText(in ReadOnlySpan<char> value, int delimeter) => value[(delimeter + DataDescription.TextSeparator.Length)..];
}
