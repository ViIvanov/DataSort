using System;
using System.Collections.Generic;

namespace DataSort.GenerateFile;

internal sealed class RowComparer : Comparer<ReadOnlyMemory<char>>
{
  public static new RowComparer Default { get; } = new RowComparer();

  public override int Compare(ReadOnlyMemory<char> x, ReadOnlyMemory<char> y) => Compare(x.Span, y.Span);

  public static int Compare(ReadOnlySpan<char> x, ReadOnlySpan<char> y) {
    var xtext = GetText(x, out var xdelimeter);
    var ytext = GetText(y, out var ydelimeter);
    var compareText = xtext.CompareTo(ytext, StringComparison.Ordinal);
    if(compareText != 0) {
      return compareText;
    }//if

    var (xnumber, ynumber) = (GetNumber(x, xdelimeter), GetNumber(y, ydelimeter));
    return xnumber.CompareTo(ynumber);
  }

  private const char NumberSeparator = '.';
  private const int NumberSeparatorLength = 2; // ". ": NumberSeparator and Space

  private static ReadOnlySpan<char> GetText(ReadOnlySpan<char> value, out int delimeter) {
    delimeter = value.IndexOf(NumberSeparator);
    if(delimeter <= 0) {
      throw new ArgumentException($"Number separator \"{NumberSeparator}\" not found in \"{value}\".", nameof(value));
    }//if

    return value[(delimeter + NumberSeparatorLength)..];
  }

  private static ulong GetNumber(ReadOnlySpan<char> value, int delimeter) => UInt64.Parse(value[..delimeter]);
}