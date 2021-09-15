using System;
using System.Diagnostics.CodeAnalysis;

namespace DataSort.GenerateFile;

internal readonly struct Row : IEquatable<Row>, IComparable<Row>
{
  private const char NumberSeparator = '.';
  private const int NumberSeparatorLength = 2; // ". ": NumberSeparator and Space

  private const int TextMaxLength = 1024;
  public static readonly string MaxString = $"{UInt64.MaxValue}. " + new string('Z', count: TextMaxLength) + "|";
  public static readonly int MaxStringLength = MaxString.Length;

  public Row(ReadOnlyMemory<char> data) {
    var textStart = data.Span.IndexOf(NumberSeparator);
    if(textStart <= 0) {
      throw new ArgumentException($"Number separator \"{NumberSeparator}\" not found in data \"{data}\".", nameof(data));
    }//if

    if(!UInt64.TryParse(data.Span[..textStart], out var number)) {
      throw new ArgumentException($"Could not parse number \"{data.Span[..textStart]}\".", nameof(data));
    }//if

    Data = data;
    TextStart = textStart;
    Number = number;
  }

  private ReadOnlyMemory<char> Data { get; }
  private int TextStart { get; }

  public ulong Number { get; }
  public ReadOnlySpan<char> Text => Data.Span[(TextStart + NumberSeparatorLength)..];

  public override bool Equals([NotNullWhen(true)] object? obj) => base.Equals(obj);
  public override int GetHashCode() => Number.GetHashCode();

  public override string ToString() => Data.ToString();

  public bool Equals(Row other) => Data.Equals(other.Data);
  public int CompareTo(Row other) {
    var compareText = Text.CompareTo(other.Text, StringComparison.Ordinal);
    if(compareText != 0) {
      return compareText;
    } else {
      return Number.CompareTo(other.Number);
    }//if
  }

  public static bool operator ==(Row left, Row right) => left.Equals(right);
  public static bool operator !=(Row left, Row right) => !(left == right);

  public static bool operator <(Row left, Row right) => left.CompareTo(right) < 0;
  public static bool operator <=(Row left, Row right) => left.CompareTo(right) <= 0;
  public static bool operator >(Row left, Row right) => left.CompareTo(right) > 0;
  public static bool operator >=(Row left, Row right) => left.CompareTo(right) >= 0;
}
