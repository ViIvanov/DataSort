using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;

namespace DataSort.GenerateFile;

internal sealed class DataGeneration : IDisposable
{
  private const string TextCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  // Limining the Text part
#if DEBUG
  private const int TextMinLength = 4;
  private const int TextMaxLength = 16;
# else
  private const int TextMinLength = 16;
  private const int TextMaxLength = 1024;
#endif // DEBUG

  private static readonly string MaxStringCore = $"{UInt64.MaxValue}. " + new string('Z', count: TextMaxLength);

#if DEBUG
  public const char LastTextCharacter = '|'; // This character should not present in AllTextCharacters
  public static readonly string MaxString = MaxStringCore + LastTextCharacter;
#else
  public static readonly string MaxString = MaxStringCore;
#endif // DEBUG

#if DEBUG
  static DataGeneration() {
    Debug.Assert(!TextCharacters.Contains(LastTextCharacter), $"{nameof(TextCharacters)} should not contain {nameof(LastTextCharacter)}.");
  }
#endif // DEBUG

  public DataGeneration(int seed, int buffersCount) {
    if(buffersCount <= 0) {
      throw new ArgumentOutOfRangeException(nameof(buffersCount), buffersCount, message: null);
    }//if

    Random = new(seed);

    var buffers = new List<char[]>(buffersCount);
    for(var index = 0; index < buffersCount; index++) {
      var buffer = ArrayPool<char>.Shared.Rent(MaxString.Length);
      buffers.Add(buffer);
    }//for
    Buffers = buffers;
  }

  private Random Random { get; }
  private List<char[]> Buffers { get; }

  public ReadOnlyMemory<char> NextData(int bufferIndex) {
    if(bufferIndex < 0 || bufferIndex >= Buffers.Count) {
      throw new ArgumentOutOfRangeException(nameof(bufferIndex), bufferIndex, message: null);
    }//if

    var buffer = Buffers[bufferIndex];
    var charactersWritten = NextData(Random, buffer);
    return buffer.AsMemory()[0..charactersWritten];
  }

  private static int NextData(Random random, Span<char> destination) {
    var number = unchecked((ulong)random.NextInt64(Int64.MinValue, Int64.MaxValue));
    var formatted = number.TryFormat(destination, out var charactersWritten);
    Debug.Assert(formatted, $"String representation of the Number {number:N0} is greater, than expected and not feat into buffer with length {destination.Length:N0}.");

    destination[charactersWritten++] = '.';
    destination[charactersWritten++] = ' ';

    var textLength = random.Next(TextMinLength, TextMaxLength + 1);
    for(var index = 0; index < textLength; index++) {
      var letterIndex = random.Next(0, TextCharacters.Length);
      destination[charactersWritten++] = TextCharacters[letterIndex];
    }//for

#if DEBUG
    // Write end-text marker to check correctnes of writing and reading data
    destination[charactersWritten++] = LastTextCharacter;
#endif // DEBUG

    return charactersWritten;
  }

  public void Dispose() {
    for(var index = 0; index < Buffers.Count; index++) {
      ArrayPool<char>.Shared.Return(Buffers[index]);
    }//for
    Buffers.Clear();
  }
}
