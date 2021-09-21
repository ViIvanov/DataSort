using System.Buffers;
using System.Diagnostics;

namespace DataSort.GenerateFile;

using Common;

internal sealed class DataGeneration : IDisposable
{
  public DataGeneration(int seed, int buffersCount) {
    if(buffersCount <= 0) {
      throw new ArgumentOutOfRangeException(nameof(buffersCount), buffersCount, message: null);
    }//if

    Random = new(seed);

    var buffers = new List<char[]>(buffersCount);
    for(var index = 0; index < buffersCount; index++) {
      var buffer = ArrayPool<char>.Shared.Rent(DataDescription.MaxString.Length);
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
    var number = GetNextUInt64(random);
    var formatted = number.TryFormat(destination, out var charactersWritten);
    Debug.Assert(formatted, $"String representation of the Number {number:N0} is greater, than expected and not feat into buffer with length {destination.Length:N0}.");

    foreach(var separator in DataDescription.TextSeparator) {
      destination[charactersWritten++] = separator;
    }//for

    var textLength = random.Next(DataDescription.TextMinLength, DataDescription.TextMaxLength + 1);
    for(var index = 0; index < textLength; index++) {
      var letterIndex = random.Next(0, DataDescription.TextCharacters.Length);
      destination[charactersWritten++] = DataDescription.TextCharacters[letterIndex];
    }//for

#if DEBUG
    // Write end-text marker to check correctnes of writing and reading data
    destination[charactersWritten++] = DataDescription.LastTextCharacter;
#endif // DEBUG

    return charactersWritten;
  }

  private static ulong GetNextUInt64(Random random) {
    var bytes = ArrayPool<byte>.Shared.Rent(sizeof(ulong));
    try {
      random.NextBytes(bytes);
      return BitConverter.ToUInt64(bytes);
    } finally {
      ArrayPool<byte>.Shared.Return(bytes);
    }//try
  }

  public void Dispose() {
    for(var index = 0; index < Buffers.Count; index++) {
      ArrayPool<char>.Shared.Return(Buffers[index]);
    }//for
    Buffers.Clear();
  }
}
