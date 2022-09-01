using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;

namespace DataSort.SortFile;

using Common;

internal sealed class DataReading : IDisposable, IAsyncDisposable
{
  public DataReading(string fileName, Encoding encoding, SortFileConfigurationOptions configuration) {
    ArgumentNullException.ThrowIfNull(encoding);
    ArgumentNullException.ThrowIfNull(configuration);

    Encoding = encoding;

    DelimeterCharacters = Environment.NewLine.ToCharArray();
    DelimeterBytes = Encoding.GetBytes(DelimeterCharacters);

    var streamBufferSize = configuration.ReadingStreamBufferSizeMiB * 1024 * 1024;
    Stream = new(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, streamBufferSize, FileOptions.SequentialScan | FileOptions.Asynchronous);

    var maxRowBytes = Encoding.GetByteCount(DataDescription.MaxString);
    var bufferSize = configuration.ReadingPipeBufferSizeFactor * maxRowBytes;
    var minimumReadSize = configuration.ReadingPipeMinimumReadSizeFactor * maxRowBytes;
    var options = new StreamPipeReaderOptions(bufferSize: bufferSize, minimumReadSize: minimumReadSize);
    Reader = PipeReader.Create(Stream, options);
  }

  public Encoding Encoding { get; }
  private char[] DelimeterCharacters { get; }
  private byte[] DelimeterBytes { get; }

  private FileStream Stream { get; }
  private PipeReader Reader { get; }

  public async IAsyncEnumerable<List<string>> ReadLinesAsync(int chunkSize, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
    if(chunkSize <= 0) {
      throw new ArgumentOutOfRangeException(nameof(chunkSize), chunkSize, "Value should be positive.");
    }//if

    var isFirstRead = true;
    var list0 = new List<string>(capacity: chunkSize);
    var list1 = new List<string>(capacity: chunkSize);
    var currentList = list0;
    while(true) {
      var result = await Reader.ReadAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

      if(isFirstRead) {
        // Read encoding preamble, if required
        var (preamble, _) = ReadPreamble(result.Buffer, result.IsCompleted);
        Reader.AdvanceTo(preamble, result.Buffer.End);
        result = await Reader.ReadAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

        isFirstRead = false;
      }//if

      var (position, isEnd) = ReadLines(result.Buffer, result.IsCompleted, currentList);

      if(currentList.Count >= chunkSize || result.IsCompleted && isEnd) {
        yield return currentList;
        currentList = currentList == list0 ? list1 : list0;
        currentList.Clear();
      }//if

      if(result.IsCompleted && isEnd) {
        await Reader.CompleteAsync().ConfigureAwait(continueOnCapturedContext: false);
        break;
      }//if

      Reader.AdvanceTo(position, result.Buffer.End);
    }//while
  }

  private (SequencePosition Position, bool End) ReadLines(in ReadOnlySequence<byte> sequence, bool isCompleted, List<string> items) {
    var reader = new SequenceReader<byte>(sequence);
    while(!reader.End) {
      if(reader.TryReadTo(out ReadOnlySpan<byte> bytes, DelimeterBytes, advancePastDelimiter: true)) {
        var value = Encoding.GetString(bytes);
        items.Add(value);
      } else if(isCompleted) {
        var value = Encoding.GetString(sequence);
        var parts = value.Split(DelimeterCharacters, StringSplitOptions.RemoveEmptyEntries);
        items.AddRange(parts);
        reader.AdvanceToEnd();
        break;
      } else {
        break;
      }//if

      if(items.Count == items.Capacity) {
        break;
      }//if
    }//while

    return (reader.Position, reader.End);
  }

  private (SequencePosition Position, bool End) ReadPreamble(in ReadOnlySequence<byte> sequence, bool isCompleted) {
    var reader = new SequenceReader<byte>(sequence);
    foreach(var item in Encoding.Preamble) {
      if(!reader.TryRead(out var value) || value != item) {
        return default;
      }//if
    }//for

    return (reader.Position, reader.End);
  }

  public void Dispose() => Stream.Dispose();

  public ValueTask DisposeAsync() => Stream.DisposeAsync();
}
