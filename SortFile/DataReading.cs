using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;

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

    BufferCount = configuration.ReadingBufferCount > 0 ? configuration.ReadingBufferCount : Environment.ProcessorCount;
    BufferChannel = Channel.CreateBounded<List<string>>(new BoundedChannelOptions(capacity: BufferCount) {
      FullMode = BoundedChannelFullMode.Wait,
      SingleReader = true,
      SingleWriter = false,
    });

    var streamBufferSize = configuration.ReadingStreamBufferSizeKiB * 1024;
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

  private int BufferCount { get; }
  private Channel<List<string>> BufferChannel { get; }

  private async Task InitializeBuffersAsync(int chunkSize, CancellationToken cancellationToken = default) {
    for(var index = 0; index < BufferCount; index++) {
      var value = new List<string>(capacity: chunkSize);
      await BufferChannel.Writer.WriteAsync(value, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    }//for
  }

  private ValueTask<List<string>> GetBufferAsync(CancellationToken cancellationToken = default) => BufferChannel.Reader.ReadAsync(cancellationToken);

  public ValueTask ReturnBufferAsync(List<string> value, CancellationToken cancellationToken = default) {
    ArgumentNullException.ThrowIfNull(value);

    value.Clear();
    return BufferChannel.Writer.WriteAsync(value, cancellationToken);
  }

  public async IAsyncEnumerable<List<string>> ReadLinesAsync(int chunkSize, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
    if(chunkSize <= 0) {
      throw new ArgumentOutOfRangeException(nameof(chunkSize), chunkSize, "Value should be positive.");
    }//if

    await InitializeBuffersAsync(chunkSize, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

    var isFirstRead = true;
    var currentList = await GetBufferAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
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
        currentList = await GetBufferAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
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
