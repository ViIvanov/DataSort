using System.Buffers;
using System.Text;
using System.Threading;

namespace DataSort.Common;

public sealed class FileSaving : IDisposable, IAsyncDisposable
{
  private const int DefaultStreamBufferSize = 0x_100_0000; // 16 Mb

  private int isFirstLine = 0;

  public FileSaving(string filePath, int maxBufferSize, Encoding encoding, int? streamBufferSize = null, long? requiredLength = null) {
    Encoding = encoding ?? throw new ArgumentNullException(nameof(encoding));
    NewLineBytes = Encoding.GetBytes(Environment.NewLine);
    Buffer = ArrayPool<byte>.Shared.Rent(maxBufferSize);
    Stream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.Read, streamBufferSize ?? DefaultStreamBufferSize, FileOptions.Asynchronous);

    if(requiredLength is not null) {
      try {
        Stream.SetLength(requiredLength.Value);
      } catch {
        Dispose();
        throw;
      }//try
    }//if
  }

  public Encoding Encoding { get; }
  private byte[] NewLineBytes { get; }
  private byte[] Buffer { get; }
  private FileStream Stream { get; }

  private async Task<long> WritePreambleAsync(CancellationToken cancellationToken = default) {
    var preamble = Encoding.GetPreamble();
    if(preamble.Length > 0) {
      await Stream.WriteAsync(preamble, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    }//if
    return preamble.Length;
  }

  private async Task<long> WriteNewLineAsync(CancellationToken cancellationToken = default) {
    await Stream.WriteAsync(NewLineBytes, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    return NewLineBytes.Length;
  }

  private Task<long> WritePrefixAsync(ReadOnlyMemory<char> buffer, CancellationToken cancellationToken = default)
    => Interlocked.Exchange(ref isFirstLine, 1) is 0 ? WritePreambleAsync(cancellationToken) : WriteNewLineAsync(cancellationToken);

  public async Task<long> WriteDataAsync(ReadOnlyMemory<char> buffer, CancellationToken cancellationToken = default) {
    // Prefix is encoding preamble for first line or new line for other lines.
    var prefixLengthInBytesLength = await WritePrefixAsync(buffer, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

    var bufferLengthInBytes = Encoding.GetBytes(buffer.Span, Buffer);
    await Stream.WriteAsync(Buffer.AsMemory()[0..bufferLengthInBytes], cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    return prefixLengthInBytesLength + bufferLengthInBytes;
  }

  private void CleanUp() => ArrayPool<byte>.Shared.Return(Buffer);

  public void Dispose() {
    CleanUp();
    Stream.Dispose();
  }

  public ValueTask DisposeAsync() {
    CleanUp();
    return Stream.DisposeAsync();
  }
}

