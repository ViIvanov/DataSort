using System;
using System.Buffers;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DataSort.GenerateFile;

internal sealed class FileSaving : IDisposable, IAsyncDisposable
{
  private const int DefaultStreamBufferSize = 0x_100_0000; // 16 Mb

  public FileSaving(string filePath, long requiredLength, int maxBufferSize, Encoding? encoding = null, int? streamBufferSize = null) {
    Encoding = encoding ?? Encoding.UTF8;

    NewLineBytes = Encoding.GetBytes(Environment.NewLine);
    NewLineBytesLength = NewLineBytes.Length;

    Buffer = ArrayPool<byte>.Shared.Rent(maxBufferSize);

    Stream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.Read, streamBufferSize ?? DefaultStreamBufferSize, FileOptions.Asynchronous);
    Stream.SetLength(requiredLength);
  }

  public Encoding Encoding { get; }
  private byte[] NewLineBytes { get; }
  private int NewLineBytesLength { get; }
  private byte[] Buffer { get; }
  private FileStream Stream { get; }

  public async Task<long> WritePreambleAsync(CancellationToken cancellationToken = default) {
    var preamble = Encoding.GetPreamble();
    if(preamble.Length > 0) {
      await Stream.WriteAsync(preamble, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    }//if
    return preamble.Length;
  }

  public async Task<long> WriteDataAsync(ReadOnlyMemory<char> buffer, CancellationToken cancellationToken = default) {
    var bufferLengthInBytes = Encoding.GetBytes(buffer.Span, Buffer);
    await Stream.WriteAsync(Buffer.AsMemory()[0..bufferLengthInBytes], cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    await Stream.WriteAsync(NewLineBytes, cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
    return bufferLengthInBytes + NewLineBytesLength;
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

