using System.Threading.Channels;

namespace DataSort.GenerateFile;

using Common;

internal sealed class DataGenerationChannel
{
  public DataGenerationChannel(int capacity, int concurrentGenerators) {
    Value = Channel.CreateBounded<ReadOnlyMemory<char>>(new BoundedChannelOptions(capacity) {
      FullMode = BoundedChannelFullMode.Wait,
      SingleReader = true,
      SingleWriter = false,
    });

    var taskCount = concurrentGenerators > 0 ? concurrentGenerators : Environment.ProcessorCount;
    WaitAllGeneratorsTask = Tasks.WhenAll(taskCount, async index => {
      // Need two extra buffers:
      // * one for value after the previous value added to the channel
      // * one for value after the previous value read from the channel
      var buffersCount = capacity + 2;
      using var generation = new DataGeneration(index, buffersCount);
      try {
        var iteration = 0;
        while(true) {
          var value = generation.NextData(iteration % buffersCount);
          iteration++;

          await Value.Writer.WriteAsync(value).ConfigureAwait(continueOnCapturedContext: false);
        }//while
      } catch(ChannelClosedException) {
        // Expected, channel is closed
      }//try
    });
  }

  private Channel<ReadOnlyMemory<char>> Value { get; }
  private Task WaitAllGeneratorsTask { get; }

  public ValueTask<ReadOnlyMemory<char>> NextAsync(CancellationToken cancellationToken = default) => Value.Reader.ReadAsync(cancellationToken);

  public async Task CompleteAsync() {
    Value.Writer.Complete();
    await WaitAllGeneratorsTask.ConfigureAwait(continueOnCapturedContext: false);
  }
}
