using System.Diagnostics;

namespace DataSort.Common;

public static class ThreadSafeStopwatch
{
  public static bool IsRunning(this ThreadLocal<Stopwatch> stopwatch) => (stopwatch ?? throw new ArgumentNullException(nameof(stopwatch))).Value?.IsRunning ?? false;

  public static void Start(this ThreadLocal<Stopwatch> stopwatch) => (stopwatch ?? throw new ArgumentNullException(nameof(stopwatch))).Value?.Start();
  public static void Stop(this ThreadLocal<Stopwatch> stopwatch) => (stopwatch ?? throw new ArgumentNullException(nameof(stopwatch))).Value?.Stop();

  public static void Restart(this ThreadLocal<Stopwatch> stopwatch) => (stopwatch ?? throw new ArgumentNullException(nameof(stopwatch))).Value?.Restart();
  public static void Reset(this ThreadLocal<Stopwatch> stopwatch) => (stopwatch ?? throw new ArgumentNullException(nameof(stopwatch))).Value?.Reset();

  public static TimeSpan Elapsed(this ThreadLocal<Stopwatch> stopwatch) => new TimeSpan(stopwatch.ElapsedTicks());
  public static long ElapsedTicks(this ThreadLocal<Stopwatch> stopwatch) => stopwatch?.Values.Sum(item => item.ElapsedTicks) ?? throw new ArgumentNullException(nameof(stopwatch));
  public static long ElapsedMilliseconds(this ThreadLocal<Stopwatch> stopwatch) => stopwatch?.Values.Sum(item => item.ElapsedMilliseconds) ?? throw new ArgumentNullException(nameof(stopwatch));
}
