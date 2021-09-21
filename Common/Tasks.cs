namespace DataSort.Common;

public static class Tasks
{
  public static Task WhenAll(int count, Func<int, Task> factory, CancellationToken cancellationToken = default) {
    var array = new Task[count];
    for(var index = 0; index < array.Length; index++) {
      array[index] = Task.Run(() => factory(index), cancellationToken);
    }//for
    return Task.WhenAll(array);
  }
}
