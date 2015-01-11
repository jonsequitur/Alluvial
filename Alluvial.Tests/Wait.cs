using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Alluvial.Tests
{
    public static class Wait
    {
        public static async Task Until(
            Func<bool> until,
            TimeSpan? pollInterval = null,
            TimeSpan? timeout = null)
        {
            timeout = timeout ?? TimeSpan.FromSeconds(10);
            pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(100);

            var tcs = new TaskCompletionSource<bool>();

            var timer = new Stopwatch();
            timer.Start();

            Task.Run(async () =>
                           {
                               while (!tcs.Task.IsCompleted &&
                                      !tcs.Task.IsFaulted &&
                                      !tcs.Task.IsCanceled)
                               {
                                   if (timer.Elapsed >= timeout)
                                   {
                                       tcs.SetException(new TimeoutException());
                                       break;
                                   }

                                   try
                                   {
                                       if (until())
                                       {
                                           tcs.SetResult(true);
                                       }
                                       else
                                       {
                                           await Task.Delay(pollInterval.Value);
                                       }
                                   }
                                   catch (Exception exception)
                                   {
                                       tcs.SetException(exception);
                                   }
                               }
                           });

            await tcs.Task;

            if (tcs.Task.IsFaulted)
            {
                throw tcs.Task.Exception;
            }
        }
    }
}