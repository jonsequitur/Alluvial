using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Alluvial.Tests
{
    public static class Wait
    {
        public static async Task Timeout(this Task task)
        {
            TimeSpan timeout;
            if (Debugger.IsAttached)
            {
                timeout = TimeSpan.FromMinutes(5);
            }
            else
            {
                timeout = TimeSpan.FromSeconds(20);
            }

            if (task.IsCompleted)
            {
                return;
            }

            if (task == await Task.WhenAny(task, Task.Delay(timeout)))
            {
                await task;
            }
            else
            {
                throw new TimeoutException();
            }
        }

        public static async Task Until(
            Func<bool> until,
            TimeSpan? pollInterval = null,
            TimeSpan? timeout = null)
        {
            timeout = timeout ??
                      (Debugger.IsAttached
                           ? (TimeSpan.FromMinutes(5))
                           : (TimeSpan.FromSeconds(20)));

            pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(100);

            var timer = new Stopwatch();
            timer.Start();

            while (true)
            {
                if (until())
                {
                    return;
                }

                if (timer.Elapsed > timeout)
                {
                    throw new TimeoutException();
                }

                await Task.Delay(pollInterval.Value);
            }
        }
    }
}