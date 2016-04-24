using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    internal static class TaskExtensions
    {
        public static async Task<IEnumerable<T>> AwaitAll<T>(this IEnumerable<Task<T>> tasks)
        {
            var tasksArray = tasks.ToArray();
            await Task.WhenAll(tasksArray);
            return tasksArray.Select(t => t.Result);
        }

        public static Task<T> CompletedTask<T>(this T result) => 
            Task.FromResult(result);

        public static async Task TimeoutAfter(
            this Task task,
            Task timeout)
        {
            if (task.IsCompleted)
            {
                return;
            }

            if (task == await Task.WhenAny(task, timeout))
            {
                await task;
            }
            else
            {
                throw new TimeoutException();
            }
        }
    }
}