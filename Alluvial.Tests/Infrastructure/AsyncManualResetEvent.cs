// based on http://blogs.msdn.com/b/pfxteam/archive/2012/02/11/10266920.aspx

using System.Threading;
using System.Threading.Tasks;

namespace Alluvial.Tests
{
    public class AsyncManualResetEvent
    {
        private volatile TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();

        public Task WaitAsync()
        {
            return tcs.Task;
        }

        public void Set()
        {
            tcs.TrySetResult(true);
        }

        public void Reset()
        {
            while (true)
            {
                var tcs = this.tcs;
                if (!tcs.Task.IsCompleted ||
                    Interlocked.CompareExchange(ref this.tcs,
                                                new TaskCompletionSource<bool>(), tcs) == tcs)
                {
                    return;
                }
            }
        }
    }
}