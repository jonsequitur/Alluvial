// based on http://blogs.msdn.com/b/pfxteam/archive/2012/02/11/10266920.aspx

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial.Tests
{
    public class AsyncCountdownEvent
    {
        private readonly AsyncManualResetEvent manualResetEvent = new AsyncManualResetEvent();
        private int remainingCount;

        public AsyncCountdownEvent(int initialCount)
        {
            if (initialCount <= 0)
            {
                throw new ArgumentOutOfRangeException("initialCount");
            }
            remainingCount = initialCount;
        }

        public Task WaitAsync()
        {
            return manualResetEvent.WaitAsync();
        }

        public void Signal()
        {
            if (remainingCount <= 0)
            {
                throw new InvalidOperationException();
            }

            int newCount = Interlocked.Decrement(ref remainingCount);
            if (newCount == 0)
            {
                manualResetEvent.Set();
            }
            else if (newCount < 0)
            {
                throw new InvalidOperationException();
            }
        }

        public Task SignalAndWait()
        {
            Signal();
            return WaitAsync();
        }
    }
}