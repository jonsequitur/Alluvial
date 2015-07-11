// based on http://blogs.msdn.com/b/pfxteam/archive/2012/02/11/10266920.aspx

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial.Tests
{
    public class AsyncBarrier
    {
        private readonly int initialParticipantCount;
        private int remainingParticipantCount;

        private ConcurrentStack<TaskCompletionSource<bool>> waiting;

        public AsyncBarrier(int participantCount)
        {
            if (participantCount <= 0)
            {
                throw new ArgumentOutOfRangeException("participantCount");
            }
            remainingParticipantCount = initialParticipantCount = participantCount;
            waiting = new ConcurrentStack<TaskCompletionSource<bool>>();
        }

        public Task SignalAndWait()
        {
            var tcs = new TaskCompletionSource<bool>();

            waiting.Push(tcs);

            if (Interlocked.Decrement(ref remainingParticipantCount) == 0)
            {
                remainingParticipantCount = initialParticipantCount;

                var waiters = waiting;
                waiting = new ConcurrentStack<TaskCompletionSource<bool>>();

                Parallel.ForEach(waiters, w => w.SetResult(true));
            }
            return tcs.Task;
        }
    }
}