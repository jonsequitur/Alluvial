using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    public abstract class DistributorBase<T> : IDistributor<T>
    {
        private Func<Lease<T>, Task> onReceive;
        private readonly int maxDegreesOfParallelism;
        private bool stopped;
        protected readonly TimeSpan waitInterval;
        protected readonly Leasable<T>[] leasables;
        private int leasesHeld;

        protected DistributorBase(
            Leasable<T>[] leasables,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null)
        {
            if (leasables == null)
            {
                throw new ArgumentNullException("leasables");
            }
            if (maxDegreesOfParallelism <= 0)
            {
                throw new ArgumentException("maxDegreesOfParallelism must be at least 1.");
            }
            this.leasables = leasables;
            this.maxDegreesOfParallelism = Math.Min(maxDegreesOfParallelism, leasables.Count());
            this.waitInterval = waitInterval ?? TimeSpan.FromSeconds(.5);
        }

        public void OnReceive(Func<Lease<T>, Task> onReceive)
        {
            if (this.onReceive != null)
            {
                throw new InvalidOperationException("OnReceive has already been called. It can only be called once per distributor.");
            }
            this.onReceive = onReceive;
        }

        public async Task Distribute(int count)
        {
            await RunOne(loop: false);
        }

        public async Task Start()
        {
            if (onReceive == null)
            {
                throw new InvalidOperationException("You must call OnReceive before calling Start.");
            }

            for (var i = 0; i < maxDegreesOfParallelism; i++)
            {
#pragma warning disable 4014
                // deliberately fire and forget so that the tasks can run in parallel
                RunOne(loop: true);
#pragma warning restore 4014
            }
        }

        private async Task RunOne(bool loop)
        {
            if (stopped)
            {
                Debug.WriteLine("[Distribute] Aborting");
                return;
            }

            Debug.WriteLine("[Distribute] Polling");

            Lease<T> lease = null;
            try
            {
                lease = await AcquireLease();
            }
            catch (Exception exception)
            {
                Debug.WriteLine("[Distribute] Exception during AcquireLease:\n" + exception);
            }

            if (lease != null)
            {
                Interlocked.Increment(ref leasesHeld);

                try
                {
                    var receive = onReceive(lease);

                    // the cancellation token will be set for a shorter period of time but can be extended using Lease.Extend, so 10 minutes is the upper bound
                    var timeout = Task.Delay(TimeSpan.FromMinutes(10), lease.CancellationToken);

                    await receive.TimeoutAfter(timeout);
                }
                catch (Exception exception)
                {
                    Debug.WriteLine(string.Format("[Distribute] Exception during OnReceive for lease {0}:\n{1}", lease, exception));
                }

                try
                {
                    await ReleaseLease(lease);
                }
                catch (Exception exception)
                {
                    Debug.WriteLine(string.Format("[Distribute] Exception during ReleaseLease for lease {0}:\n{1}", lease, exception));
                }

                Interlocked.Decrement(ref leasesHeld);
            }
            else
            {
                if (loop)
                {
                    await Task.Delay(waitInterval);
                }
            }

            if (loop)
            {
#pragma warning disable 4014
                // async recursion. we don't await in order to truncate the call stack.
                Task.Run(() => RunOne(loop));
#pragma warning restore 4014
            }
        }

        protected abstract Task ReleaseLease(Lease<T> lease);

        protected abstract Task<Lease<T>> AcquireLease();

        public async Task Stop()
        {
            stopped = true;

            while (leasesHeld > 0)
            {
                Debug.WriteLine(string.Format("[Distribute] Stop: waiting for {0} to complete", leasesHeld));
                await Task.Delay(waitInterval);
            }
        }

        public void Dispose()
        {
            Stop();
        }
    }
}