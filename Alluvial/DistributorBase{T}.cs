using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    public abstract class DistributorBase<T> : IDistributor<T>, IEnumerable<T>
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
            if (leasables.Length ==0)
            {
                throw new ArgumentException("There must be at least one leasable.");
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

        public virtual async Task<IEnumerable<T>> Distribute(int count)
        {
            var acquired = new List<T>();

            while (acquired.Count < count)
            {
                var acquisition = await TryRunOne(loop: false);
                if (acquisition.Acquired)
                {
                    acquired.Add(acquisition.Value);
                }
                else
                {
                    await Task.Delay((int) (waitInterval.TotalMilliseconds/leasables.Length));
                }
            }

            return acquired;
        }

        public virtual async Task Start()
        {
            if (onReceive == null)
            {
                throw new InvalidOperationException("You must call OnReceive before calling Start.");
            }

            for (var i = 0; i < maxDegreesOfParallelism; i++)
            {
#pragma warning disable 4014
                // deliberately fire and forget so that the tasks can run in parallel
                TryRunOne(loop: true);
#pragma warning restore 4014
            }
        }

        private async Task<LeaseAcquisitionAttempt> TryRunOne(bool loop)
        {
            if (stopped)
            {
                Debug.WriteLine("[Distribute] Aborting");
                return LeaseAcquisitionAttempt.Failed();
            }

            Debug.WriteLine("[Distribute] Trying to acquire lease");

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
                Debug.WriteLine("[Distribute] Did not acquire lease");

                if (loop)
                {
                    await Task.Delay(waitInterval);
                }
                else
                {
                    return LeaseAcquisitionAttempt.Failed();
                }
            }

            if (loop)
            {
#pragma warning disable 4014
                // async recursion. we don't await in order to truncate the call stack.
                Task.Run(() => TryRunOne(loop));
#pragma warning restore 4014
            }
            
            return LeaseAcquisitionAttempt.Succeeded(lease.Resource);
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

        private struct LeaseAcquisitionAttempt
        {
            public T Value { get; private set; }

            public bool Acquired { get; private set; }

            public static LeaseAcquisitionAttempt Failed()
            {
                return new LeaseAcquisitionAttempt
                {
                    Acquired = false
                };
            }

            public static LeaseAcquisitionAttempt Succeeded(T value)
            {
                return new LeaseAcquisitionAttempt
                {
                    Acquired = true,
                    Value = value
                };
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return leasables.Select(l => l.Resource).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}