using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    public abstract class StreamQueryDistributorBase : IStreamQueryDistributor
    {
        protected readonly ConcurrentDictionary<LeasableResource, Lease> workInProgress = new ConcurrentDictionary<LeasableResource, Lease>();
        protected Func<Lease, Task> onReceive;
        protected int maxDegreesOfParallelism;
        protected bool stopped;
        protected TimeSpan waitInterval;
        protected readonly LeasableResource[] LeasablesResource;

        protected StreamQueryDistributorBase(
            LeasableResource[] LeasablesResource,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null)
        {
            if (LeasablesResource == null)
            {
                throw new ArgumentNullException("LeasablesResource");
            }
            if (maxDegreesOfParallelism <= 0)
            {
                throw new ArgumentException("maxDegreesOfParallelism must be at least 1.");
            }
            this.LeasablesResource = LeasablesResource;
            this.maxDegreesOfParallelism = Math.Min(maxDegreesOfParallelism, LeasablesResource.Count());
            this.waitInterval = waitInterval ?? TimeSpan.FromSeconds(.5);
        }

        public void OnReceive(Func<Lease, Task> onReceive)
        {
            if (this.onReceive != null)
            {
                throw new InvalidOperationException("OnReceive has already been called. It can only be called once per distributor.");
            }
            this.onReceive = onReceive;
        }

        public async Task Start()
        {
            if (onReceive == null)
            {
                throw new InvalidOperationException("You must call OnReceive before calling Start.");
            }

            for (var i = 0; i < maxDegreesOfParallelism; i++)
            {
                RunOne();
            }
        }

        private async Task RunOne()
        {
            if (stopped)
            {
                Debug.WriteLine("[Distribute] Aborting");
                return;
            }

            Debug.WriteLine("[Distribute] Polling");

            var lease = await AcquireLease();

            if (lease != null)
            {
                try
                {
                    var receive = onReceive(lease);

                    var timeout = Task.Delay(TimeSpan.FromMinutes(10), lease.CancellationToken);

                    await receive.TimeoutAfter(timeout);
                }
                catch (Exception exception)
                {
                    Debug.WriteLine(exception);
                }

                ReleaseLease(lease);
            }
            else
            {
                await Task.Delay(waitInterval);
            }

            Task.Run(() => RunOne());
        }

        protected abstract Task ReleaseLease(Lease lease);

        protected abstract Task<Lease> AcquireLease();

        public async Task Stop()
        {
            if (stopped)
            {
                return;
            }

            stopped = true;

            Debug.WriteLine("Stop");

            while (workInProgress.Count > 0)
            {
                Debug.WriteLine("Stop: waiting for " + workInProgress.Count + " to complete");
                await Task.Delay(waitInterval);
            }
        }

        public void Dispose()
        {
            Stop();
        }
    }
}