using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    public abstract class StreamQueryDistributorBase : IStreamQueryDistributor
    {
        protected readonly ConcurrentDictionary<DistributorLease, DistributorUnitOfWork> workInProgress = new ConcurrentDictionary<DistributorLease, DistributorUnitOfWork>();
        protected Func<DistributorUnitOfWork, Task> onReceive;
        protected int maxDegreesOfParallelism;
        protected bool stopped;
        protected TimeSpan waitInterval;
        protected readonly DistributorLease[] leases;

        protected StreamQueryDistributorBase(
            DistributorLease[] leases,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null)
        {
            if (leases == null)
            {
                throw new ArgumentNullException("leases");
            }
            if (maxDegreesOfParallelism <= 0)
            {
                throw new ArgumentException("maxDegreesOfParallelism must be at least 1.");
            }
            this.leases = leases;
            this.maxDegreesOfParallelism = Math.Min(maxDegreesOfParallelism, leases.Count());
            this.waitInterval = waitInterval ?? TimeSpan.FromSeconds(.5);
        }

        public void OnReceive(Func<DistributorUnitOfWork, Task> onReceive)
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

        protected abstract Task RunOne();

        protected abstract Task Complete(DistributorLease lease);

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