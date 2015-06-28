using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    public class InMemoryStreamQueryDistributor : IStreamQueryDistributor
    {
        private readonly ConcurrentDictionary<Lease, DistributorUnitOfWork> workInProgress = new ConcurrentDictionary<Lease, DistributorUnitOfWork>();
        private readonly Lease[] leases;
        private Func<DistributorUnitOfWork, Task> onReceive;
        private readonly int maxDegreesOfParallelism;
        private bool stopped;
        private readonly TimeSpan waitInterval;

        public InMemoryStreamQueryDistributor(
            Lease[] leases,
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

        private async Task RunOne()
        {
            if (stopped)
            {
                Debug.WriteLine("Aborting");
                return;
            }

            var availableLease = leases
                .Where(l => l.LastReleased + waitInterval < DateTimeOffset.UtcNow)
                .OrderBy(l => l.LastReleased)
                .FirstOrDefault(l => !workInProgress.ContainsKey(l));
            
            Debug.WriteLine("Polling");

            if (availableLease != null)
            {
                Debug.WriteLine("RunOne: available lease = " + availableLease.Name);

                var unitOfWork = new DistributorUnitOfWork(availableLease);

                if (workInProgress.TryAdd(availableLease, unitOfWork))
                {
                    unitOfWork.Lease.LastGranted = DateTimeOffset.UtcNow;

                    try
                    {
                        await onReceive(unitOfWork);
                    }
                    catch (Exception exception)
                    {
                    }

                    Complete(unitOfWork.Lease);
                }
            }
            else
            {
                await Task.Delay(waitInterval);
            }

            Task.Run(() => RunOne());
        }

        private void Complete(Lease lease)
        {
            DistributorUnitOfWork _;
            if (workInProgress.TryRemove(lease, out _))
            {
                lease.LastReleased = DateTime.UtcNow;
            }
        }

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
                Debug.WriteLine("Stop: waiting for " + workInProgress.Count + " to complete" );
                await Task.Delay(waitInterval);
            }
        }

        public void Dispose()
        {
            Stop();
        }
    }
}