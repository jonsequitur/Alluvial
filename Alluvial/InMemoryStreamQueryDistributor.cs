using System;
using System.Collections.Concurrent;
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
            Parallel.ForEach(Enumerable.Range(1, maxDegreesOfParallelism),
                             _ => RunOne());
        }

        private async Task RunOne()
        {
            if (stopped)
            {
                return;
            }

            var availableLease = leases.OrderBy(l => l.LastReleased)
                                       .FirstOrDefault(l => !workInProgress.ContainsKey(l));

            var unitOfWork = new DistributorUnitOfWork(availableLease);

            if (availableLease == null || !workInProgress.TryAdd(availableLease, unitOfWork))
            {
                await Task.Delay(waitInterval);
            }
            else
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
            stopped = true;

            while (workInProgress.Any())
            {
                await Task.Delay(150);
            }
        }

        public void Dispose()
        {
            Stop();
        }
    }
}