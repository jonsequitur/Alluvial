using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    public class InMemoryStreamQueryDistributor : StreamQueryDistributorBase
    {
        public InMemoryStreamQueryDistributor(
            DistributorLease[] leases,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null) :
                base(leases,
                     maxDegreesOfParallelism,
                     waitInterval)
        {
        }

        protected override async Task RunOne()
        {
            if (stopped)
            {
                Debug.WriteLine("[Distribute] Aborting");
                return;
            }

            var now = DateTimeOffset.UtcNow;

            Debug.WriteLine("[Distribute] Polling");

            var availableLease = leases
                .Where(l => l.LastReleased + waitInterval < now)
                .OrderBy(l => l.LastReleased)
                .FirstOrDefault(l => !workInProgress.ContainsKey(l));

            if (availableLease != null)
            {
                var unitOfWork = new DistributorUnitOfWork(availableLease);

                if (workInProgress.TryAdd(availableLease, unitOfWork))
                {
                    unitOfWork.Lease.LastGranted = now;

                    try
                    {
                        await onReceive(unitOfWork)
                            .TimeoutAfter(unitOfWork.Lease.Duration);
                    }
                    catch (Exception exception)
                    {
                        Debug.WriteLine(exception);
                    }

                    Complete(unitOfWork);
                }
            }
            else
            {
                await Task.Delay(waitInterval);
            }

            Task.Run(() => RunOne());
        }

        protected void Cancel(DistributorLease lease)
        {
            Debug.WriteLine("[Distribute] canceling: " + lease);
            DistributorUnitOfWork _;
            if (workInProgress.TryRemove(lease, out _))
            {
                lease.LastReleased = DateTimeOffset.UtcNow;
            }
        }

        protected override async Task Complete(DistributorUnitOfWork work)
        {
            if (!workInProgress.Values.Any(w => w.Equals(work)))
            {
                Debug.WriteLine("[Distribute] failed to complete: " + work);
                return;
            }

            DistributorUnitOfWork _;

            if (workInProgress.TryRemove(work.Lease, out _))
            {
                Debug.WriteLine("[Distribute] complete: " + work);
                work.Lease.LastReleased = DateTimeOffset.UtcNow;
            }
        }
    }
}