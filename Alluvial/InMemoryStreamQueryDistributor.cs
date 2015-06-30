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
                Debug.WriteLine("Aborting");
                return;
            }

            var now = DateTimeOffset.UtcNow;

            var availableLease = leases
                .Where(l => l.LastReleased + waitInterval < now)
                .OrderBy(l => l.LastReleased)
                .FirstOrDefault(l =>
                {
                    if (!workInProgress.ContainsKey(l))
                    {
                        return true;
                    }

                    if (l.LastGranted < now - l.Duration)
                    {
                        // FIX: (RunOne) workInProgress needs to have the items removed / canceled / etc, so an owner token is going to be needed 
                        return true;
                    }

                    return false;
                });

            Debug.WriteLine("Polling");

            if (availableLease != null)
            {
                Debug.WriteLine("RunOne: available lease = " + availableLease.Name);

                var unitOfWork = new DistributorUnitOfWork(availableLease);

                if (workInProgress.TryAdd(availableLease, unitOfWork))
                {
                    unitOfWork.Lease.LastGranted = now;

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

        protected override async Task Complete(DistributorLease lease)
        {
            DistributorUnitOfWork _;
            if (workInProgress.TryRemove(lease, out _))
            {
                lease.LastReleased = DateTime.UtcNow;
            }
        }
    }
}