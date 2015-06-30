using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    public class InMemoryStreamQueryDistributor : StreamQueryDistributorBase
    {
        public InMemoryStreamQueryDistributor(
            LeasableResource[] LeasablesResource,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null) :
                base(LeasablesResource,
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

            var availableLease = LeasablesResource
                .Where(l => l.LeaseLastReleased + waitInterval < now)
                .OrderBy(l => l.LeaseLastReleased)
                .FirstOrDefault(l => !workInProgress.ContainsKey(l));

            if (availableLease != null)
            {
                var lease = new Lease(availableLease);

                if (workInProgress.TryAdd(availableLease, lease))
                {
                    lease.LeasableResource.LeaseLastGranted = now;

                    try
                    {
                        await onReceive(lease)
                            .TimeoutAfter(lease.LeasableResource.Duration);
                    }
                    catch (Exception exception)
                    {
                        Debug.WriteLine(exception);
                    }

                    Complete(lease);
                }
            }
            else
            {
                await Task.Delay(waitInterval);
            }

            Task.Run(() => RunOne());
        }

        protected void Cancel(LeasableResource leasableResource)
        {
            Debug.WriteLine("[Distribute] canceling: " + leasableResource);
            Lease _;
            if (workInProgress.TryRemove(leasableResource, out _))
            {
                leasableResource.LeaseLastReleased = DateTimeOffset.UtcNow;
            }
        }

        protected override async Task Complete(Lease lease)
        {
            if (!workInProgress.Values.Any(l => l.Equals(lease)))
            {
                Debug.WriteLine("[Distribute] failed to complete: " + lease);
                return;
            }

            Lease _;

            if (workInProgress.TryRemove(lease.LeasableResource, out _))
            {
                Debug.WriteLine("[Distribute] complete: " + lease);
                lease.LeasableResource.LeaseLastReleased = DateTimeOffset.UtcNow;
            }
        }
    }
}