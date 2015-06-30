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
                .Where(l => l.LastReleased + waitInterval < now)
                .OrderBy(l => l.LastReleased)
                .FirstOrDefault(l => !workInProgress.ContainsKey(l));

            if (availableLease != null)
            {
                var unitOfWork = new Lease(availableLease);

                if (workInProgress.TryAdd(availableLease, unitOfWork))
                {
                    unitOfWork.LeasableResource.LastGranted = now;

                    try
                    {
                        await onReceive(unitOfWork)
                            .TimeoutAfter(unitOfWork.LeasableResource.Duration);
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

        protected void Cancel(LeasableResource leasableResource)
        {
            Debug.WriteLine("[Distribute] canceling: " + leasableResource);
            Lease _;
            if (workInProgress.TryRemove(leasableResource, out _))
            {
                leasableResource.LastReleased = DateTimeOffset.UtcNow;
            }
        }

        protected override async Task Complete(Lease work)
        {
            if (!workInProgress.Values.Any(w => w.Equals(work)))
            {
                Debug.WriteLine("[Distribute] failed to complete: " + work);
                return;
            }

            Lease _;

            if (workInProgress.TryRemove(work.LeasableResource, out _))
            {
                Debug.WriteLine("[Distribute] complete: " + work);
                work.LeasableResource.LastReleased = DateTimeOffset.UtcNow;
            }
        }
    }
}