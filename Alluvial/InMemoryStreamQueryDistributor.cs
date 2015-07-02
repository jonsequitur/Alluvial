using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
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

            var availableResource = AcquireAvailableResource(now);

            if (availableResource != null)
            {
                var cancellationTokenSource = new CancellationTokenSource();
                
                var lease = new Lease(availableResource,
                                      availableResource.DefaultDuration,
                                      extend: ts => cancellationTokenSource.CancelAfter(ts));
                
                cancellationTokenSource.CancelAfter(lease.Duration);

                if (workInProgress.TryAdd(availableResource, lease))
                {
                    lease.LeasableResource.LeaseLastGranted = now;

                    try
                    {
                        var receive = onReceive(lease);

                        var timeout = Task.Delay(TimeSpan.FromMinutes(10), cancellationTokenSource.Token);

                        await receive.TimeoutAfter(timeout);
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

        private LeasableResource AcquireAvailableResource(DateTimeOffset asOf)
        {
            return LeasablesResource
                .Where(l => l.LeaseLastReleased + waitInterval < asOf)
                .OrderBy(l => l.LeaseLastReleased)
                .FirstOrDefault(l => !workInProgress.ContainsKey(l));
        }

        protected override async Task Complete(Lease lease)
        {
            lease.NotifyCompleted();

            if (!workInProgress.Values.Any(l => l.GetHashCode().Equals(lease.GetHashCode())))
            {
                Debug.WriteLine("[Distribute] failed to complete: " + lease);
                return;
            }

            Lease _;

            if (workInProgress.TryRemove(lease.LeasableResource, out _))
            {
                lease.LeasableResource.LeaseLastReleased = DateTimeOffset.UtcNow;
                Debug.WriteLine("[Distribute] complete: " + lease);
            }
        }
    }
}