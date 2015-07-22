using System;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    public static class StreamQueryDistributor
    {
        public static IDistributor Trace(
            this IDistributor distributor,
            Action<Lease> onLeaseAcquired = null,
            Action<Lease> onLeaseReleasing = null)
        {
            return Create(
                start: () =>
                {
                    System.Diagnostics.Trace.WriteLine("[Distribute] Start");
                    return distributor.Start();
                },
                onReceive: receive =>
                {
                    onLeaseAcquired = onLeaseAcquired ?? TraceOnLeaseAcquired;
                    onLeaseReleasing = onLeaseReleasing ?? TraceOnLeaseReleasing;

                    // FIX: (Trace) this doesn't do anything if OnReceive was called before Trace, so a proper pipeline model may be better here.
                    distributor.OnReceive(async lease =>
                    {
                        onLeaseAcquired(lease);
                        await receive(lease);
                        onLeaseReleasing(lease);
                    });
                }, stop: () =>
                {
                    System.Diagnostics.Trace.WriteLine("[Distribute] Stop");
                    return distributor.Stop();
                }, distribute: distributor.Distribute);
        }



        private static void TraceOnLeaseReleasing(Lease lease)
        {
            System.Diagnostics.Trace.WriteLine("[Distribute] OnReceive (done) " + lease);
        }

        private static void TraceOnLeaseAcquired(Lease lease)
        {
            System.Diagnostics.Trace.WriteLine("[Distribute] OnReceive " + lease);
        }

        private static IDistributor Create(Func<Task> start, Action<Func<Lease, Task>> onReceive, Func<Task> stop, Func<int, Task> distribute)
        {
            return new AnonymousDistributor(start, onReceive, stop, distribute);
        }
    }
}