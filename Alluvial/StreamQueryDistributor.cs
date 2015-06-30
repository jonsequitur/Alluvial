using System;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    public static class StreamQueryDistributor
    {
        public static IStreamQueryDistributor Trace(this IStreamQueryDistributor distributor)
        {
            return Create(
                start: () =>
                {
                    System.Diagnostics.Trace.WriteLine("[Distribute] Start");
                    return distributor.Start();
                },
                doWork: doWork =>
                {
                    // FIX: (Trace) this doesn't do anything if OnReceive was called before Trace, so a proper pipeline model may be better here.
                    distributor.OnReceive(async lease =>
                    {
                        System.Diagnostics.Trace.WriteLine("[Distribute] OnReceive " + lease);
                        await doWork(lease);
                    });
                }, stop: () =>
                {
                    System.Diagnostics.Trace.WriteLine("[Distribute] Stop");
                    return distributor.Stop();
                });
        }

        private static IStreamQueryDistributor Create(Func<Task> start, Action<Func<Lease, Task>> doWork, Func<Task> stop)
        {
            return new AnonymousStreamQueryDistributor(start, doWork, stop);
        }
    }
}