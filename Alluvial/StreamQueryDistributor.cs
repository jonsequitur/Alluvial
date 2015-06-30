using System;
using System.Threading.Tasks;

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
                    distributor.OnReceive(async unitOfWork =>
                    {
                        System.Diagnostics.Trace.WriteLine("[Distribute] OnReceive " + unitOfWork);
                        await doWork(unitOfWork);
                    });
                }, stop: () =>
                {
                    System.Diagnostics.Trace.WriteLine("[Distribute] Stop");
                    return distributor.Stop();
                });
        }

        private static IStreamQueryDistributor Create(Func<Task> start, Action<Func<DistributorUnitOfWork, Task>> doWork, Func<Task> stop)
        {
            return new AnonymousStreamQueryDistributor(start, doWork, stop);
        }
    }
}