using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    public static class Distributor
    {
        public static IDistributor<T> Create<T>(
            Func<Task> start,
            Action<Func<Lease<T>, Task>> onReceive,
            Func<Task> stop,
            Func<int, Task<IEnumerable<T>>> distribute)
        {
            return new AnonymousDistributor<T>(start, onReceive, stop, distribute);
        }

        internal static IDistributor<IStreamQueryPartition<TPartition>> DistributeQueriesInProcess<TPartition>(
            this IEnumerable<IStreamQueryPartition<TPartition>> partitions,
            int maxDegreesOfParallelism = 5,
            Func<IStreamQueryPartition<TPartition>, string> named = null,
            string pool = "",
            TimeSpan? waitInterval = null,
            TimeSpan? defaultLeaseDuration = null)
        {
            var leasables = partitions.Leasable(named);

            return new InMemoryDistributor<IStreamQueryPartition<TPartition>>(
                leasables,
                pool,
                maxDegreesOfParallelism,
                waitInterval,
                defaultLeaseDuration);
        }

        private static Leasable<IStreamQueryPartition<TPartition>>[] Leasable<TPartition>(
            this IEnumerable<IStreamQueryPartition<TPartition>> partitions,
            Func<IStreamQueryPartition<TPartition>, string> named = null)
        {
            if (partitions == null)
            {
                throw new ArgumentNullException("partitions");
            }

            named = named ?? (p => p.ToString());

            return partitions.Select(p => new Leasable<IStreamQueryPartition<TPartition>>(p, named(p)))
                             .ToArray();
        }

        public static IDistributor<T> Trace<T>(
            this IDistributor<T> distributor,
            Action<Lease<T>> onLeaseAcquired = null,
            Action<Lease<T>> onLeaseReleasing = null)
        {
            if (distributor == null)
            {
                throw new ArgumentNullException("distributor");
            }

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

        private static void TraceOnLeaseAcquired<T>(Lease<T> lease)
        {
            System.Diagnostics.Trace.WriteLine("[Distribute] OnReceive " + lease);
        }

        private static void TraceOnLeaseReleasing<T>(Lease<T> lease)
        {
            System.Diagnostics.Trace.WriteLine("[Distribute] OnReceive (done) " + lease);
        }
    }
}