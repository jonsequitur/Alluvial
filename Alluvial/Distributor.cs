using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with distributors.
    /// </summary>
    public static class Distributor
    {
        /// <summary>
        /// Creates an anonymous distributor.
        /// </summary>
        /// <typeparam name="T">The type of the distributed resource.</typeparam>
        /// <param name="start">A delegate that when called starts the distributor.</param>
        /// <param name="onReceive">A delegate to be called when a lease becomes available.</param>
        /// <param name="stop">A delegate that when called stops the distributor.</param>
        /// <param name="distribute">A delegate that when called distributes the specified number of leases.</param>
        /// <returns>An anonymous distributor instance.</returns>
        public static IDistributor<T> Create<T>(
            Func<Task> start,
            Action<DistributorPipeAsync<T>> onReceive,
            Func<Task> stop,
            Func<int, Task<IEnumerable<T>>> distribute) =>
                new AnonymousDistributor<T>(start, onReceive, stop, distribute);

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
                throw new ArgumentNullException(nameof(partitions));
            }

            named = named ?? (p => p.ToString());

            return partitions.Select(p => new Leasable<IStreamQueryPartition<TPartition>>(p, named(p)))
                             .ToArray();
        }

        /// <summary>
        /// Wraps a distributor with tracing behaviors when leases are acquired and released.
        /// </summary>
        /// <exception cref="System.ArgumentNullException"></exception>
        public static IDistributor<T> Trace<T>(
            this IDistributor<T> distributor,
            Action<Lease<T>> onLeaseAcquired = null,
            Action<Lease<T>> onLeaseReleasing = null)
        {
            if (distributor == null)
            {
                throw new ArgumentNullException(nameof(distributor));
            }

            onLeaseAcquired = onLeaseAcquired ?? TraceOnLeaseAcquired;
            onLeaseReleasing = onLeaseReleasing ?? TraceOnLeaseReleasing;

            var newDistributor = Create(
                start: () =>
                {
                    System.Diagnostics.Trace.WriteLine("[Distribute] Start");
                    return distributor.Start();
                },
                stop: () =>
                {
                    System.Diagnostics.Trace.WriteLine("[Distribute] Stop");
                    return distributor.Stop();
                },
                distribute: distributor.Distribute,
                onReceive: distributor.OnReceive);

            distributor.OnReceive(async (lease, next) =>
            {
                onLeaseAcquired(lease);
                await next(lease);
                onLeaseReleasing(lease);
            });

            return newDistributor;
        }
        
        public static void OnReceive<T>(
            this IDistributor<T> distributor,
            Func<Lease<T>, Task> receive)
        {
            if (receive == null)
            {
                throw new ArgumentNullException(nameof(receive));
            }

            distributor.OnReceive(async (lease, next) =>
            {
                await next(lease);
                await receive(lease);
            });
        }

        private static void TraceOnLeaseAcquired<T>(Lease<T> lease) =>
            System.Diagnostics.Trace.WriteLine("[Distribute] OnReceive " + lease);

        private static void TraceOnLeaseReleasing<T>(Lease<T> lease) =>
            System.Diagnostics.Trace.WriteLine("[Distribute] OnReceive (done) " + lease);
    }
}