using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static System.Diagnostics.Trace;

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

        /// <summary>
        /// Creates an in-memory distributor.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partitions.</typeparam>
        /// <param name="partitions">The partitions to be leased out.</param>
        /// <param name="maxDegreesOfParallelism">The maximum degrees of parallelism.</param>
        /// <param name="pool">The pool.</param>
        /// <param name="waitInterval">The wait interval. If not specified, the default is 5 seconds.</param>
        /// <param name="defaultLeaseDuration">Default duration of the lease. If not specified, the default is 1 minute.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException"></exception>
        public static IDistributor<IStreamQueryPartition<TPartition>> CreateInMemoryDistributor<TPartition>(
            this IEnumerable<IStreamQueryPartition<TPartition>> partitions,
            int maxDegreesOfParallelism = 5,
            string pool = "default",
            TimeSpan? waitInterval = null,
            TimeSpan? defaultLeaseDuration = null)
        {
            if (partitions == null)
            {
                throw new ArgumentNullException(nameof(partitions));
            }

            var leasables = partitions.CreateLeasables();

            return new InMemoryDistributor<IStreamQueryPartition<TPartition>>(
                leasables,
                pool,
                maxDegreesOfParallelism,
                waitInterval,
                defaultLeaseDuration);
        }

        public static Leasable<IStreamQueryPartition<TPartition>>[] CreateLeasables<TPartition>(
            this IEnumerable<IStreamQueryPartition<TPartition>> partitions)
        {
            if (partitions == null)
            {
                throw new ArgumentNullException(nameof(partitions));
            }

            return partitions.Select(p => new Leasable<IStreamQueryPartition<TPartition>>(p, p.ToString()))
                             .ToArray();
        }

        /// <summary>
        /// Distributes all available leases.
        /// </summary>
        /// <param name="distributor">The distributor.</param>
        public static async Task<IEnumerable<T>> DistributeAll<T>(this IDistributor<T> distributor) =>
            await distributor.Distribute(int.MaxValue);

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

            if (onLeaseAcquired == null && onLeaseReleasing == null)
            {
                onLeaseAcquired = lease => TraceOnLeaseAcquired(distributor, lease);
                onLeaseReleasing = lease => TraceOnLeaseReleasing(distributor, lease);
            }
            else
            {
                onLeaseAcquired = onLeaseAcquired ?? (l => { });
                onLeaseReleasing = onLeaseReleasing ?? (l => { });
            }

            var newDistributor = Create(
                start: () =>
                {
                    WriteLine($"[Distribute] {distributor}: Start");
                    return distributor.Start();
                },
                stop: () =>
                {
                    WriteLine($"[Distribute] {distributor}: Stop");
                    return distributor.Stop();
                },
                distribute: distributor.Distribute,
                onReceive: @async =>
                {
                    // when someone calls OnReceive on the AnonymousDistributor, pass the call along to the wrapped distributor...
                    distributor.OnReceive(@async);

                    // ... then add another call to the pipeline to check for exceptions
                    distributor.OnReceive(async (lease, next) =>
                    {
                        try
                        {
                            await next(lease);

                            if (lease.Exception != null)
                            {
                                WriteLine($"[Distribute] {distributor}: Exception: {lease.Exception}");
                            }
                        }
                        catch (Exception exception)
                        {
                            WriteLine($"[Distribute] {distributor}: Exception: {exception}");
                            throw;
                        }
                    });
                });

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
            if (distributor == null)
            {
                throw new ArgumentNullException(nameof(distributor));
            }
            if (receive == null)
            {
                throw new ArgumentNullException(nameof(receive));
            }

            distributor.OnReceive(async (lease, next) =>
            {
                await receive(lease);
                await next(lease);
            });
        }

        private static void TraceOnLeaseAcquired<T>(IDistributor<T> distributor, Lease<T> lease) =>
            WriteLine($"[Distribute] {distributor}: OnReceive " + lease);

        private static void TraceOnLeaseReleasing<T>(IDistributor<T> distributor, Lease<T> lease) =>
            WriteLine($"[Distribute] {distributor}: OnReceive (done) " + lease);
    }
}