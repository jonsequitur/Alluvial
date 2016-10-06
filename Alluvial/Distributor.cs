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
        /// Configures the distributor to release leases as soon as the work on the lease is completed.
        /// </summary>
        /// <typeparam name="T">The type of the distributed resource.</typeparam>
        /// <param name="distributor">The distributor.</param>
        public static IDistributor<T> ReleaseLeasesWhenWorkIsDone<T>(
            this IDistributor<T> distributor)
        {
            distributor.OnReceive(async (lease, next) =>
            {
                await next(lease);
                await lease.Release();
            });

            return distributor;
        }

        /// <summary>
        /// Creates an anonymous distributor.
        /// </summary>
        /// <typeparam name="T">The type of the distributed resource.</typeparam>
        /// <param name="start">A delegate that when called starts the distributor.</param>
        /// <param name="onReceive">A delegate to be called when a lease becomes available.</param>
        /// <param name="onException">A delegate to be called when the distributor catches an exception while acquiring, distributing, or releasing a lease.</param>
        /// <param name="stop">A delegate that when called stops the distributor.</param>
        /// <param name="distribute">A delegate that when called distributes the specified number of leases.</param>
        /// <returns>An anonymous distributor instance.</returns>
        public static IDistributor<T> Create<T>(
            Func<Task> start,
            Action<DistributorPipeAsync<T>> onReceive,
            Func<Task> stop,
            Func<int, Task<IEnumerable<T>>> distribute,
            Action<Action<Exception, Lease<T>>> onException) =>
                new AnonymousDistributor<T>(start, onReceive, stop, distribute, onException);

        /// <summary>
        /// Creates an in-memory distributor.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partitions.</typeparam>
        /// <param name="partitions">The partitions to be leased out.</param>
        /// <param name="maxDegreesOfParallelism">The maximum degrees of parallelism.</param>
        /// <param name="pool">The pool.</param>
        /// <param name="defaultLeaseDuration">Default duration of the lease. If not specified, the default is 1 minute.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException"></exception>
        public static IDistributor<IStreamQueryPartition<TPartition>> CreateInMemoryDistributor<TPartition>(
            this IEnumerable<IStreamQueryPartition<TPartition>> partitions,
            int maxDegreesOfParallelism = 5,
            string pool = "default",
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
            Action<Lease<T>> onLeaseWorkDone = null,
            Action<Exception, Lease<T>> onException = null)
        {
            if (distributor == null)
            {
                throw new ArgumentNullException(nameof(distributor));
            }

            if (onLeaseAcquired == null &&
                onLeaseWorkDone == null &&
                onException == null)
            {
                onLeaseAcquired = lease => TraceOnLeaseAcquired(distributor, lease);
                onLeaseWorkDone = lease => TraceOnLeaseWorkDone(distributor, lease);
                onException = (exception, lease) => TraceOnException(distributor, exception, lease);
            }
            else
            {
                onLeaseAcquired = onLeaseAcquired ?? (l => { });
                onLeaseWorkDone = onLeaseWorkDone ?? (l => { });
                onException = onException ?? ((exception, lease) => { });
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
                onReceive: distributor.OnReceive,
                onException: distributor.OnException);

            distributor.OnReceive(async (lease, next) =>
            {
                onLeaseAcquired(lease);
                await next(lease);
                onLeaseWorkDone(lease);
            });

            distributor.OnException(onException);

            return newDistributor;
        }

        private static void TraceOnException<T>(
            IDistributor<T> distributor,
            Exception exception,
            Lease<T> lease)
        {
            if (lease != null)
            {
                WriteLine($"[Distribute] {distributor}: Exception: {lease.Exception}");
            }
            else if (exception != null)
            {
                WriteLine($"[Distribute] {distributor}: Exception: {exception}");
            }
        }

        /// <summary>
        /// Specifies a delegate to be called when a lease is available.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="distributor">The distributor.</param>
        /// <param name="receive">The delegate called when work is available to be done.</param>
        /// <exception cref="System.ArgumentNullException">
        /// </exception>
        /// <remarks>
        /// For the duration of the lease, the leased resource will not be available to any other instance.
        /// </remarks>
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

        private static void TraceOnLeaseWorkDone<T>(IDistributor<T> distributor, Lease<T> lease) =>
            WriteLine($"[Distribute] {distributor}: OnReceive (done) " + lease);
    }
}