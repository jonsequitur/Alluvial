using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    /// <summary>
    /// An in-memory distributor.
    /// </summary>
    /// <typeparam name="T">The type of the resources distributed by the distributor.</typeparam>
    public class InMemoryDistributor<T> : DistributorBase<T>
    {
        private static readonly ConcurrentDictionary<string, ConcurrentDictionary<Leasable<T>, Lease<T>>> workInProgressGlobal =
            new ConcurrentDictionary<string, ConcurrentDictionary<Leasable<T>, Lease<T>>>();

        private readonly ConcurrentDictionary<Leasable<T>, Lease<T>> workInProgress;
        private readonly TimeSpan defaultLeaseDuration;

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryDistributor{T}"/> class.
        /// </summary>
        /// <param name="leasables">The leasables.</param>
        /// <param name="pool">The pool.</param>
        /// <param name="maxDegreesOfParallelism">The maximum degrees of parallelism.</param>
        /// <param name="waitInterval">The wait interval.</param>
        /// <param name="defaultLeaseDuration">Default duration of the lease.</param>
        /// <exception cref="System.ArgumentNullException"></exception>
        public InMemoryDistributor(
            Leasable<T>[] leasables,
            string pool,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null,
            TimeSpan? defaultLeaseDuration = null) :
                base(leasables,
                     maxDegreesOfParallelism,
                     waitInterval)
        {
            if (pool == null)
            {
                throw new ArgumentNullException(nameof(pool));
            }

            workInProgress = workInProgressGlobal.GetOrAdd(pool, s => new ConcurrentDictionary<Leasable<T>, Lease<T>>());
            this.defaultLeaseDuration = defaultLeaseDuration ?? TimeSpan.FromMinutes(1);
        }

        /// <summary>
        /// Attempts to acquire a lease.
        /// </summary>
        /// <returns></returns>
        protected override async Task<Lease<T>> AcquireLease()
        {
            await Task.Yield();

            var resource = Leasables
                .Where(l => l.LeaseLastReleased + WaitInterval < DateTimeOffset.UtcNow)
                .OrderBy(l => l.LeaseLastReleased)
                .FirstOrDefault(l => !workInProgress.ContainsKey(l));

            if (resource == null)
            {
                return null;
            }

            var lease = new Lease<T>(resource, defaultLeaseDuration, OwnerToken.Next());

            if (workInProgress.TryAdd(resource, lease))
            {
                lease.NotifyGranted();

                return lease;
            }

            return null;
        }

        /// <summary>
        /// Releases the specified lease.
        /// </summary>
        protected override Task ReleaseLease(Lease<T> lease)
        {
            if (workInProgress.Values.Any(l => l.GetHashCode().Equals(lease.GetHashCode())))
            {
                Lease<T> _;

                if (workInProgress.TryRemove(lease.Leasable, out _))
                {
                    lease.NotifyReleased();
                }

                lease.NotifyCompleted();
            }

            return Unit.Default.CompletedTask();
        }

        private static class OwnerToken
        {
            private static int value = int.MinValue;

            private static readonly object lockObj = new object();

            public static int Next()
            {
                lock (lockObj)
                {
                    if (value == int.MaxValue)
                    {
                        value = int.MinValue;
                    }
                    else
                    {
                        value++;
                    }

                    return value;
                }
            }
        }
    }
}