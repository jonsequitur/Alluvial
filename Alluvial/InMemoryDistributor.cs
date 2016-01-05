using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    public class InMemoryDistributor<T> : DistributorBase<T>
    {
        private static readonly ConcurrentDictionary<string, ConcurrentDictionary<Leasable<T>, Lease<T>>> workInProgressGlobal =
            new ConcurrentDictionary<string, ConcurrentDictionary<Leasable<T>, Lease<T>>>();

        private readonly ConcurrentDictionary<Leasable<T>, Lease<T>> workInProgress;
        private readonly TimeSpan defaultLeaseDuration;

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
                throw new ArgumentNullException("pool");
            }

            workInProgress = workInProgressGlobal.GetOrAdd(pool, s => new ConcurrentDictionary<Leasable<T>, Lease<T>>());
            this.defaultLeaseDuration = defaultLeaseDuration ?? TimeSpan.FromMinutes(1);
        }

        protected override async Task<Lease<T>> AcquireLease()
        {
            var resource = leasables
                .Where(l => l.LeaseLastReleased + waitInterval < DateTimeOffset.UtcNow)
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

        protected override async Task ReleaseLease(Lease<T> lease)
        {
            if (!workInProgress.Values.Any(l => l.GetHashCode().Equals(lease.GetHashCode())))
            {
                Debug.WriteLine("[Distribute] ReleaseLease (failed): " + lease);
                return;
            }

            Lease<T> _;

            if (workInProgress.TryRemove(lease.Leasable, out _))
            {
                lease.NotifyReleased();
                Debug.WriteLine("[Distribute] ReleaseLease: " + lease);
            }

            lease.NotifyCompleted();
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