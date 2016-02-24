using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    /// <summary>
    /// Provides common distributor functionality.
    /// </summary>
    /// <typeparam name="T">The type of the resources distributed by the distributor.</typeparam>
    /// <seealso cref="Alluvial.IDistributor{T}" />
    /// <seealso cref="System.Collections.Generic.IEnumerable{T}" />
    public abstract class DistributorBase<T> : IDistributor<T>
    {
        private readonly int maxDegreesOfParallelism;
        private bool stopped;
        private readonly TimeSpan waitInterval;
        private readonly Leasable<T>[] leasables;
        private int leasesHeld;
        private DistributorPipeAsync<T> pipeline;

        /// <summary>
        /// Initializes a new instance of the <see cref="DistributorBase{T}"/> class.
        /// </summary>
        /// <param name="leasables">The leasable resources to be distributed by the distributor.</param>
        /// <param name="maxDegreesOfParallelism">The maximum number of leases to be distributed at one time by this distributor instance.</param>
        /// <param name="waitInterval">The interval to wait after a lease is released before which leased resource should not become available again.</param>
        /// <exception cref="System.ArgumentNullException"></exception>
        /// <exception cref="System.ArgumentException">
        /// There must be at least one leasable.
        /// or
        /// maxDegreesOfParallelism must be at least 1.
        /// </exception>
        protected DistributorBase(
            Leasable<T>[] leasables,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null)
        {
            if (leasables == null)
            {
                throw new ArgumentNullException(nameof(leasables));
            }
            if (leasables.Length ==0)
            {
                throw new ArgumentException("There must be at least one leasable.");
            }
            if (maxDegreesOfParallelism <= 0)
            {
                throw new ArgumentException("maxDegreesOfParallelism must be at least 1.");
            }
            this.leasables = leasables;
            this.maxDegreesOfParallelism = Math.Min(maxDegreesOfParallelism, leasables.Length);
            this.waitInterval = waitInterval ?? TimeSpan.FromSeconds(.5);
        }

        /// <summary>
        /// Gets the interval to wait after a lease is released before which leased resource should not become available again.
        /// </summary>
        protected TimeSpan WaitInterval => waitInterval;

        /// <summary>
        /// Gets the leasables that the distributor can distribute.
        /// </summary>
        protected Leasable<T>[] Leasables => leasables;

        /// <summary>
        /// Called when a lease is available.
        /// </summary>
        /// <param name="receive">The delegate called when work is available to be done.</param>
        /// <remarks>
        /// For the duration of the lease, the leased resource will not be available to any other instance.
        /// </remarks>
        public void OnReceive(DistributorPipeAsync<T> receive)
        {
            if (receive == null)
            {
                throw new ArgumentNullException(nameof(receive));
            }

            if (pipeline == null)
            {
                pipeline = receive;
            }
            else
            {
                var previousPipeline = pipeline;

                pipeline = (lease, next) =>
                           receive(lease,
                                   l => previousPipeline(l,
                                                 _ => Unit.Default.CompletedTask()));
            }
        }

        /// <summary>
        /// Distributes the specified number of leases.
        /// </summary>
        /// <param name="count">The number of leases to distribute.</param>
        public virtual async Task<IEnumerable<T>> Distribute(int count)
        {
            EnsureOnReceiveHasBeenCalled();

            var acquired = new List<T>();

            while (acquired.Count < count)
            {
                var acquisition = await TryRunOne(loop: false);
                if (acquisition.Acquired)
                {
                    acquired.Add(acquisition.Value);
                }
                else
                {
                    await Task.Delay((int) (waitInterval.TotalMilliseconds/leasables.Length));
                }
            }

            return acquired;
        }

        /// <summary>
        /// Starts distributing work.
        /// </summary>
        public virtual Task Start()
        {
            EnsureOnReceiveHasBeenCalled();

            Parallel.For(0,
                         maxDegreesOfParallelism,
                         async _ => await TryRunOne(loop: true));

            return Unit.Default.CompletedTask();
        }

        private void EnsureOnReceiveHasBeenCalled()
        {
            if (pipeline == null)
            {
                throw new InvalidOperationException("You must call OnReceive before starting the distributor.");
            }
        }

        private async Task<LeaseAcquisitionAttempt> TryRunOne(bool loop)
        {
            if (stopped)
            {
                Debug.WriteLine("[Distribute] Aborting");
                return LeaseAcquisitionAttempt.Failed();
            }

            Debug.WriteLine("[Distribute] Trying to acquire lease");

            Lease<T> lease = null;
            try
            {
                lease = await AcquireLease();
            }
            catch (Exception exception)
            {
                Debug.WriteLine($"[Distribute] Exception during AcquireLease:\n{exception}");
            }

            if (lease != null)
            {
                Interlocked.Increment(ref leasesHeld);

                try
                {
                    var receive = pipeline(lease,
                                           _ => Unit.Default.CompletedTask());

                    await Task.WhenAny(receive, lease.Expiration());
                }
                catch (Exception exception)
                {
                    Debug.WriteLine($"[Distribute] Exception during OnReceive for lease {lease}:\n{exception}");
                }

                try
                {
                    await ReleaseLease(lease);
                }
                catch (Exception exception)
                {
                    Debug.WriteLine($"[Distribute] Exception during ReleaseLease for lease {lease}:\n{exception}");
                }

                Interlocked.Decrement(ref leasesHeld);
            }
            else
            {
                Debug.WriteLine("[Distribute] Did not acquire lease");

                if (loop)
                {
                    await Task.Delay(waitInterval);
                }
                else
                {
                    return LeaseAcquisitionAttempt.Failed();
                }
            }

            if (loop)
            {
#pragma warning disable 4014
                // async recursion. we don't await in order to truncate the call stack.
                Task.Run(() => TryRunOne(true));
#pragma warning restore 4014
            }

            if (lease != null)
            {
                return LeaseAcquisitionAttempt.Succeeded(lease.Resource);
            }

            return LeaseAcquisitionAttempt.Failed();
        }

        /// <summary>
        /// Releases the specified lease.
        /// </summary>
        protected abstract Task ReleaseLease(Lease<T> lease);

        /// <summary>
        /// Attempts to acquire a lease.
        /// </summary>
        /// <returns>A task whose result is either a lease (if acquired) or null.</returns>
        protected abstract Task<Lease<T>> AcquireLease();

        /// <summary>
        /// Completes all currently leased work and stops distributing further work.
        /// </summary>
        public async Task Stop()
        {
            stopped = true;

            while (leasesHeld > 0)
            {
                Debug.WriteLine($"[Distribute] Stop: waiting for {leasesHeld} to complete");
                await Task.Delay(waitInterval);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose() => Task.Run(() => Stop()).Wait();

        private struct LeaseAcquisitionAttempt
        {
            public T Value { get; private set; }

            public bool Acquired { get; private set; }

            public static LeaseAcquisitionAttempt Failed()
            {
                return new LeaseAcquisitionAttempt
                {
                    Acquired = false
                };
            }

            public static LeaseAcquisitionAttempt Succeeded(T value)
            {
                return new LeaseAcquisitionAttempt
                {
                    Acquired = true,
                    Value = value
                };
            }
        }
    }
}