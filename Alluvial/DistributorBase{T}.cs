using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

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

        private readonly TimeSpan waitBeforeStop;
        private readonly TimeSpan waitAfterFailureToAcquireLease;

        private readonly Leasable<T>[] leasables;
        private int countOfLeasesInUse;
        private DistributorPipeAsync<T> pipeline;

        private event Action<Exception, Lease<T>> CaughtException;

        /// <summary>
        /// Initializes a new instance of the <see cref="DistributorBase{T}"/> class.
        /// </summary>
        /// <param name="leasables">The leasable resources to be distributed by the distributor.</param>
        /// <param name="maxDegreesOfParallelism">The maximum number of leases to be distributed at one time by this distributor instance.</param>
        /// <param name="pool">The name of the pool of leasable resources from which leases are acquired.</param>
        /// <exception cref="System.ArgumentNullException"></exception>
        /// <exception cref="System.ArgumentException">
        /// There must be at least one leasable.
        /// or
        /// maxDegreesOfParallelism must be at least 1.
        /// </exception>
        protected DistributorBase(
            Leasable<T>[] leasables,
            string pool,
            int maxDegreesOfParallelism = 5)
        {
            if (leasables == null)
            {
                throw new ArgumentNullException(nameof(leasables));
            }
            if (leasables.Length == 0)
            {
                throw new ArgumentException("There must be at least one leasable.");
            }
            if (string.IsNullOrWhiteSpace(pool))
            {
                throw new ArgumentException("Argument 'pool' cannot be null, empty, or consist entirely of whitespace.");
            }
            if (maxDegreesOfParallelism <= 0)
            {
                throw new ArgumentException("maxDegreesOfParallelism must be at least 1.");
            }

            Pool = pool;

            this.leasables = leasables;
            this.maxDegreesOfParallelism = Math.Min(maxDegreesOfParallelism, leasables.Length);

            // ReSharper disable once PossibleLossOfFraction
            waitAfterFailureToAcquireLease = TimeSpan.FromMilliseconds(5000/maxDegreesOfParallelism);
            waitBeforeStop = TimeSpan.FromSeconds(1);
        }

        /// <summary>
        /// Gets or sets the name of the pool from which the distributor distributes leases.
        /// </summary>
        protected string Pool { get; set; }

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
                pipeline = receive.PipeInto(next: pipeline);
            }
        }

        /// <summary>
        ///  Specifies a delegate to be called when an exception is thrown when acquiring, distributing out, or releasing a lease.
        /// </summary>
        /// <remarks>
        /// The lease argument may be null if the exception occurs during lease acquisition.
        /// </remarks>
        public void OnException(Action<Exception, Lease<T>> onException) => CaughtException += onException;

        /// <summary>
        /// Distributes the specified number of leases.
        /// </summary>
        /// <param name="count">The number of leases to distribute.</param>
        public virtual async Task<IEnumerable<T>> Distribute(int count)
        {
            EnsureOnReceiveHasBeenCalled();

            stopped = false;

            var acquired = new List<T>();

            count = Math.Min(count, leasables.Length);

            while (acquired.Count < count &&
                   !stopped)
            {

                var acquisition = await TryRunOne(loop: false);
                if (acquisition.Acquired)
                {
                    acquired.Add(acquisition.Value);
                }
                else
                {
                    await Task.Delay(waitAfterFailureToAcquireLease);
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

            stopped = false;

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
                return LeaseAcquisitionAttempt.Failed();
            }

#if DEBUG
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            Debug.WriteLine($"[Distribute] {ToString()}: Trying to acquire lease");
#endif

            Lease<T> lease = null;
            try
            {
                lease = await AcquireLease();
            }
            catch (Exception exception)
            {
                PublishException(exception, lease);
            }

            if (lease != null)
            {
#if DEBUG
                Debug.WriteLine($"[Distribute] {ToString()}: Acquired lease {lease.ResourceName} @ {stopwatch.ElapsedMilliseconds}ms");
#endif

                Interlocked.Increment(ref countOfLeasesInUse);

                try
                {
                    var receive = Task.Run(async () =>
                    {
                        try
                        {
                            await pipeline(
                                lease,
                                _ => Unit.Default.CompletedTask());
                        }
                        catch (Exception ex)
                        {
                            PublishException(ex, lease);
                        }
                    });

                    var r = await Task.WhenAny(receive, lease.Expiration());

                    if (r.Status == TaskStatus.Faulted)
                    {
                        PublishException(r.Exception, lease);
                    }
                }
                catch (Exception exception)
                {
                    PublishException(exception, lease);
                }

                Interlocked.Decrement(ref countOfLeasesInUse);
            }
            else
            {
#if DEBUG
                Debug.WriteLine($"[Distribute] {ToString()}: Did not acquire lease @ {stopwatch.ElapsedMilliseconds}ms");
#endif

                if (loop)
                {
                    await Task.Delay(waitAfterFailureToAcquireLease);
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
        /// Publishes an exception caught by the distributor so that it can be observed asynchronously.
        /// </summary>
        /// <param name="exception">The exception.</param>
        /// <param name="lease">The lease, if any, that was held when the exception was thrown.</param>
        protected void PublishException(
            Exception exception, 
            Lease<T> lease = null)
        {
            if (exception.HasBeenPublished())
            {
                return;
            }

            exception.MarkAsPublished();

            if (lease != null)
            {
                lease.Exception = exception;
            }
            CaughtException?.Invoke(exception, lease);
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

            while (countOfLeasesInUse > 0)
            {
                Debug.WriteLine($"[Distribute] {ToString()}: Stop: waiting for {countOfLeasesInUse} leases to complete");
                await Task.Delay(waitBeforeStop);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose() => Task.Run(Stop).Wait();

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString() => Pool;

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