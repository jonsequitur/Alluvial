using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial.Distributors
{
    /// <summary>
    /// A time-bound exclusive lease to a known resource.
    /// </summary>
    /// <typeparam name="T">The type of the resource.</typeparam>
    public class Lease<T>
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly Leasable<T> leasable;
        private readonly int ownerToken;
        private readonly Func<TimeSpan, Task<TimeSpan>> extend;
        private TimeSpan duration;

        /// <summary>
        /// Initializes a new instance of the <see cref="Lease{T}"/> class.
        /// </summary>
        /// <param name="leasable">The leasable resource.</param>
        /// <param name="duration">The duration of the lease.</param>
        /// <param name="ownerToken">The owner token.</param>
        /// <param name="extend">The extend.</param>
        /// <exception cref="System.ArgumentNullException">leasable</exception>
        public Lease(
            Leasable<T> leasable,
            TimeSpan duration,
            int ownerToken,
            Func<TimeSpan, Task<TimeSpan>> extend = null)
        {
            if (leasable == null)
            {
                throw new ArgumentNullException(nameof(leasable));
            }

            this.leasable = leasable;
            this.duration = duration;
            this.ownerToken = ownerToken;
            this.extend = extend;
            LastGranted = this.leasable.LeaseLastGranted;
            LastReleased = this.leasable.LeaseLastReleased;
            cancellationTokenSource.CancelAfter(Duration);
        }

        /// <summary>
        /// Gets the time at which the lease was last released.
        /// </summary>
        public DateTimeOffset LastReleased { get; }

        /// <summary>
        /// Gets the time at which the lease was last granted.
        /// </summary>
        public DateTimeOffset LastGranted { get; }

        /// <summary>
        /// Used by the distributor to notify release of the lease.
        /// </summary>
        public void NotifyReleased(DateTimeOffset? at = null) => 
            leasable.LeaseLastReleased = at ?? DateTimeOffset.UtcNow;

        /// <summary>
        /// Cancels the lease.
        /// </summary>
        public void Cancel() => cancellationTokenSource.Cancel();

        /// <summary>
        /// Gets the duration for which the lease is granted.
        /// </summary>
        public TimeSpan Duration => duration;

        /// <summary>
        /// Gets a task that completes when the lease is released or expired.
        /// </summary>
        public async Task Expiration()
        {
            try
            {
                await Task.Delay(TimeSpan.FromMinutes(60), CancellationToken);
            }
            catch (TaskCanceledException)
            {
            }
        }

        /// <summary>
        /// Gets a token that the owner of the lease uses for operations relating to the lease, such as cancelation and renewal.
        /// </summary>
        public int OwnerToken => ownerToken;

        /// <summary>
        /// Gets a cancellation token that can be used to cancel the task associated with the lease.
        /// </summary>
        public CancellationToken CancellationToken => cancellationTokenSource.Token;

        /// <summary>
        /// Extends the lease.
        /// </summary>
        /// <param name="by">The amount of time by which to extend the lease.</param>
        /// <returns></returns>
        /// <exception cref="System.InvalidOperationException">The lease cannot be extended.</exception>
        public async Task Extend(TimeSpan by)
        {
            if (by < TimeSpan.Zero)
            {
                throw new ArgumentException("Lease cannot be extended by a negative timespan.");
            }

            if (cancellationTokenSource.IsCancellationRequested)
            {
                throw new InvalidOperationException("The lease cannot be extended.");
            }

            if (extend != null)
            {
                @by = await extend(@by);
            }

            duration += by;
            cancellationTokenSource.CancelAfter(duration);

            Debug.WriteLine($"[Lease] extended by {by}: {this}");
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString() => $"lease:{ResourceName} ({OwnerToken}) total duration {duration} (last granted @ {LastGranted}, last released @ {LastReleased})";

        internal Leasable<T> Leasable => leasable;

        /// <summary>
        /// Gets the resource to which exclusive access is being leased.
        /// </summary>
        public T Resource => leasable.Resource;

        /// <summary>
        /// Gets the name of the resource to which exclusive access is being leased.
        /// </summary>
        public string ResourceName => leasable.Name;

        internal void NotifyGranted(DateTimeOffset? at = null) => 
            leasable.LeaseLastGranted = at ?? DateTimeOffset.UtcNow;
    }
}