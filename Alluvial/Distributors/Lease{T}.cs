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
        private readonly Func<TimeSpan, Task> extend;
        private bool completed = false;
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
            Func<TimeSpan, Task> extend = null)
        {
            if (leasable == null)
            {
                throw new ArgumentNullException(nameof(leasable));
            }

            this.leasable = leasable;
            this.duration = duration;
            this.ownerToken = ownerToken;
            this.extend = extend;
            this.LastGranted = this.leasable.LeaseLastGranted;
            this.LastReleased = this.leasable.LeaseLastReleased;
            cancellationTokenSource.CancelAfter(Duration);
        }

        public DateTimeOffset LastReleased { get; }

        public DateTimeOffset LastGranted { get; }

        public TimeSpan Duration => duration;

        public int OwnerToken => ownerToken;

        public CancellationToken CancellationToken => cancellationTokenSource.Token;

        public async Task Extend(TimeSpan by)
        {
            Debug.WriteLine(string.Format("[Distribute] requesting extension: {0}: ", this) + duration);

            if (completed || cancellationTokenSource.IsCancellationRequested)
            {
                throw new InvalidOperationException("The lease cannot be extended.");
            }

            extend?.Invoke(@by);

            duration += by;
            cancellationTokenSource.CancelAfter(by);

            Debug.WriteLine($"[Distribute] extended: {this}: " + duration);
        }

        public override string ToString()
        {
            return $"lease:{ResourceName} (last granted @ {LastGranted}, last released @ {LastReleased}) ({OwnerToken})";
        }

        internal Leasable<T> Leasable => leasable;

        public T Resource => leasable.Resource;

        public string ResourceName => leasable.Name;

        internal void NotifyCompleted()
        {
            completed = true;
        }

        internal void NotifyGranted(DateTimeOffset? at = null)
        {
            leasable.LeaseLastGranted = at ?? DateTimeOffset.UtcNow;
        }

        public void NotifyReleased(DateTimeOffset? at = null)
        {
            leasable.LeaseLastReleased = at ?? DateTimeOffset.UtcNow;
        }
    }
}