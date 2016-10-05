using System;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// A time-bound exclusive lease to a known resource.
    /// </summary>
    /// <typeparam name="T">The type of the resource.</typeparam>
    public class Lease<T> : Lease
    {
        private readonly Leasable<T> leasable;
        private readonly int ownerToken;

        /// <summary>
        /// Initializes a new instance of the <see cref="Lease{T}" /> class.
        /// </summary>
        /// <param name="leasable">The leasable resource.</param>
        /// <param name="duration">The duration of the lease.</param>
        /// <param name="ownerToken">The owner token.</param>
        /// <param name="expireIn">A delegate which can be called to change the lease duration.</param>
        /// <param name="release">A delegate to be called to release the lease.</param>
        /// <exception cref="System.ArgumentNullException">leasable</exception>
        public Lease(
            Leasable<T> leasable,
            TimeSpan duration,
            int ownerToken,
            Func<TimeSpan, Task<TimeSpan>> expireIn = null,
            Func<Task> release = null) :
            base(duration, expireIn, release)
        {
            if (leasable == null)
            {
                throw new ArgumentNullException(nameof(leasable));
            }

            this.leasable = leasable;
            this.ownerToken = ownerToken;
          
            LastGranted = this.leasable.LeaseLastGranted;
            LastReleased = this.leasable.LeaseLastReleased;
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
        /// Gets a token that the owner of the lease uses for operations relating to the lease, such as cancelation and renewal.
        /// </summary>
        public int OwnerToken => ownerToken;

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString() => $"lease:{ResourceName} ({OwnerToken}) (last granted @ {LastGranted}, last released @ {LastReleased})";

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