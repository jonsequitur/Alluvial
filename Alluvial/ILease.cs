using System;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// A time-bound exclusive lease.
    /// </summary>
    public interface ILease
    {
        /// <summary>
        /// Gets a task that completes when the lease is released or expired.
        /// </summary>
        Task Expiration();

        /// <summary>
        /// Sets the lease to expire after the specified period of time.
        /// </summary>
        /// <param name="timespan">The duration after which the lease should expire.</param>
        Task ExpireIn(TimeSpan timespan);

        /// <summary>
        /// Releases the lease, making it available for acquisition by other workers.
        /// </summary>
        Task Release();
    }
}