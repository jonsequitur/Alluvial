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
        /// Cancels the lease.
        /// </summary>
        void Cancel();

        /// <summary>
        /// Gets a task that completes when the lease is released or expired.
        /// </summary>
        Task Expiration();

        /// <summary>
        /// Extends the lease.
        /// </summary>
        /// <param name="by">The amount of time by which to extend the lease.</param>
        /// <returns></returns>
        /// <exception cref="System.InvalidOperationException">The lease cannot be extended.</exception>
        Task Extend(TimeSpan by);
    }
}