using System;

namespace Alluvial.Distributors.Sql
{
    /// <summary>
    /// An object to which access can be managed using a lease.
    /// </summary>
    public class Leasable
    {
        /// <summary>
        /// Gets or sets the name of the resource.
        /// </summary>
        public string ResourceName { get; set; }

        /// <summary>
        /// Gets or sets the pool from which leases to the resource are drawn.
        /// </summary>
        public string Pool { get; set; }

        /// <summary>
        /// Gets or sets the time at which the lease was last granted.
        /// </summary>
        public DateTimeOffset LeaseLastGranted { get; set; }

        /// <summary>
        /// Gets or sets the time at which the lease was last released.
        /// </summary>
        public DateTimeOffset LeaseLastReleased { get; set; }

        /// <summary>
        /// Gets or sets the time at which the lease expires.
        /// </summary>
        public DateTimeOffset LeaseExpires { get; set; }
    }
}