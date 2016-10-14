using System;

namespace Alluvial
{
    /// <summary>
    /// The exception thrown when an attempt is made to modify expired lease.
    /// </summary>
    /// <seealso cref="System.InvalidOperationException" />
    public class LeaseExpiredException : InvalidOperationException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="LeaseExpiredException"/> class.
        /// </summary>
        public LeaseExpiredException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LeaseExpiredException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public LeaseExpiredException(string message) : base(message)
        {
        }
    }
}