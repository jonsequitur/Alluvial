using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    /// <summary>
    /// Distributes parallel work using time-bounded leases.
    /// </summary>
    public interface IDistributor<T> : IDisposable
    {
        /// <summary>
        /// Specifies a delegate to be called when a lease is available.
        /// </summary>
        /// <param name="onReceive">The delegate called when work is available to be done.</param>
        /// <remarks>For the duration of the lease, the leased resource will not be available to any other instance.</remarks>
        void OnReceive(Func<Lease<T>, Task> onReceive);

        /// <summary>
        /// Starts distributing work.
        /// </summary>
        Task Start();

        /// <summary>
        /// Distributes the specified number of leases.
        /// </summary>
        /// <param name="count">The number of leases to distribute.</param>
        Task<IEnumerable<T>> Distribute(int count);

        /// <summary>
        /// Completes all currently leased work and stops distributing further work.
        /// </summary>
        Task Stop();
    }
}