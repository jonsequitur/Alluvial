using System;
using System.Collections.Generic;
using System.Threading.Tasks;

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
        void OnReceive(DistributorPipeAsync<T> onReceive);

        /// <summary>
        ///  Specifies a delegate to be called when an exception is thrown when acquiring, distributing out, or releasing a lease.
        /// </summary>
        /// <remarks>
        /// The lease argument may be null if the exception occurs during lease acquisition.
        /// </remarks>
        void OnException(Action<Exception, Lease<T>> onException);

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