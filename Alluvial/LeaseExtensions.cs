using System;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Provides methods for working with leases.
    /// </summary>
    public static class LeaseExtensions
    {
        /// <summary>
        /// Keeps the lease alive by periodically calling <see cref="ILease.ExpireIn" />.
        /// </summary>
        /// <param name="lease">The lease.</param>
        /// <param name="frequency">The frequency with which to refresh the lease.</param>
        public static IDisposable KeepAlive(
            this ILease lease,
            TimeSpan frequency)
        {
            var disposed = false;

            var timeToExpiration = TimeSpan.FromMilliseconds(frequency.TotalMilliseconds*2);

            Task.Factory.StartNew(async () =>
                {
                    do
                    {
                        await lease.ExpireIn(timeToExpiration);
                        await Task.Delay(frequency);
                    } while (!disposed);
                }, TaskCreationOptions.LongRunning);

            return new AnonymousDisposable(() => { disposed = true; });
        }
    }
}