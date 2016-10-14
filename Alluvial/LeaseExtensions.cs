using System;
using System.Diagnostics;
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
                        try
                        {
                            await lease.ExpireIn(timeToExpiration);
                            await Task.Delay(frequency);
                        }
                        catch (Exception ex) when (ex.IsTransient())
                        {
                            Debug.WriteLine($"[KeepAlive] Caught transient exception: {ex}");
                        }
                    } while (!disposed);
                }, TaskCreationOptions.LongRunning);

            return new AnonymousDisposable(() => { disposed = true; });
        }
    }
}