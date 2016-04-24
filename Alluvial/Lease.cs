using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// A time-bound exclusive lease.
    /// </summary>
    public class Lease : ILease
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly Func<TimeSpan, Task<TimeSpan>> extend;
        private TimeSpan duration;

        /// <summary>
        /// Initializes a new instance of the <see cref="Lease"/> class.
        /// </summary>
        /// <param name="duration">The duration of the lease.</param>
        /// <param name="extend">A delegate that will be called if the lease is extended.</param>
        public Lease(TimeSpan duration, Func<TimeSpan, Task<TimeSpan>> extend = null)
        {
            this.duration = duration;
            this.extend = extend;
            cancellationTokenSource.CancelAfter(Duration);
        }

        /// <summary>
        /// Gets the duration for which the lease is granted.
        /// </summary>
        public TimeSpan Duration => duration;

        /// <summary>
        /// Gets a cancellation token that can be used to cancel the task associated with the lease.
        /// </summary>
        public CancellationToken CancellationToken => cancellationTokenSource.Token;

        /// <summary>
        /// Cancels the lease.
        /// </summary>
        public void Cancel() => cancellationTokenSource.Cancel();

        /// <summary>
        /// Gets a task that completes when the lease is released or expired.
        /// </summary>
        public async Task Expiration()
        {
            try
            {
                await Task.Delay(TimeSpan.FromMinutes(60), CancellationToken);
            }
            catch (TaskCanceledException)
            {
            }
        }

        /// <summary>
        /// Extends the lease.
        /// </summary>
        /// <param name="by">The amount of time by which to extend the lease.</param>
        /// <exception cref="System.InvalidOperationException">The lease cannot be extended.</exception>
        public async Task Extend(TimeSpan by)
        {
            if (@by < TimeSpan.Zero)
            {
                throw new ArgumentException("Lease cannot be extended by a negative timespan.");
            }

            if (cancellationTokenSource.IsCancellationRequested)
            {
                throw new InvalidOperationException("The lease cannot be extended.");
            }

            if (extend != null)
            {
                @by = await extend(@by);
            }

            duration += @by;
            cancellationTokenSource.CancelAfter(duration);

            Debug.WriteLine($"[Lease] extended by {@by}: {this}");
        }

        internal static ILease CreateDefault()
        {
            return new Lease(TimeSpan.FromMinutes(5));
        }
    }
}