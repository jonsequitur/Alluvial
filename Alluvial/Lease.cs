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
        private readonly Func<TimeSpan, Task<TimeSpan>> expireIn;
        private readonly Func<Task> release;
        private TimeSpan duration;

        /// <summary>
        /// Initializes a new instance of the <see cref="Lease"/> class.
        /// </summary>
        /// <param name="duration">The duration of the lease.</param>
        /// <param name="release">A delegate that is called to release the lease.</param>
        /// <param name="expireIn">A delegate which can be called to change the lease duration.</param>
        public Lease(
            TimeSpan duration, 
            Func<TimeSpan, Task<TimeSpan>> expireIn = null,
            Func<Task> release = null)
        {
            if (duration.Ticks < 0)
            {
                throw new ArgumentException("Lease duration cannot be negative.");
            }

            this.duration = duration;
            this.expireIn = expireIn;
            this.release = release;
            cancellationTokenSource.CancelAfter(duration);
        }
        
        /// <summary>
        /// Gets a cancellation token that can be used to cancel the task associated with the lease.
        /// </summary>
        public CancellationToken CancellationToken => cancellationTokenSource.Token;

        /// <summary>
        /// Gets a task that completes when the lease is released or expired.
        /// </summary>
        public async Task Expiration()
        {
            if (IsReleased)
            {
                return;
            }

            try
            {
                await Task.Delay(TimeSpan.FromMinutes(60), CancellationToken);
            }
            catch (TaskCanceledException)
            {
            }
        }

        public async Task ExpireIn(TimeSpan timespan)
        {
            // FIX: (ExpireIn) 
            if (timespan < TimeSpan.Zero)
            {
                throw new ArgumentException("Lease cannot be extended by a negative timespan.");
            }
            
            if (cancellationTokenSource.IsCancellationRequested)
            {
                throw new InvalidOperationException("The lease cannot be extended.");
            }

            if (expireIn != null)
            {
                await expireIn(timespan);
            }

            cancellationTokenSource.CancelAfter(timespan);

            Debug.WriteLine($"[Lease] set to expire in {timespan}: {this}");
        }
        
        /// <summary>
        /// Gets an exception caught during handling of the lease, if any.
        /// </summary>
        public Exception Exception { get; internal set; }

        /// <summary>
        /// Gets a value indicating whether the lease has been released.
        /// </summary>
        /// <value>
        /// <c>true</c> if the lease has been released; otherwise, <c>false</c>.
        /// </value>
        public bool IsReleased => cancellationTokenSource.IsCancellationRequested;
  
        /// <summary>
        /// Releases the lease, making it available for acquisition by other workers.
        /// </summary>
        public async Task Release()
        {
            if (release != null)
            {
                await release();
            }

            cancellationTokenSource.Cancel();
        }

        internal static ILease CreateDefault() => new Lease(TimeSpan.FromMinutes(5));
    }
}