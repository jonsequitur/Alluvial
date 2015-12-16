using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial.Distributors
{
    public class Lease<T>
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly Leasable<T> leasable;
        private readonly dynamic ownerToken;
        private readonly Func<TimeSpan, Task> extend;
        private bool completed = false;
        private TimeSpan duration;

        public Lease(
            Leasable<T> leasable,
            TimeSpan duration,
            dynamic ownerToken = null,
            Func<TimeSpan, Task> extend = null)
        {
            if (leasable == null)
            {
                throw new ArgumentNullException("leasable");
            }

            this.leasable = leasable;
            this.duration = duration;
            this.ownerToken = ownerToken;
            this.extend = extend;

            cancellationTokenSource.CancelAfter(Duration);
        }

        public TimeSpan Duration
        {
            get
            {
                return duration;
            }
        }

        public dynamic OwnerToken
        {
            get
            {
                return ownerToken;
            }
        }

        public CancellationToken CancellationToken
        {
            get
            {
                return cancellationTokenSource.Token;
            }
        }

        public async Task Extend(TimeSpan by)
        {
            Debug.WriteLine(string.Format("[Distribute] requesting extension: {0}: ", this) + duration);

            if (completed || cancellationTokenSource.IsCancellationRequested)
            {
                throw new InvalidOperationException("The lease cannot be extended.");
            }

            if (extend != null)
            {
                extend(by);
            }

            duration += by;
            cancellationTokenSource.CancelAfter(by);

            Debug.WriteLine(string.Format("[Distribute] extended: {0}: ", this) + duration);
        }

        public override string ToString()
        {
            return leasable + " (" + ownerToken + ")";
        }

        internal Leasable<T> Leasable
        {
            get
            {
                return leasable;
            }
        }

        public T Resource
        {
            get
            {
                return leasable.Resource;
            }
        }

        public string ResourceName
        {
            get
            {
                return leasable.Name;
            }
        }

        public DateTimeOffset LastGranted
        {
            get
            {
                return leasable.LeaseLastGranted;
            }
        }

        internal void NotifyCompleted()
        {
            completed = true;
        }

        internal void NotifyGranted(DateTimeOffset? at = null)
        {
            leasable.LeaseLastGranted = at ?? DateTimeOffset.UtcNow;
        }

        public void NotifyReleased(DateTimeOffset? at = null)
        {
            leasable.LeaseLastReleased = at ?? DateTimeOffset.UtcNow;
        }
    }
}