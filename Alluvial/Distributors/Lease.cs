using System;
using System.Threading.Tasks;

namespace Alluvial.Distributors
{
    public class Lease
    {
        private readonly LeasableResource leasableResource;
        private TimeSpan duration;
        private readonly Action<TimeSpan> extend;
        private bool completed;

        public Lease(
            LeasableResource leasableResource,
            TimeSpan duration,
            Action<TimeSpan> extend) 
        {
            if (leasableResource == null)
            {
                throw new ArgumentNullException("leasableResource");
            }
            if (extend == null)
            {
                throw new ArgumentNullException("extend");
            }
            this.leasableResource = leasableResource;
            this.duration = duration;
            this.extend = extend;
            completed = false;
        }

        public LeasableResource LeasableResource
        {
            get
            {
                return leasableResource;
            }
        }

        public TimeSpan Duration
        {
            get
            {
                return duration;
            }
        }

        public async Task Extend(TimeSpan by)
        {
            Console.WriteLine(string.Format("[Distribute] requesting extension: {0}: ", this) + duration);

            if (completed)
            {
                throw new InvalidOperationException("The lease cannot be extended.");
            }

            duration += by;
            extend(by);

            Console.WriteLine(string.Format("[Distribute] extended: {0}: ", this) + duration);
        }

        public override string ToString()
        {
            return LeasableResource.ToString();
        }

        internal void NotifyCompleted()
        {
            completed = true;
        }
    }
}