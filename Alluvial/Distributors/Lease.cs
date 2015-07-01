using System;
using System.Threading.Tasks;

namespace Alluvial.Distributors
{
    public struct Lease
    {
        private readonly LeasableResource leasableResource;
        private TimeSpan duration;
        private readonly Action<TimeSpan> extend;

        public Lease(LeasableResource leasableResource, TimeSpan duration, Action<TimeSpan> extend) : this()
        {
            if (leasableResource == null)
            {
                throw new ArgumentNullException("leasableResource");
            }
            this.leasableResource = leasableResource;
            this.duration = duration;
            this.extend = extend;
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
            Console.WriteLine(string.Format("[Distribute] extending: {0}: ", this) + duration);

            duration += by;
            extend(by);

            Console.WriteLine(string.Format("[Distribute] extended: {0}: ", this) + duration);
        }

        public override string ToString()
        {
            return LeasableResource.ToString();
        }
    }
}