using System;

namespace Alluvial.Distributors
{
    public class LeasableResource
    {
        public LeasableResource(string name, TimeSpan duration)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (duration == new TimeSpan())
            {
                throw new ArgumentException("duration cannot be zero.");
            }
            Name = name;
            Duration = duration;
        }

        public TimeSpan Duration { get; set; }

        public string Name { get; private set; }

        public DateTimeOffset LeaseLastGranted { get; set; }

        public DateTimeOffset LeaseLastReleased { get; set; }

        public bool IsLeaseExpired(DateTimeOffset asOf)
        {
            return LeaseLastReleased < LeaseLastGranted &&
                   LeaseLastGranted < asOf - Duration;
        }

        public override string ToString()
        {
            return string.Format("leasable resource:{0} (granted @ {1}, released @ {2})",
                                 Name,
                                 LeaseLastGranted,
                                 LeaseLastReleased);
        }
    }
}