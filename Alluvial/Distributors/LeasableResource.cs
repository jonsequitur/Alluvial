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

        public DateTimeOffset LastGranted { get; set; }

        public DateTimeOffset LastReleased { get; set; }

        public bool IsExpired(DateTimeOffset asOf)
        {
            return LastReleased < LastGranted &&
                   LastGranted < asOf - Duration;
        }

        public override string ToString()
        {
            return string.Format("lease:{0} (granted @ {1}, released @ {2})",
                                 Name,
                                 LastGranted,
                                 LastReleased);
        }
    }
}