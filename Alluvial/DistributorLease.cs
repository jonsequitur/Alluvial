using System;

namespace Alluvial
{
    public class DistributorLease
    {
        public DistributorLease(string name, TimeSpan duration)
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
    }
}