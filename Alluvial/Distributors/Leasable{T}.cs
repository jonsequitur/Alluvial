using System;

namespace Alluvial.Distributors
{
    public class Leasable<T>
    {
        public Leasable(
            T resource,
            string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            Resource = resource;
            Name = name;
        }

        public string Name { get; private set; }

        public DateTimeOffset LeaseLastGranted { get; set; }

        public DateTimeOffset LeaseLastReleased { get; set; }

        public T Resource { get; private set; }

        public override string ToString()
        {
            return string.Format("leasable resource:{0} (granted @ {1}, released @ {2})",
                                 Name,
                                 LeaseLastGranted,
                                 LeaseLastReleased);
        }
    }
}