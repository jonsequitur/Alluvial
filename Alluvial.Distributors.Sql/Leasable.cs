using System;

namespace Alluvial.Distributors.Sql
{
    public class Leasable
    {
        public string ResourceName { get; set; }
        public string Pool { get; set; }
        public DateTimeOffset LeaseLastGranted { get; set; }
        public DateTimeOffset LeaseLastReleased { get; set; }
        public DateTimeOffset LeaseExpires { get; set; }
    }
}