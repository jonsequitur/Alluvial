using System;

namespace Alluvial.Streams.ItsDomainSql
{
    public class EventStreamChange : IComparable<EventStreamChange>
    {
        public Guid AggregateId { get; set; }
        public string AggregateType { get; set; }
        public long AbsoluteSequenceNumber { get; set; }

        public int CompareTo(EventStreamChange other)
        {
            return AggregateId.CompareTo(other.AggregateId);
        }

        protected bool Equals(EventStreamChange other)
        {
            return AggregateId.Equals(other.AggregateId);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            if (ReferenceEquals(this, obj))
            {
                return true;
            }
            if (obj.GetType() != GetType())
            {
                return false;
            }
            return Equals((EventStreamChange)obj);
        }

        public override int GetHashCode() => AggregateId.GetHashCode();
    }
}