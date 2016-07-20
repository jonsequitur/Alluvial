using System;
using System.Data.SqlTypes;

namespace Alluvial.PartitionBuilders
{
    internal class SqlGuidRangePartition : StreamQueryRangePartition<Guid>
    {
        public SqlGuidRangePartition(
            Guid lowerBoundExclusive,
            Guid upperBoundInclusive) :
                base(lowerBoundExclusive, upperBoundInclusive)
        {
        }

        protected override bool IsWithinUpperBound(Guid value)
        {
            return new SqlGuid(value).CompareTo(new SqlGuid(UpperBoundInclusive)) <= 0;
        }

        protected override bool IsWithinLowerBound(Guid value)
        {
            return new SqlGuid(value).CompareTo(new SqlGuid(LowerBoundExclusive)) > 0;
        }
    }
}