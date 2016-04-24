using System;
using System.Data.SqlTypes;

namespace Alluvial.PartitionBuilders
{
    internal class SqlGuidRangePartition : StreamQueryRangePartition<Guid>
    {
        public override bool Contains(Guid value) =>
            new SqlGuid(value).CompareTo(new SqlGuid(LowerBoundExclusive)) > 0 &&
            new SqlGuid(value).CompareTo(new SqlGuid(UpperBoundInclusive)) <= 0;
    }
}