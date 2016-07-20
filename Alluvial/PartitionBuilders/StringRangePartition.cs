using System;

namespace Alluvial.PartitionBuilders
{
    internal class StringRangePartition : StreamQueryRangePartition<string>
    {
        public StringRangePartition(
            string lowerBoundExclusive,
            string upperBoundInclusive) :
                base(lowerBoundExclusive, upperBoundInclusive)
        {
        }

        protected override bool IsWithinLowerBound(string value) =>
            string.Compare(value, LowerBoundExclusive, StringComparison.InvariantCultureIgnoreCase) > 0;

        protected override bool IsWithinUpperBound(string value) =>
            string.Compare(value, UpperBoundInclusive, StringComparison.InvariantCultureIgnoreCase) <= 0;
    }
}