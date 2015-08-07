using System;

namespace Alluvial.PartitionBuilders
{
    internal class StringRangePartition : StreamQueryRangePartition<string>
    {
        public override bool Contains(string word)
        {
            return
                string.Compare(word, LowerBoundExclusive, StringComparison.InvariantCultureIgnoreCase) > 0 &&
                string.Compare(word, UpperBoundInclusive, StringComparison.InvariantCultureIgnoreCase) <= 0;
        }
    }
}