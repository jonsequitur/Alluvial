using System;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial.PartitionBuilders
{
    internal static class Int32PartitionBuilder
    {
        public static IEnumerable<IStreamQueryRangePartition<int>> ByRange(
            Int32 lowerBoundExclusive,
            Int32 upperBoundInclusive,
            int numberOfPartitions)
        {
            var space = upperBoundInclusive - lowerBoundExclusive;

            foreach (var i in Enumerable.Range(0, numberOfPartitions))
            {
                var lower = lowerBoundExclusive + (i*(space/numberOfPartitions));

                var upper = lowerBoundExclusive + ((i + 1)*(space/numberOfPartitions));

                if (i == numberOfPartitions - 1)
                {
                    upper = upperBoundInclusive;
                }

                yield return new StreamQueryRangePartition<int>
                {
                    LowerBoundExclusive = lower,
                    UpperBoundInclusive = upper
                };
            }
        }
    }
}