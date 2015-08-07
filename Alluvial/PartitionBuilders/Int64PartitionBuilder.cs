using System;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial.PartitionBuilders
{
    internal static class Int64PartitionBuilder
    {
        public static IEnumerable<IStreamQueryRangePartition<long>> ByRange(
            Int64 lowerBoundExclusive,
            Int64 upperBoundInclusive,
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

                yield return new StreamQueryRangePartition<long>
                {
                    LowerBoundExclusive = lower,
                    UpperBoundInclusive = upper
                };
            }
        }
    }
}