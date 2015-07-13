using System;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial.PartitionBuilders
{
    internal class Int64PartitionBuilder
    {
        public IEnumerable<IStreamQueryPartition<long>> Build(
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

                yield return new StreamQueryPartition<Int64>
                {
                    LowerBoundExclusive = lower,
                    UpperBoundInclusive = upper
                };
            }
        }
    }
}