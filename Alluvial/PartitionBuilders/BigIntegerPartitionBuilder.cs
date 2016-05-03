using System.Collections.Generic;
using System.Linq;
using System.Numerics;

namespace Alluvial.PartitionBuilders
{
    internal static class BigIntegerPartitionBuilder
    {
        public static IEnumerable<IStreamQueryRangePartition<BigInteger>> ByRange(
            BigInteger lowerBoundExclusive,
            BigInteger upperBoundInclusive,
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

                yield return new StreamQueryRangePartition<BigInteger>
                {
                    LowerBoundExclusive = lower,
                    UpperBoundInclusive = upper
                };
            }
        }
    }
}