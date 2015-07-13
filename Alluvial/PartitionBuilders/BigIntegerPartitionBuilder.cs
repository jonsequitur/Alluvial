using System.Collections.Generic;
using System.Linq;
using System.Numerics;

namespace Alluvial.PartitionBuilders
{
    internal class BigIntegerPartitionBuilder
    {
        public IEnumerable<IStreamQueryPartition<BigInteger>> Build(
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

                yield return new StreamQueryPartition<BigInteger>
                {
                    LowerBoundExclusive = lower,
                    UpperBoundInclusive = upper
                };
            }
        }
    }
}