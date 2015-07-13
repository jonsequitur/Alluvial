using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;

namespace Alluvial
{
    /// <summary>
    /// Provides methods for working with stream queries.
    /// </summary>
    [DebuggerStepThrough]
    public static class StreamQuery
    {
        /// <summary>
        /// Creates a query over the specified stream.
        /// </summary>
        public static IStreamIterator<TData, TCursor> CreateQuery<TData, TCursor>(
            this IStream<TData, TCursor> stream,
            ICursor<TCursor> cursor = null,
            int? batchCount = null)
        {
            return new StreamQuery<TData, TCursor>(
                stream,
                cursor ?? stream.NewCursor())
            {
                BatchCount = batchCount
            };
        }

        /// <summary>
        /// Creates a stream query partition having the specified boundaries.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition boundaries.</typeparam>
        /// <param name="lowerBoundExclusive">The lower, exclusive boundary.</param>
        /// <param name="upperBoundInclusive">The upper, inclusive boundary.</param>
        /// <returns></returns>
        public static IStreamQueryPartition<TPartition> Partition<TPartition>(
            TPartition lowerBoundExclusive,
            TPartition upperBoundInclusive)
        {
            return new StreamQueryPartition<TPartition>
            {
                LowerBoundExclusive = lowerBoundExclusive,
                UpperBoundInclusive = upperBoundInclusive
            };
        }

        public static IEnumerable<IStreamQueryPartition<TPartition>> Among<TPartition>(this IStreamQueryPartition<TPartition> partition,
                                                                                       int numberOfPartitions)
        {
            if (typeof (TPartition) == typeof (Guid))
            {
                return (IEnumerable<IStreamQueryPartition<TPartition>>) new GuidPartitionBuilder().Build(
                    (dynamic) partition.LowerBoundExclusive,
                    (dynamic) partition.UpperBoundInclusive,
                    numberOfPartitions);
            }

            if (typeof (TPartition) == typeof (int))
            {
                return (IEnumerable<IStreamQueryPartition<TPartition>>) new Int32PartitionBuilder().Build(
                    (dynamic) partition.LowerBoundExclusive,
                    (dynamic) partition.UpperBoundInclusive,
                    numberOfPartitions);
            }
            
            if (typeof (TPartition) == typeof (long))
            {
                return (IEnumerable<IStreamQueryPartition<TPartition>>) new Int64PartitionBuilder().Build(
                    (dynamic) partition.LowerBoundExclusive,
                    (dynamic) partition.UpperBoundInclusive,
                    numberOfPartitions);
            }

            // TODO: (Among) make this expandable

            throw new ArgumentException("Partitions of type {0} cannot be generated dynamically.");
        }
    }

    internal class GuidPartitionBuilder
    {
        public IEnumerable<IStreamQueryPartition<Guid>> Build(
            Guid lowerBoundExclusive,
            Guid upperBoundInclusive,
            int numberOfPartitions)
        {
            var upperBigIntInclusive = upperBoundInclusive.ToBigInteger();
            var lowerBigIntExclusive = lowerBoundExclusive.ToBigInteger();
            var space = upperBigIntInclusive - lowerBigIntExclusive;

            foreach (var i in Enumerable.Range(0, numberOfPartitions))
            {
                var lower = lowerBigIntExclusive + (i*(space/numberOfPartitions));

                var upper = lowerBigIntExclusive + ((i + 1)*(space/numberOfPartitions));

                if (i == numberOfPartitions - 1)
                {
                    upper = upperBigIntInclusive;
                }

                yield return new StreamQueryPartition<Guid>
                {
                    LowerBoundExclusive = lower.ToGuid(),
                    UpperBoundInclusive = upper.ToGuid()
                };
            }
        }
    }

    internal class Int32PartitionBuilder
    {
        public IEnumerable<IStreamQueryPartition<Int32>> Build(
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

                yield return new StreamQueryPartition<Int32>
                {
                    LowerBoundExclusive = lower,
                    UpperBoundInclusive = upper
                };
            }
        }
    }

    internal class Int64PartitionBuilder
    {
        public IEnumerable<IStreamQueryPartition<Int64>> Build(
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