using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using Alluvial.PartitionBuilders;

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
            int? batchSize = null)
        {
            return new StreamQuery<TData, TCursor>(
                stream,
                cursor ?? stream.NewCursor())
            {
                BatchSize = batchSize
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

        /// <summary>
        /// Splits a query partition into several smaller, non-overlapping, gapless partitions.
        /// </summary>
        public static IEnumerable<IStreamQueryPartition<TPartition>> Among<TPartition>(
            this IStreamQueryPartition<TPartition> partition,
            int numberOfPartitions)
        {
            if (typeof (TPartition) == typeof (Guid))
            {
                return (IEnumerable<IStreamQueryPartition<TPartition>>) new SqlGuidPartitionBuilder.Builder().Build(
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

            if (typeof (TPartition) == typeof (BigInteger))
            {
                return (IEnumerable<IStreamQueryPartition<TPartition>>) new BigIntegerPartitionBuilder().Build(
                    (dynamic) partition.LowerBoundExclusive,
                    (dynamic) partition.UpperBoundInclusive,
                    numberOfPartitions);
            }

            throw new ArgumentException("Partitions of type {0} cannot be generated dynamically.");
        }
    }
}