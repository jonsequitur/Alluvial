using System;
using System.Collections.Generic;
using System.Numerics;
using Alluvial.PartitionBuilders;

namespace Alluvial
{
    /// <summary>
    /// Methods for creating and evaluating query partitions.
    /// </summary>
    public static class Partition
    {
        /// <summary>
        /// Creates a partition based on a predicate.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="where">A predicate that evaluates whether a given value falls within the partition.</param>
        /// <param name="named">The name of the partition.</param>
        public static IStreamQueryPartition<TPartition> Where<TPartition>(
            Func<TPartition, bool> where, string named)
        {
            return new StreamQueryPartition<TPartition>(@where, named);
        }

        /// <summary>
        /// Determines whether a value is within the specified partition.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="value">The value.</param>
        /// <param name="partition">The partition.</param>
        public static bool IsWithinPartition<TPartition>(this TPartition value, IStreamQueryPartition<TPartition> partition)
        {
            return partition.Contains(value);
        }

        /// <summary>
        /// Creates a stream query partition having the specified boundaries.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition boundaries.</typeparam>
        /// <param name="lowerBoundExclusive">The lower, exclusive boundary.</param>
        /// <param name="upperBoundInclusive">The upper, inclusive boundary.</param>
        /// <returns></returns>
        public static IStreamQueryRangePartition<TPartition> ByRange<TPartition>(
            TPartition lowerBoundExclusive,
            TPartition upperBoundInclusive) where TPartition : IComparable<TPartition>
        {
            if (typeof (TPartition) == typeof (Guid))
            {
                return (IStreamQueryRangePartition<TPartition>) new SqlGuidRangePartition
                {
                    LowerBoundExclusive = (dynamic) lowerBoundExclusive,
                    UpperBoundInclusive = (dynamic) upperBoundInclusive
                };
            }

            return new StreamQueryRangePartition<TPartition>
            {
                LowerBoundExclusive = lowerBoundExclusive,
                UpperBoundInclusive = upperBoundInclusive
            };
        }

        public static IStreamQueryPartition<TPartition> ByValue<TPartition>(TPartition value)
        {
            return new StreamQueryValuePartition<TPartition>(value);
        }

        /// <summary>
        /// Splits a query partition into several smaller, non-overlapping, gapless partitions.
        /// </summary>
        public static IEnumerable<IStreamQueryRangePartition<TPartition>> Among<TPartition>(
            this IStreamQueryRangePartition<TPartition> partition,
            int numberOfPartitions)
        {
            if (typeof (TPartition) == typeof (Guid))
            {
                dynamic partitions = SqlGuidPartitionBuilder.ByRange(
                    (dynamic) partition.LowerBoundExclusive,
                    (dynamic) partition.UpperBoundInclusive,
                    numberOfPartitions);
                return partitions;
            }

            if (typeof (TPartition) == typeof (int))
            {
                dynamic partitions = Int32PartitionBuilder.ByRange(
                    (dynamic) partition.LowerBoundExclusive,
                    (dynamic) partition.UpperBoundInclusive,
                    numberOfPartitions);
                return partitions;
            }

            if (typeof (TPartition) == typeof (long))
            {
                dynamic partitions = Int64PartitionBuilder.ByRange(
                    (dynamic) partition.LowerBoundExclusive,
                    (dynamic) partition.UpperBoundInclusive,
                    numberOfPartitions);
                return partitions;
            }

            if (typeof (TPartition) == typeof (BigInteger))
            {
                dynamic partitions = BigIntegerPartitionBuilder.ByRange(
                    (dynamic) partition.LowerBoundExclusive,
                    (dynamic) partition.UpperBoundInclusive,
                    numberOfPartitions);
                return partitions;
            }

            throw new ArgumentException(string.Format("Partitions of type {0} cannot be generated dynamically.", typeof (TPartition)));
        }
    }
}