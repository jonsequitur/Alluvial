using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using Alluvial.PartitionBuilders;

namespace Alluvial
{
    /// <summary>
    /// Methods for creating and evaluating query partitions.
    /// </summary>
    public static class Partition
    {
        /// <summary>
        /// Creates a partition containing the full range of guids.
        /// </summary>
        public static IStreamQueryRangePartition<Guid> AllGuids() =>
            ByRange(Guid.Empty,
                    Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"));

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

            throw new ArgumentException($"Partitions of type {typeof (TPartition)} cannot be generated dynamically.");
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
                return (IStreamQueryRangePartition<TPartition>) new SqlGuidRangePartition(
                    (dynamic) lowerBoundExclusive, 
                    (dynamic) upperBoundInclusive);
            }

            return new StreamQueryRangePartition<TPartition>(
                lowerBoundExclusive,
                upperBoundInclusive);
        }

        /// <summary>
        /// Creates a partition specified by a single value.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="value">The value.</param>
        public static IStreamQueryPartition<TPartition> ByValue<TPartition>(TPartition value) =>
            new StreamQueryValuePartition<TPartition>(value);

        /// <summary>
        /// Distributes values into a set of partitions.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <typeparam name="T">The types of the values to partition.</typeparam>
        /// <param name="values">The values.</param>
        /// <param name="partitions">The partitions.</param>
        /// <returns>A sequence of groupings, by partition.</returns>
        public static IEnumerable<IGrouping<TPartition, T>> DistributeInto<TPartition, T>(
            this IEnumerable<T> values,
            IEnumerable<TPartition> partitions)
             where TPartition : IStreamQueryPartition<T>
         {
             return partitions
                 .Select(partition => Grouping.Create(partition,
                                                      values.Where(partition.Contains)));
         }

        /// <summary>
        /// Determines whether a value is within the specified partition.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="value">The value.</param>
        /// <param name="partition">The partition.</param>
        public static bool IsWithinPartition<TPartition>(
            this TPartition value,
            IStreamQueryPartition<TPartition> partition) =>
                partition.Contains(value);

        /// <summary>
        /// Creates a partition based on a predicate.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="where">A predicate that evaluates whether a given value falls within the partition.</param>
        /// <param name="named">The name of the partition.</param>
        public static IStreamQueryPartition<TPartition> Where<TPartition>(
            Func<TPartition, bool> where,
            string named) =>
                new StreamQueryPartition<TPartition>(@where, named);

        /// <summary>
        /// Filters a queryable to the data within a specified range partition.
        /// </summary>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <typeparam name="TPartition">The type of the partition value.</typeparam>
        /// <param name="source">The source queryable.</param>
        /// <param name="key">A selector for the partitioned value.</param>
        /// <param name="partition">The partition.</param>
        public static IQueryable<TData> WithinPartition<TData, TPartition>(
            this IQueryable<TData> source,
            Expression<Func<TData, TPartition>> key,
            IStreamQueryRangePartition<TPartition> partition)
        {
            Expression selectKey = key.Body;
            Expression lower = Expression.Constant(partition.LowerBoundExclusive);
            Expression upper = Expression.Constant(partition.UpperBoundInclusive);

            MethodInfo compareTo;

            if (typeof (TPartition) == typeof (Guid) &&
                source is EnumerableQuery)
            {
                compareTo = MethodInfoFor<SqlGuid>.CompareTo;
                lower = Expression.Convert(lower, typeof (SqlGuid));
                upper = Expression.Convert(upper, typeof (SqlGuid));
                selectKey = Expression.Convert(selectKey, typeof (SqlGuid));
            }
            else
            {
                compareTo = MethodInfoFor<TPartition>.CompareTo;
            }

            var selectLeft = Expression.GreaterThan(
                Expression.Call(selectKey,
                                compareTo,
                                lower), Expression.Constant(0));

            var selectRight = Expression.LessThanOrEqual(
                Expression.Call(selectKey,
                                compareTo,
                                upper), Expression.Constant(0));

            var filterExpression = Expression.AndAlso(selectLeft, selectRight);

            return source.Where(
                Expression.Lambda<Func<TData, bool>>(filterExpression, key.Parameters.Single()));
        }

        private static class MethodInfoFor<T>
        {
            public static readonly MethodInfo CompareTo = typeof (T).GetMethod("CompareTo",
                                                                               new[] { typeof (T) });
        }
    }
}