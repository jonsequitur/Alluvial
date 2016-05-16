using System;
using System.Collections.Generic;

namespace Alluvial.Distributors.Sql
{
    /// <summary>
    /// Methods for working with distributors.
    /// </summary>
    public static class Distributor
    {
        /// <summary>
        /// Creates a SQL-brokered distributor.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="partitions">The partitions to be leased out.</param>
        /// <param name="database">The database where the leases are stored.</param>
        /// <param name="pool">The pool.</param>
        ///   /// <param name="waitInterval">The interval to wait after a lease is released before which leased resource should not become available again. If not specified, the default is .5 seconds.</param>
        /// <param name="defaultLeaseDuration">The default duration of a lease. If not specified, the default duration is five minutes.</param>
        /// <param name="maxDegreesOfParallelism">The maximum number of leases to be distributed at one time by this distributor instance.</param>
        /// <exception cref="System.ArgumentNullException">
        /// </exception>
        public static IDistributor<IStreamQueryPartition<TPartition>> CreateSqlBrokeredDistributor<TPartition>(
            this IEnumerable<IStreamQueryPartition<TPartition>> partitions,
            SqlBrokeredDistributorDatabase database,
            string pool,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null,
            TimeSpan? defaultLeaseDuration = null)
        {
            if (partitions == null)
            {
                throw new ArgumentNullException(nameof(partitions));
            }
            if (database == null)
            {
                throw new ArgumentNullException(nameof(database));
            }
            if (pool == null)
            {
                throw new ArgumentNullException(nameof(pool));
            }

            var leasables = partitions.CreateLeasables();

            var distributor = new SqlBrokeredDistributor<IStreamQueryPartition<TPartition>>(
                leasables,
                database,
                pool,
                maxDegreesOfParallelism,
                waitInterval,
                defaultLeaseDuration);

            return distributor;
        }
    }
}