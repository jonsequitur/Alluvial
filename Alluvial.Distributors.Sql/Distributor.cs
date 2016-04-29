using System;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial.Distributors.Sql
{
    /// <summary>
    /// Methods for working with distributors.
    /// </summary>
    public static class Distributor
    {
        /// <summary>
        /// Distributes the work of querying specified partitions using a SQL-backed distributor.
        /// </summary>
        /// <typeparam name="TData">The type of the data in the stream.</typeparam>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="partitions">The partitions to distribute.</param>
        /// <param name="database">The database backing the SQL brokered distributor.</param>
        /// <param name="pool">The pool in which the leasable resources are registered.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">
        /// </exception>
        public static IDistributedStreamCatchup<TData, TPartition> DistributeSqlBrokeredLeasesAmong<TData, TPartition>(
            this IDistributedStreamCatchup<TData, TPartition> catchup,
            IEnumerable<IStreamQueryPartition<TPartition>> partitions,
            SqlBrokeredDistributorDatabase database,
            string pool)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }
            if (partitions == null)
            {
                throw new ArgumentNullException(nameof(partitions));
            }

            var partitionsArray = partitions as IStreamQueryPartition<TPartition>[] ?? partitions.ToArray();

            var distributor = new SqlBrokeredDistributor<IStreamQueryPartition<TPartition>>(
                partitionsArray.CreateLeasables(),
                database,
                pool);

            return catchup.DistributeAmong(partitionsArray, distributor);
        }
    }
}