using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial.Distributors.Sql
{
    /// <summary>
    /// Distributes time-bound work across machines using leases stored in a SQL database.
    /// </summary>
    /// <typeparam name="T">The type of of the distributed resource.</typeparam>
    public class SqlBrokeredDistributor<T> : DistributorBase<T>
    {
        private readonly SqlBrokeredDistributorDatabase database;
        private readonly string pool;
        private readonly TimeSpan defaultLeaseDuration;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlBrokeredDistributor{T}"/> class.
        /// </summary>
        /// <param name="leasables">The leasable resources.</param>
        /// <param name="database">The database.</param>
        /// <param name="pool">The name of the pool of leasable resources from which leases are acquired.</param>
        /// <param name="maxDegreesOfParallelism">The maximum degrees of parallelism per machine.</param>
        /// <param name="waitInterval">The interval to wait before a released lease can be reacquired.</param>
        /// <param name="defaultLeaseDuration">The default duration of a lease. If not specified, the default duration is five minutes.</param>
        /// <exception cref="System.ArgumentNullException">
        /// database
        /// or
        /// pool
        /// </exception>
        public SqlBrokeredDistributor(
            Leasable<T>[] leasables,
            SqlBrokeredDistributorDatabase database,
            string pool,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null,
            TimeSpan? defaultLeaseDuration = null)
            : base(leasables, maxDegreesOfParallelism, waitInterval)
        {
            if (database == null)
            {
                throw new ArgumentNullException(nameof(database));
            }
            if (pool == null)
            {
                throw new ArgumentNullException(nameof(pool));
            }
            this.database = database;
            this.pool = pool;
            this.defaultLeaseDuration = defaultLeaseDuration ?? TimeSpan.FromMinutes(5);
        }

        /// <summary>
        /// Attempts to acquire a lease.
        /// </summary>
        /// <returns>A task whose result is either a lease (if acquired) or null.</returns>
        protected override async Task<Lease<T>> AcquireLease()
        {
            using (var connection = new SqlConnection(database.ConnectionString))
            {
                await connection.OpenAsync();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = @"Alluvial.AcquireLease";
                    cmd.Parameters.AddWithValue(@"@waitIntervalMilliseconds", WaitInterval.TotalMilliseconds);
                    cmd.Parameters.AddWithValue(@"@leaseDurationMilliseconds", defaultLeaseDuration.TotalMilliseconds);
                    cmd.Parameters.AddWithValue(@"@pool", pool);

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var resourceName = await reader.GetFieldValueAsync<string>(0);
                            var leaseLastGranted = await reader.GetFieldValueAsync<dynamic>(2);
                            var leaseLastReleased = await reader.GetFieldValueAsync<dynamic>(3);
                            var token = await reader.GetFieldValueAsync<dynamic>(5);

                            var resource = Leasables.Single(l => l.Name == resourceName);

                            resource.LeaseLastGranted = leaseLastGranted is DBNull
                                                            ? DateTimeOffset.MinValue
                                                            : (DateTimeOffset) leaseLastGranted;
                            resource.LeaseLastReleased = leaseLastReleased is DBNull
                                                             ? DateTimeOffset.MinValue
                                                             : (DateTimeOffset) leaseLastReleased;

                            Lease<T> lease = null;
                            lease = new Lease<T>(resource,
                                                 defaultLeaseDuration,
                                                 (int) token,
                                                 extend: by => ExtendLease(lease, @by));
                            return lease;
                        }
                    }
                }

                return null;
            }
        }

        private async Task ExtendLease(Lease<T> lease, TimeSpan by)
        {
            using (var connection = new SqlConnection(database.ConnectionString))
            {
                await connection.OpenAsync();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = @"Alluvial.ExtendLease";
                    cmd.Parameters.AddWithValue(@"@resourceName", lease.ResourceName);
                    cmd.Parameters.AddWithValue(@"@byMilliseconds", by.TotalMilliseconds);
                    cmd.Parameters.AddWithValue(@"@token", lease.OwnerToken);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        /// <summary>
        /// Releases the specified lease.
        /// </summary>
        protected override async Task ReleaseLease(Lease<T> lease)
        {
            using (var connection = new SqlConnection(database.ConnectionString))
            {
                await connection.OpenAsync();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = @"Alluvial.ReleaseLease";
                    cmd.Parameters.AddWithValue(@"@resourceName", lease.ResourceName);
                    cmd.Parameters.AddWithValue(@"@token", lease.OwnerToken);

                    try
                    {
                        var leaseLastReleased = (DateTimeOffset) await cmd.ExecuteScalarAsync();
                        lease.NotifyReleased(leaseLastReleased);
                        Debug.WriteLine($"[Distribute] ReleaseLease: {lease}");
                    }
                    catch (Exception exception)
                    {
                        Debug.WriteLine($"[Distribute] ReleaseLease (failed): {lease}\n{exception}");
                    }
                }
            }
        }
    }
}