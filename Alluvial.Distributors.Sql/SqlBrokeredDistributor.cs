using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
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
        private readonly TimeSpan defaultLeaseDuration;
        private bool isDatabaseInitialized;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlBrokeredDistributor{T}"/> class.
        /// </summary>
        /// <param name="leasables">The leasable resources.</param>
        /// <param name="database">The database where the leases are stored.</param>
        /// <param name="pool">The name of the pool of leasable resources from which leases are acquired.</param>
        /// <param name="maxDegreesOfParallelism">The maximum number of leases to be distributed at one time by this distributor instance.</param>
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
            TimeSpan? defaultLeaseDuration = null)
            : base(leasables,
                   pool,
                   maxDegreesOfParallelism)
        {
            if (database == null)
            {
                throw new ArgumentNullException(nameof(database));
            }

            this.database = database;
            this.defaultLeaseDuration = defaultLeaseDuration ?? TimeSpan.FromMinutes(5);
        }

        /// <summary>
        /// Distributes the specified number of leases.
        /// </summary>
        /// <param name="count">The number of leases to distribute.</param>
        public override async Task<IEnumerable<T>> Distribute(int count)
        {
            await EnsureDatabaseIsInitialized();
            return await base.Distribute(count);
        }

        /// <summary>
        /// Starts distributing work.
        /// </summary>
        public override async Task Start()
        {
            await EnsureDatabaseIsInitialized();
            await base.Start();
        }

        /// <summary>
        /// Attempts to acquire a lease.
        /// </summary>
        /// <returns>A task whose result is either a lease (if acquired) or null.</returns>
        protected override async Task<Lease<T>> AcquireLease()
        {
            Lease<T> lease = null;

            using (var connection = new SqlConnection(database.ConnectionString))
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"Alluvial.AcquireLease";
                cmd.Parameters.AddWithValue(@"@waitIntervalMilliseconds", 0);
                cmd.Parameters.AddWithValue(@"@leaseDurationMilliseconds", defaultLeaseDuration.TotalMilliseconds);
                cmd.Parameters.AddWithValue(@"@pool", Pool);

                await connection.OpenAsync(backoff: TimeSpan.FromSeconds(1));

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
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

                        lease = new Lease<T>(resource,
                                             defaultLeaseDuration,
                                             (int) token,
                                             extend: by => ExtendLease(lease, @by));
                    }

                    reader.Dispose();
                    connection.Close();
                }

                return lease;
            }
        }

        private async Task<TimeSpan> ExtendLease(Lease<T> lease, TimeSpan by)
        {
            using (var connection = new SqlConnection(database.ConnectionString))
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"Alluvial.ExtendLease";
                cmd.Parameters.AddWithValue(@"@resourceName", lease.ResourceName);
                cmd.Parameters.AddWithValue(@"@byMilliseconds", by.TotalMilliseconds);
                cmd.Parameters.AddWithValue(@"@token", lease.OwnerToken);

                await connection.OpenAsync();
                var newCancelationTime = (DateTimeOffset) await cmd.ExecuteScalarAsync();
                connection.Close();

                return newCancelationTime - DateTimeOffset.UtcNow;
            }
        }

        /// <summary>
        /// Releases the specified lease.
        /// </summary>
        protected override async Task ReleaseLease(Lease<T> lease)
        {
            using (var connection = new SqlConnection(database.ConnectionString))
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"Alluvial.ReleaseLease";
                cmd.Parameters.AddWithValue(@"@resourceName", lease.ResourceName);
                cmd.Parameters.AddWithValue(@"@token", lease.OwnerToken);

                await connection.OpenAsync();
                var leaseLastReleased = (DateTimeOffset) await cmd.ExecuteScalarAsync();
                connection.Close();

                lease.NotifyReleased(leaseLastReleased);
            }
        }

        private async Task EnsureDatabaseIsInitialized()
        {
            if (isDatabaseInitialized)
            {
                return;
            }

            await database.InitializeSchema();
            await database.RegisterLeasableResources(
                Leasables,
                Pool);

            isDatabaseInitialized = true;
        }
    }
}