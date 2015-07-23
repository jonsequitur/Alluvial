using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial.Distributors.Sql
{
    public class SqlBrokeredDistributor<T> : DistributorBase<T>
    {
        private readonly SqlBrokeredDistributorDatabase settings;
        private readonly string scope;
        private readonly TimeSpan defaultLeaseDuration;

        public SqlBrokeredDistributor(
            Leasable<T>[] leasables,
            SqlBrokeredDistributorDatabase settings,
            string scope,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null,
            TimeSpan? defaultLeaseDuration = null)
            : base(leasables, maxDegreesOfParallelism, waitInterval)
        {
            if (settings == null)
            {
                throw new ArgumentNullException("settings");
            }
            if (scope == null)
            {
                throw new ArgumentNullException("scope");
            }
            this.settings = settings;
            this.scope = scope;
            this.defaultLeaseDuration = defaultLeaseDuration ?? TimeSpan.FromMinutes(5);
        }

        protected override async Task<Lease<T>> AcquireLease()
        {
            using (var connection = new SqlConnection(settings.ConnectionString))
            {
                await connection.OpenAsync();

                var cmd = connection.CreateCommand();
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"Alluvial.AcquireLease";
                cmd.Parameters.AddWithValue(@"@waitIntervalMilliseconds", waitInterval.TotalMilliseconds);
                cmd.Parameters.AddWithValue(@"@leaseDurationMilliseconds", defaultLeaseDuration.TotalMilliseconds);
                cmd.Parameters.AddWithValue(@"@scope", scope);

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var resourceName = await reader.GetFieldValueAsync<string>(0);
                        var leaseLastGranted = await reader.GetFieldValueAsync<dynamic>(2);
                        var leaseLastReleased = await reader.GetFieldValueAsync<dynamic>(3);
                        var token = await reader.GetFieldValueAsync<dynamic>(5);

                        var resource = this.leasables.Single(l => l.Name == resourceName);

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

                return null;
            }
        }

        private async Task ExtendLease<T>(Lease<T> lease, TimeSpan by)
        {
            using (var connection = new SqlConnection(settings.ConnectionString))
            {
                await connection.OpenAsync();

                var cmd = connection.CreateCommand();
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"Alluvial.ExtendLease";
                cmd.Parameters.AddWithValue(@"@resourceName", lease.ResourceName);
                cmd.Parameters.AddWithValue(@"@byMilliseconds", by.TotalMilliseconds);
                cmd.Parameters.AddWithValue(@"@token", (int) lease.OwnerToken);

                await cmd.ExecuteNonQueryAsync();
            }
        }

        protected override async Task ReleaseLease(Lease<T> lease)
        {
            using (var connection = new SqlConnection(settings.ConnectionString))
            {
                await connection.OpenAsync();

                var cmd = connection.CreateCommand();
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"Alluvial.ReleaseLease";
                cmd.Parameters.AddWithValue(@"@resourceName", lease.ResourceName);
                cmd.Parameters.AddWithValue(@"@token", (int) lease.OwnerToken);

                try
                {
                    var leaseLastReleased = (DateTimeOffset) await cmd.ExecuteScalarAsync();
                    lease.NotifyReleased(leaseLastReleased);
                    Debug.WriteLine("[Distribute] ReleaseLease: " + lease);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine("[Distribute] ReleaseLease (failed): " + lease + "\n" + ex);
                }
            }
        }
    }
}