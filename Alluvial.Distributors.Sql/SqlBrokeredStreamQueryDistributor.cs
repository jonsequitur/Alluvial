using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial.Distributors.Sql
{
    public class SqlBrokeredStreamQueryDistributor : StreamQueryDistributorBase
    {
        private readonly SqlBrokeredStreamQueryDistributorDatabase settings;
        private readonly TimeSpan defaultLeaseDuration;

        public SqlBrokeredStreamQueryDistributor(
            LeasableResource[] LeasablesResource,
            SqlBrokeredStreamQueryDistributorDatabase settings,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null,
            TimeSpan? defaultLeaseDuration = null)
            : base(LeasablesResource, maxDegreesOfParallelism, waitInterval)
        {
            if (settings == null)
            {
                throw new ArgumentNullException("settings");
            }
            this.settings = settings;
            this.defaultLeaseDuration = defaultLeaseDuration ?? TimeSpan.FromMinutes(5);
        }

        protected override async Task<Lease> AcquireLease()
        {
            using (var connection = new SqlConnection(settings.ConnectionString))
            {
                await connection.OpenAsync();

                var cmd = connection.CreateCommand();
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"Alluvial.AcquireLease";
                cmd.Parameters.AddWithValue(@"@waitIntervalMilliseconds", waitInterval.TotalMilliseconds);
                cmd.Parameters.AddWithValue(@"@leaseDurationMilliseconds", defaultLeaseDuration.TotalMilliseconds);
                cmd.Parameters.AddWithValue(@"@scope", Scope);

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var resourceName = await reader.GetFieldValueAsync<string>(0);
                        var leaseLastGranted = await reader.GetFieldValueAsync<dynamic>(2);
                        var leaseLastReleased = await reader.GetFieldValueAsync<dynamic>(3);
                        var token = await reader.GetFieldValueAsync<dynamic>(5);

                        var resource = new LeasableResource(resourceName, defaultLeaseDuration)
                        {
                            LeaseLastGranted = leaseLastGranted is DBNull
                                ? DateTimeOffset.MinValue
                                : (DateTimeOffset) leaseLastGranted,
                            LeaseLastReleased = leaseLastReleased is DBNull
                                ? DateTimeOffset.MinValue
                                : (DateTimeOffset) leaseLastReleased
                        };

                        return new Lease(resource,
                                         resource.DefaultLeaseDuration,
                                         token);
                    }
                }

                return null;
            }
        }

        public string Scope { get; set; }

        protected override async Task ReleaseLease(Lease lease)
        {
            using (var connection = new SqlConnection(settings.ConnectionString))
            {
                await connection.OpenAsync();

                var cmd = connection.CreateCommand();
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"Alluvial.ReleaseLease";
                cmd.Parameters.AddWithValue(@"@resourceName", lease.LeasableResource.Name);
                cmd.Parameters.AddWithValue(@"@token", (int) lease.OwnerToken);

                try
                {
                    var leaseLastReleased = (DateTimeOffset) await cmd.ExecuteScalarAsync();
                    lease.LeasableResource.LeaseLastReleased = leaseLastReleased;
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