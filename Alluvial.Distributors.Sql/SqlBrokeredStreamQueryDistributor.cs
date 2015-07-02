using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial.Distributors.Sql
{
    public class SqlBrokeredStreamQueryDistributorDatabase
    {
        public string ConnectionString { get; set; }
    }



    public class SqlBrokeredStreamQueryDistributor : StreamQueryDistributorBase
    {
        private readonly SqlBrokeredStreamQueryDistributorDatabase settings;

        public SqlBrokeredStreamQueryDistributor(
            LeasableResource[] LeasablesResource,
            SqlBrokeredStreamQueryDistributorDatabase settings,
            int maxDegreesOfParallelism = 5,
            TimeSpan? waitInterval = null)
            : base(LeasablesResource, maxDegreesOfParallelism, waitInterval)
        {
            if (settings == null)
            {
                throw new ArgumentNullException("settings");
            }
            this.settings = settings;
        }

        protected override async Task RunOne()
        {
            if (stopped)
            {
                Debug.WriteLine("Aborting");
                return;
            }

            Debug.WriteLine("Polling");

            var availableLease = await AcquireAvailableResource();

            if (availableLease != null)
            {
                Debug.WriteLine("RunOne: available lease = " + availableLease.Name);

                var lease = new Lease(availableLease, availableLease.DefaultDuration, null);

                if (workInProgress.TryAdd(availableLease, lease))
                {
                    lease.LeasableResource.LeaseLastGranted = DateTimeOffset.UtcNow;

                    try
                    {
                        await onReceive(lease);
                    }
                    catch (Exception exception)
                    {
                    }

                    Complete(lease);
                }
            }
            else
            {
                await Task.Delay(waitInterval);
            }

            Task.Run(() => RunOne());
        }

        private async Task<LeasableResource> AcquireAvailableResource()
        {
            using (var connection = new SqlConnection(settings.ConnectionString))
            {
                var cmd = connection.CreateCommand();
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"AcquireLease";
                cmd.Parameters.AddWithValue(@"@waitIntervalMilliseconds", waitInterval.TotalMilliseconds);
                cmd.Parameters.AddWithValue(@"@leaseDurationMilliseconds", DefaultLeaseDuration.TotalMilliseconds);
                cmd.Parameters.AddWithValue(@"@scope", Scope);

                LeasableResource availableLease = null;

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var id = await reader.GetFieldValueAsync<string>(0);

                        var leaseLastGranted = await reader.GetFieldValueAsync<DateTimeOffset>(2);
                        var leaseLastReleased = await reader.GetFieldValueAsync<DateTimeOffset>(3);
                        dynamic token = await reader.GetFieldValueAsync<dynamic>(4);

                        availableLease = new LeasableResource(id, TimeSpan.FromMinutes(1))
                        {
                            LeaseLastGranted = leaseLastGranted,
                            LeaseLastReleased = leaseLastReleased
                        };
                    }
                }

                return availableLease;
            }
        }

        public string Scope { get; set; }

        public TimeSpan DefaultLeaseDuration
        {
            get
            {
                return TimeSpan.FromMinutes(1);
            }
        }

        protected override async Task Complete(Lease lease)
        {
        }
    }
}