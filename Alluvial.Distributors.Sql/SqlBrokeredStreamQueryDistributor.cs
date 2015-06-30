using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial.Distributors.Sql
{
    public class SqlBrokeredStreamQueryDistributorDatabaseSettings
    {
        public string ConnectionString { get; set; }
    }

    public class SqlBrokeredStreamQueryDistributor : StreamQueryDistributorBase
    {
        private readonly SqlBrokeredStreamQueryDistributorDatabaseSettings settings;

        public SqlBrokeredStreamQueryDistributor(
            LeasableResource[] LeasablesResource, 
            SqlBrokeredStreamQueryDistributorDatabaseSettings settings,
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

            using (var connection = new SqlConnection(settings.ConnectionString))
            {
                var token = Guid.NewGuid();
                var cmd = connection.CreateCommand();
                cmd.CommandText = @"
SELECT TOP 1 * FROM Leases
WHERE DATEADD(MILLISECOND, 1000, LastReleased) < SYSDATETIMEOFFSET()
ORDER BY LastReleased";
                cmd.Parameters.AddWithValue(@"token", token);
            }

            var availableLease = LeasablesResource
                .Where(l => l.LastReleased + waitInterval < DateTimeOffset.UtcNow)
                .OrderBy(l => l.LastReleased)
                .FirstOrDefault(l => !workInProgress.ContainsKey(l));

            if (availableLease != null)
            {
                Debug.WriteLine("RunOne: available lease = " + availableLease.Name);

                var unitOfWork = new Lease(availableLease);

                if (workInProgress.TryAdd(availableLease, unitOfWork))
                {
                    unitOfWork.LeasableResource.LastGranted = DateTimeOffset.UtcNow;

                    try
                    {
                        await onReceive(unitOfWork);
                    }
                    catch (Exception exception)
                    {
                    }

                    Complete(unitOfWork);
                }
            }
            else
            {
                await Task.Delay(waitInterval);
            }

            Task.Run(() => RunOne());
        }

        protected override async Task Complete(Lease lease)
        {
        }
    }
}