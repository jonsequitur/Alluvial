using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Alluvial.Distributors;
using Alluvial.Distributors.Sql;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public class SqlBrokeredStreamQueryDistributorTests : StreamQueryDistributorTests
    {
        private SqlBrokeredStreamQueryDistributor distributor;

        private readonly SqlBrokeredStreamQueryDistributorDatabase settings = new SqlBrokeredStreamQueryDistributorDatabase
        {
            ConnectionString = @"Data Source=(localdb)\v11.0; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialSqlDistributor"
        };

        protected override IStreamQueryDistributor CreateDistributor(
            Func<Lease, Task> onReceive = null,
            LeasableResource[] leasableResources = null,
            int maxDegreesOfParallelism = 1,
            string name = null,
            TimeSpan? waitInterval = null)
        {
            leasableResources = leasableResources ?? DefaultLeasableResources;

            distributor = new SqlBrokeredStreamQueryDistributor(
                leasableResources ,
                settings,
                maxDegreesOfParallelism,
                waitInterval);
            distributor.Scope = DateTime.Now.Ticks.ToString();
            if (onReceive != null)
            {
                distributor.OnReceive(onReceive);
            }

            ProvisionLeasableResources(leasableResources, distributor.Scope);

            return distributor;
        }

        private void ProvisionLeasableResources(LeasableResource[] leasableResources, string scope)
        {
            using (var connection = new SqlConnection(settings.ConnectionString))
            {
                connection.Open();

                foreach (var resource in leasableResources)
                {
                    var cmd = connection.CreateCommand();
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = @"
INSERT INTO [Alluvial].[Leases]
            ([ResourceName] ,[Scope])
     VALUES 
           (@resourceName, @scope)";
                    cmd.Parameters.AddWithValue(@"@resourceName", resource.Name);
                    cmd.Parameters.AddWithValue(@"@scope", scope);
                    cmd.ExecuteScalar();
                }
            }
        }

        protected override TimeSpan DefaultLeaseDuration
        {
            get
            {
                return TimeSpan.FromSeconds(2);
            }
        }

        [TearDown]
        public void TearDown()
        {
            if (distributor != null)
            {
                distributor.Stop().Wait();
            }
        }
    }
}