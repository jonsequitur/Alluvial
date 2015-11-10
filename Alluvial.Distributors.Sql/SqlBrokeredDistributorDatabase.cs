using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Alluvial.Distributors.Sql
{
    /// <summary>
    /// Provides methods for setting up a SQL-based distributor. 
    /// </summary>
    public class SqlBrokeredDistributorDatabase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SqlBrokeredDistributorDatabase"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <exception cref="System.ArgumentNullException">connectionString</exception>
        public SqlBrokeredDistributorDatabase(string connectionString)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException("connectionString");
            }
            ConnectionString = connectionString;
        }

        internal string ConnectionString { get; private set; }

        /// <summary>
        /// Creates the distributor database and initializes its schema.
        /// </summary>
        public async Task CreateDatabase()
        {
            var builder = new SqlConnectionStringBuilder(ConnectionString);

            var distributorDatabaseName = builder.InitialCatalog;
            builder.InitialCatalog = "master";

            using (var connection = new SqlConnection(builder.ConnectionString))
            {
                await connection.OpenAsync();

                var cmd = connection.CreateCommand();
                cmd.CommandText = string.Format(
                    @"IF db_id('{0}') IS NULL
    CREATE DATABASE [{0}]", distributorDatabaseName);
                cmd.CommandType = CommandType.Text;

                await cmd.ExecuteScalarAsync();

                await RunScript(connection,
                                @"Alluvial.Distributors.Sql.CreateDatabase.sql",
                                distributorDatabaseName);

                await InitializeSchema(connection, distributorDatabaseName);
            }
        }

        /// <summary>
        /// Initializes the SQL distributor schema.
        /// </summary>
        /// <remarks>This can be used to create the necessary database objects within an existing database. They are created in the "Alluvial" namespace.</remarks>
        public async Task InitializeSchema(SqlConnection connection)
        {
            await InitializeSchema(connection, connection.Database);
        }

        private async Task InitializeSchema(SqlConnection connection, string distributorDatabaseName)
        {
            await RunScript(connection,
                            @"Alluvial.Distributors.Sql.InitializeSchema.sql",
                            distributorDatabaseName);
        }

        /// <summary>
        /// Creates records for leasable resources in the distributor database, which can then be acquired and leased out via a <see cref="SqlBrokeredDistributor{T}" />.
        /// </summary>
        /// <param name="leasables">The leasable resources.</param>
        /// <param name="pool">The pool of resources. A distributor instances acquires leases from a single pool.</param>
        public async Task RegisterLeasableResources<T>(Leasable<T>[] leasables, string pool)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                foreach (var resource in leasables)
                {
                    var cmd = connection.CreateCommand();
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = @"
IF NOT EXISTS (SELECT * FROM [Alluvial].[Leases] 
               WHERE Pool = @pool AND 
               ResourceName = @resourceName)
    BEGIN
        INSERT INTO [Alluvial].[Leases]
                        ([ResourceName],
                         [Pool],
                         [LastGranted],
                         [LastReleased],
                         [Expires])
                 VALUES 
                        (@resourceName, 
                         @pool,
                         @lastGranted,
                         @lastReleased,
                         @expires)
    END";
                    cmd.Parameters.AddWithValue(@"@resourceName", resource.Name);
                    cmd.Parameters.AddWithValue(@"@pool", pool);
                    cmd.Parameters.AddWithValue(@"@lastGranted", resource.LeaseLastGranted);
                    cmd.Parameters.AddWithValue(@"@lastReleased", resource.LeaseLastReleased);
                    cmd.Parameters.AddWithValue(@"@expires", DateTimeOffset.MinValue);

                    await cmd.ExecuteScalarAsync();
                }
            }
        }

        private async Task RunScript(SqlConnection connection, string alluvialDistributorsSqlInitializeschemaSql, string databaseName)
        {
            var scriptStream = typeof (SqlBrokeredDistributorDatabase).Assembly.GetManifestResourceStream(alluvialDistributorsSqlInitializeschemaSql);

            var scripts = new StreamReader(scriptStream)
                .ReadToEnd()
                .Replace(@"[{DatabaseName}]", string.Format(@"[{0}]", databaseName))
                .Split(new[] { "GO" }, StringSplitOptions.RemoveEmptyEntries);

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            foreach (var script in scripts)
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = script;
                cmd.CommandType = CommandType.Text;

                Trace.WriteLine(script, typeof (SqlBrokeredDistributorDatabase).Name);

                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}