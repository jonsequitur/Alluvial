using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
                throw new ArgumentNullException(nameof(connectionString));
            }
            ConnectionString = connectionString;
        }

        internal string ConnectionString { get; }

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
                cmd.CommandText =
                    $@"
IF db_id('{distributorDatabaseName}') IS NULL
    BEGIN
        CREATE DATABASE [{distributorDatabaseName
                        }]
        SELECT 'created'
    END
ELSE
    BEGIN
        SELECT 'already exists'
    END";
                cmd.CommandType = CommandType.Text;

                var result = await cmd.ExecuteScalarAsync() as string;

                if (result == "created")
                {
                    await RunScript(connection,
                                    @"Alluvial.Distributors.Sql.CreateDatabase.sql",
                                    distributorDatabaseName);

                    await InitializeSchema(connection, distributorDatabaseName);
                }
            }
        }

        public async Task<IEnumerable<Leasable>> GetLeasables()
        {
            var leasables = new List<Leasable>();

            using (var connection = new SqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = @"SELECT [ResourceName], [Pool], [LastGranted], [LastReleased], [Expires] FROM Alluvial.Leases";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var leasable = new Leasable
                            {
                                ResourceName = await reader.GetFieldValueAsync<string>(0),
                                Pool = await reader.GetFieldValueAsync<string>(1),
                                LeaseLastGranted = await reader.GetFieldValueAsync<DateTimeOffset>(2),
                                LeaseLastReleased = await reader.GetFieldValueAsync<DateTimeOffset>(3),
                                LeaseExpires = await reader.GetFieldValueAsync<DateTimeOffset>(4)
                            };

                            leasables.Add(leasable);
                        }
                    }
                }
            }

            return leasables;
        }

        /// <summary>
        /// Initializes the SQL distributor schema.
        /// </summary>
        /// <remarks>This can be used to create the necessary database objects within an existing database. They are created in the "Alluvial" namespace.</remarks>
        public async Task InitializeSchema()
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                await InitializeSchema(connection, connection.Database);
            }
        }

        /// <summary>
        /// Initializes the SQL distributor schema.
        /// </summary>
        /// <remarks>This can be used to create the necessary database objects within an existing database. They are created in the "Alluvial" namespace.</remarks>
        public static async Task InitializeSchema(DbConnection connection) =>
            await InitializeSchema(connection, connection.Database);

        /// <summary>
        /// Initializes the SQL distributor schema.
        /// </summary>
        /// <remarks>This can be used to create the necessary database objects within an existing database. They are created in the "Alluvial" namespace.</remarks>
        private static async Task InitializeSchema(DbConnection connection, string distributorDatabaseName) =>
            await RunScript(connection,
                            @"Alluvial.Distributors.Sql.InitializeSchema.sql",
                            distributorDatabaseName);

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
                await RegisterLeasableResources(leasables, pool, connection);
            }
        }

        /// <summary>
        /// Creates records for leasable resources in the distributor database, which can then be acquired and leased out via a <see cref="SqlBrokeredDistributor{T}" />.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="leasables">The leasable resources.</param>
        /// <param name="pool">The pool of resources. A distributor instances acquires leases from a single pool.</param>
        /// <param name="connection">The connection on which to execute the SQL operations to create the lease records.</param>
        public static async Task RegisterLeasableResources<T>(
            Leasable<T>[] leasables,
            string pool,
            DbConnection connection)
        {
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

                AddParameter(cmd, @"@resourceName", resource.Name);
                AddParameter(cmd, @"@pool", pool);
                AddParameter(cmd, @"@lastGranted", resource.LeaseLastGranted);
                AddParameter(cmd, @"@lastReleased", resource.LeaseLastReleased);
                AddParameter(cmd, @"@expires", DateTimeOffset.MinValue);

                await cmd.ExecuteScalarAsync();
            }
        }

        private static void AddParameter(
            DbCommand cmd,
            string name,
            object value)
        {
            var parameter = cmd.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            cmd.Parameters.Add(parameter);
        }

        private static async Task RunScript(
            DbConnection connection,
            string resourceName,
            string databaseName)
        {
            var scriptStream = typeof (SqlBrokeredDistributorDatabase).Assembly.GetManifestResourceStream(resourceName);

            var scripts = new StreamReader(scriptStream)
                .ReadToEnd()
                .Replace(@"[{DatabaseName}]", $@"[{databaseName}]")
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