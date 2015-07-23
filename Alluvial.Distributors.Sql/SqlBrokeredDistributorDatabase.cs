using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Alluvial.Distributors.Sql
{
    public class SqlBrokeredDistributorDatabase
    {
        public string ConnectionString { get; set; }

        public async Task CreateDatabase()
        {
            var builder = new SqlConnectionStringBuilder(ConnectionString);

            var initialCatalog = builder.InitialCatalog;
            builder.InitialCatalog = "master";

            using (var connection = new SqlConnection(builder.ConnectionString))
            {
                await connection.OpenAsync();

                var cmd = connection.CreateCommand();
                cmd.CommandText = string.Format(
                    @"IF db_id('{0}') IS NULL
    CREATE DATABASE [{0}]", initialCatalog);
                cmd.CommandType = CommandType.Text;

                await cmd.ExecuteScalarAsync();
            }
        }

        public async Task InitializeSchema()
        {
            var builder = new SqlConnectionStringBuilder(ConnectionString);

            using (var connection = new SqlConnection(builder.ConnectionString))
            {
                await InitializeSchema(connection);
            }
        }

        public static async Task InitializeSchema(SqlConnection connection)
        {
            var scriptStream = typeof (SqlBrokeredDistributorDatabase).Assembly.GetManifestResourceStream(@"Alluvial.Distributors.Sql.CreateDatabase.sql");
            var scripts = new StreamReader(scriptStream)
                .ReadToEnd()
                .Replace(@"[{DatabaseName}]", string.Format(@"[{0}]", connection.Database))
                .Split(new[] { "GO" }, StringSplitOptions.RemoveEmptyEntries);
            await connection.OpenAsync();

            foreach (var script in scripts)
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = script;
                cmd.CommandType = CommandType.Text;

                Trace.WriteLine(script);

                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}