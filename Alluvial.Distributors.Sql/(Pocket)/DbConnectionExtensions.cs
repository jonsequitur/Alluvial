using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Alluvial
{
    internal static class DbConnectionExtensions
    {
        public static async Task OpenAsync(
            this DbConnection connection,
            TimeSpan backoff,
            int numberOfRetries = 5)
        {
            if (connection.State == ConnectionState.Open)
            {
                return;
            }

            var attempt = 1;

            while (attempt++ <= numberOfRetries)
            {
                try
                {
                    await connection.OpenAsync();
                    return;
                }
                catch (InvalidOperationException exception)
                {
                    if (!exception.Message.Contains("The timeout period elapsed prior to obtaining a connection from the pool."))
                    {
                        throw;
                    }
                }

                var delay = TimeSpan.FromMilliseconds(backoff.TotalMilliseconds*attempt);

                Debug.WriteLine($"[SqlConnection] backing off on connection pool acquire: {delay.TotalSeconds}s");

                await Task.Delay(delay);
            }
        }
    }
}