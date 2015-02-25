using System.Security.Cryptography.X509Certificates;

namespace Alluvial
{
    /// <summary>
    /// A query specification for a stream.
    /// </summary>
    public interface IStreamQuery<TCursor>
    {
        /// <summary>
        /// Gets the cursor for the query.
        /// </summary>
        ICursor<TCursor> Cursor { get; }

        /// <summary>
        /// Gets or sets the maximum number of items to retrieve in a single batch.
        /// </summary>
        int? BatchCount { get; set; }
    }
}