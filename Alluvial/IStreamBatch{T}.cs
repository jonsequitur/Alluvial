using System.Collections.Generic;

namespace Alluvial
{
    /// <summary>
    /// A batch of datam from a stream.
    /// </summary>
    /// <typeparam name="TData">The type of the stream's data.</typeparam>
    public interface IStreamBatch<out TData> : IEnumerable<TData>
    {
        /// <summary>
        /// Gets the number of items in the batch.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Indicates the cursor position at which the batch starts.
        /// </summary>
        dynamic StartsAtCursorPosition { get; }
    }
}