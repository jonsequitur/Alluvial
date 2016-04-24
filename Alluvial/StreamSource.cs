using System;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with stream sources.
    /// </summary>
    public static class StreamSource
    {
        /// <summary>
        /// Creates a stream source.
        /// </summary>
        /// <typeparam name="TKey">The type of the key that identifies streams.</typeparam>
        /// <typeparam name="TData">The type of the data in the streams.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="open">A delegate that, given a key, opens the corresponding stream.</param>
        /// <returns></returns>
        public static IStreamSource<TKey, TData, TCursor> Create<TKey, TData, TCursor>(Func<TKey, IStream<TData, TCursor>> open) =>
            new AnonymousStreamSource<TKey, TData, TCursor>(open);
    }
}