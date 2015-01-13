namespace Alluvial
{
    /// <summary>
    /// Provides access to data streams.
    /// </summary>
    /// <typeparam name="TKey">The type of the stream id by which streams are accessed.</typeparam>
    /// <typeparam name="TData">The type of the data returned by the streams.</typeparam>
    public interface IDataStreamSource<in TKey, TData>
    {
        /// <summary>
        /// Opens a data stream having the specified key.
        /// </summary>
        IDataStream<TData> Open(TKey key);
    }
}