namespace Alluvial
{
    /// <summary>
    /// Provides access to streams.
    /// </summary>
    /// <typeparam name="TKey">The type of the stream id by which streams are accessed.</typeparam>
    /// <typeparam name="TData">The type of the data returned by the streams.</typeparam>
    public interface IStreamSource<in TKey, TData>
    {
        /// <summary>
        /// Opens a stream having the specified key.
        /// </summary>
        IStream<TData> Open(TKey key);
    }
}