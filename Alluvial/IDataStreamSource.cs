namespace Alluvial
{
    public interface IDataStreamSource<in TKey, TData>
    {
        IDataStream<TData> Open(TKey key);
    }
}