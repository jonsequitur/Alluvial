namespace Alluvial
{
    public interface IDistributedStreamCatchup<out TData, TPartition> : IStreamCatchup<TData>
    {
        IDistributor<IStreamQueryPartition<TPartition>> Distributor { get; }
    }
}