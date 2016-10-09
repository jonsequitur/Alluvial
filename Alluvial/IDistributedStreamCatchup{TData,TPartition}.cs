namespace Alluvial
{
    /// <summary>
    /// An persistent query over a partitioned stream of data, which updates one or more stream aggregators and whose workload can be distributed across multiple instances.
    /// </summary>
    /// <typeparam name="TData">The type of the data that the catchup pushes to the aggregators.</typeparam>
    /// <typeparam name="TPartition">The type of the partition.</typeparam>
    /// <seealso cref="Alluvial.IStreamCatchup{TData}" />
    /// <seealso cref="IStreamQueryPartition{TPartition}" />
    public interface IDistributedStreamCatchup<out TData, TPartition> :
        IStreamCatchup<TData>,
        IDistributor<IStreamQueryPartition<TPartition>>
    {
    }
}