using System.Threading.Tasks;

namespace Alluvial
{
    public interface IDistributedStreamCatchup<out TData, TPartition> : IStreamCatchup<TData>
    {
        Task ReceiveLease(Lease<IStreamQueryPartition<TPartition>> lease);
    }
}