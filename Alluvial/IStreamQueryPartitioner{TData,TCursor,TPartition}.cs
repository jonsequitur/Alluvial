using System.Threading.Tasks;

namespace Alluvial
{
    public interface IStreamQueryPartitioner<TData, TCursor, in TPartition>
    {
        Task<IStream<TData, TCursor>> GetStream(IStreamQueryPartition<TPartition> partition);
    }
}