using System;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    public interface IPartitionedStream<TData, TCursor, out TPartition>
    {
        Task<IStream<TData, TCursor>> GetStream(IStreamQueryPartition<TPartition> partition);
    }
}