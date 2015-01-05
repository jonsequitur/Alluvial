using System.Threading.Tasks;

namespace Alluvial
{
    public interface IDataStream<TData>
    {
        string Id { get; }
        Task<IStreamQueryBatch<TData>> Fetch(IStreamQuery<TData> query);
    }
}