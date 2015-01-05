using System.Threading.Tasks;

namespace Alluvial
{
    public interface IStreamQuery<TData>
    {
        Task<IStreamQueryBatch<TData>> NextBatch();
        ICursor Cursor { get; }
        int? BatchCount { get; set; }
    }
}