using System.Threading.Tasks;

namespace Alluvial
{
    public interface IStreamQuery
    {
        ICursor Cursor { get; }
        int? BatchCount { get; set; }
    }

    public interface IStreamIterator<TData> : IStreamQuery
    {
        Task<IStreamBatch<TData>> NextBatch();
    }
}