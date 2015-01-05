using System.Collections.Generic;

namespace Alluvial
{
    public interface IStreamQueryBatch<out TData> : IEnumerable<TData>
    {
        int Count { get; }
        dynamic StartsAtCursorPosition { get; }
    }
}