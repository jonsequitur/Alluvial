using System.Collections.Generic;

namespace Alluvial
{
    public interface IStreamBatch<out TData> : IEnumerable<TData>
    {
        int Count { get; }

        dynamic StartsAtCursorPosition { get; }
    }
}