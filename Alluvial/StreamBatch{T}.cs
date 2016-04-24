using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("{ToString()}")]
    internal class StreamBatch<TData> : IStreamBatch<TData>
    {
        private readonly TData[] results;

        public StreamBatch(TData[] results, dynamic startsAtCursorPosition)
        {
            StartsAtCursorPosition = startsAtCursorPosition;
            this.results = results ?? new TData[0];
        }

        public IEnumerator<TData> GetEnumerator() => results.Cast<TData>().GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public int Count => results.Length;

        public dynamic StartsAtCursorPosition { get; }

        public override string ToString() =>
            $"batch: {Count} items starting at {StartsAtCursorPosition}";
    }
}