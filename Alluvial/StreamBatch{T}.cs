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

        public IEnumerator<TData> GetEnumerator()
        {
            return results.Cast<TData>().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count
        {
            get
            {
                return results.Length;
            }
        }

        public dynamic StartsAtCursorPosition { get; private set; }

        public override string ToString()
        {
            return string.Format("batch: {0} items starting at {1}",
                                 Count,
                                 StartsAtCursorPosition);
        }
    }
}