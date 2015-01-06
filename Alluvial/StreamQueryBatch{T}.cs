using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Alluvial
{
    [DebuggerStepThrough]
    internal class StreamQueryBatch<TData> : IStreamQueryBatch<TData>
    {
        private readonly TData[] results;

        public StreamQueryBatch(TData[] results, dynamic startsAtCursorPosition)
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
    }
}