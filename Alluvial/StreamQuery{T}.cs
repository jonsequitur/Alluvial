using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("Take {BatchCountDescription} after {Cursor.Position}")]
    internal class StreamQuery<TData> : IStreamIterator<TData>
    {
        private readonly IStream<TData> stream;
        private readonly ICursor cursor;

        public StreamQuery(IStream<TData> stream, ICursor cursor)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }
            if (cursor == null)
            {
                throw new ArgumentNullException("cursor");
            }
            this.stream = stream;
            this.cursor = cursor;
        }

        public ICursor Cursor
        {
            get
            {
                return cursor;
            }
        }

        public int? BatchCount { get; set; }

        public async Task<IStreamBatch<TData>> NextBatch()
        {
            return await stream.Fetch(this);
        }

        private string BatchCountDescription
        {
            get
            {
                if (BatchCount == null)
                {
                    return "all";
                }

                return BatchCount.Value.ToString();
            }
        }
    }
}