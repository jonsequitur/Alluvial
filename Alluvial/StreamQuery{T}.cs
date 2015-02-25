using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("Take {BatchCountDescription} after {Cursor.Position}")]
    internal class StreamQuery<TData, TCursorPosition> : IStreamIterator<TData, TCursorPosition>
    {
        private readonly IStream<TData, TCursorPosition> stream;
        private readonly ICursor<TCursorPosition> cursor;

        public StreamQuery(IStream<TData, TCursorPosition> stream, ICursor<TCursorPosition> cursor)
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

        public ICursor<TCursorPosition> Cursor
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