using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("{ToString()}")]
    internal class StreamQuery<TData, TCursor> : IStreamIterator<TData, TCursor>
    {
        private readonly IStream<TData, TCursor> stream;
        private readonly ICursor<TCursor> cursor;

        public StreamQuery(IStream<TData, TCursor> stream, ICursor<TCursor> cursor)
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

        public ICursor<TCursor> Cursor
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

        private dynamic BatchCountDescription
        {
            get
            {
                if (BatchCount == null)
                {
                    return "all";
                }

                return BatchCount.Value;
            }
        }

        public override string ToString()
        {
            return string.Format("query: take {0} after {1} from {2}", BatchCountDescription, cursor.Position, stream);
        }
    }
}