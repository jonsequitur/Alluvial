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
                throw new ArgumentNullException(nameof(stream));
            }
            if (cursor == null)
            {
                throw new ArgumentNullException(nameof(cursor));
            }
            this.stream = stream;
            this.cursor = cursor;
        }

        public ICursor<TCursor> Cursor => cursor;

        public int? BatchSize { get; set; }

        public async Task<IStreamBatch<TData>> NextBatch() => await stream.Fetch(this);

        private dynamic BatchSizeDescription => BatchSize ?? (dynamic) "all";

        public override string ToString()
        {
            return $"query:take {BatchSizeDescription} after {cursor.Position} from {stream}";
        }
    }
}