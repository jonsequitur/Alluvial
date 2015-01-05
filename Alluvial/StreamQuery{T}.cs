using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class StreamQuery<TData> : IStreamQuery<TData>
    {
        private readonly IDataStream<TData> stream;
        private readonly ICursor cursor;

        public StreamQuery(IDataStream<TData> stream, ICursor cursor)
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

        public async Task<IStreamQueryBatch<TData>> NextBatch()
        {
            var batch = await stream.Fetch(this);
            return batch;
        }
    }
}