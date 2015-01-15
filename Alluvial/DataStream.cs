using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with data streams.
    /// </summary>
    public static class DataStream
    {
        /// <summary>
        /// Creates a data stream based on an enumerable sequence.
        /// </summary>
        public static IStream<TData> AsDataStream<TData>(
            this IEnumerable<TData> source)
            where TData : IComparable<TData>
        {
            return Create<TData>(Guid.NewGuid().ToString(),
                                 query => source.SkipWhile(x => query.Cursor.HasReached(x))
                                                .Take(query.BatchCount ?? int.MaxValue));
        }

        public static IStream<TData> Create<TData>(
            Func<IStreamQuery, Task<IEnumerable<TData>>> query,
            Action<IStreamQuery, IStreamBatch<TData>> advanceCursor = null)
        {
            return Create(Guid.NewGuid().ToString(), query, advanceCursor);
        }

        public static IStream<TData> Create<TData>(
            string id,
            Func<IStreamQuery, Task<IEnumerable<TData>>> query,
            Action<IStreamQuery, IStreamBatch<TData>> advanceCursor = null)
        {
            return new AnonymousStream<TData>(
                id,
                async q => StreamQueryBatch.Create(await query(q), q.Cursor),
                advanceCursor);
        }

        public static IStream<TData> Create<TData>(
            Func<IStreamQuery, IEnumerable<TData>> query,
            Action<IStreamQuery, IStreamBatch<TData>> advanceCursor = null)
        {
            return Create(Guid.NewGuid().ToString(), query, advanceCursor);
        }

        public static IStream<TData> Create<TData>(
            string id,
            Func<IStreamQuery, IEnumerable<TData>> query,
            Action<IStreamQuery, IStreamBatch<TData>> advanceCursor = null)
        {
            return new AnonymousStream<TData>(
                id,
                async q => StreamQueryBatch.Create(query(q), q.Cursor),
                advanceCursor);
        }

        /// <summary>
        /// Creates a new cursor over a data stream.
        /// </summary>
        public static ICursor CreateCursor<TData>(this IStream<TData> stream)
        {
            return Cursor.New();
        }

        /// <summary>
        /// Maps data from a stream into a new form.
        /// </summary>
        public static IStream<TTo> Map<TFrom, TTo>(
            this IStream<TFrom> sourceStream,
            Func<IEnumerable<TFrom>, IEnumerable<TTo>> map,
            string id = null)
        {
            return Create<TTo>(
                id: id ?? sourceStream.Id,
                query: async q =>
                {
                    var sourceBatch = await sourceStream.Fetch(
                        sourceStream.CreateQuery(q.Cursor, q.BatchCount));

                    var mappedBatch = map(sourceBatch);

                    return StreamQueryBatch.Create(mappedBatch, q.Cursor);
                },
                advanceCursor: async (query, batch) =>
                {
                    // don't advance the cursor in the map operation, since sourceStream.Fetch will already have done it
                });
        }

        public static IStream<IStream<TDownstream>> Requery<TUpstream, TDownstream>(
            this IStream<TUpstream> upstream,
            Func<TUpstream, IStream<TDownstream>> queryDownstream)
        {
            return Create<IStream<TDownstream>>(
                query: async upstreamQuery =>
                {
                    var cursor = upstreamQuery.Cursor;

                    var upstreamBatch = await upstream.Fetch(
                        upstream.CreateQuery(cursor, upstreamQuery.BatchCount));

                    return upstreamBatch.Select(queryDownstream);
                },
                advanceCursor: (query, batch) =>
                {
                    // we're passing the cursor through to the upstream query, so we don't want downstream queries to overwrite it
                });
        }

        public static async Task<TProjection> ProjectWith<TProjection, TData>(
            this IStream<TData> stream,
            IStreamAggregator<TProjection, TData> projector,
            TProjection projection = null)
            where TProjection : class
        {
            // QUESTION: (ProjectWith) better name? this can also be used for side effects, where TProjection is used to track the state of the work

            var cursor = (projection as ICursor) ??
                         Cursor.New();

            var query = stream.CreateQuery(cursor);

            var data = await query.NextBatch();

            if (data.Any())
            {
                projection = projector.Aggregate(projection, data);
            }

            return projection;
        }
    }
}