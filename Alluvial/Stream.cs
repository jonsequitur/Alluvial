using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using trace = System.Diagnostics.Trace;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with streams.
    /// </summary>
    public static class Stream
    {
        /// <summary>
        /// Creates a stream based on an enumerable sequence.
        /// </summary>
        public static IStream<TData> AsStream<TData>(
            this IEnumerable<TData> source)
        {
            return Create(string.Format("{0}({1})", typeof (TData), source.GetHashCode()),
                          query => source.SkipWhile(x => query.Cursor.HasReached(x))
                                         .Take(query.BatchCount ?? StreamBatch.MaxBatchCount),
                          newCursor: () => Cursor.Create(0));
        }

        public static IStream<TData> Create<TData>(
            Func<IStreamQuery, Task<IEnumerable<TData>>> query,
            Action<IStreamQuery, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor> newCursor = null)
        {
            return Create(string.Format("{0}({1})", typeof (TData), query.GetHashCode()),
                          query,
                          advanceCursor,
                          newCursor);
        }

        public static IStream<TData> Create<TData>(
            string id,
            Func<IStreamQuery, Task<IEnumerable<TData>>> query,
            Action<IStreamQuery, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor> newCursor = null)
        {
            return new AnonymousStream<TData>(
                id,
                async q =>
                {
                    var cursor = q.Cursor.Clone();
                    var data = await query(q);
                    return data as IStreamBatch<TData> ?? StreamBatch.Create(data, cursor);
                },
                advanceCursor,
                newCursor);
        }

        public static IStream<TData> Create<TData>(
            Func<IStreamQuery, IEnumerable<TData>> query,
            Action<IStreamQuery, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor> newCursor = null)
        {
            return Create(string.Format("{0}({1})", typeof (TData), query.GetHashCode()), query, advanceCursor, newCursor);
        }

        public static IStream<TData> Create<TData>(
            string id,
            Func<IStreamQuery, IEnumerable<TData>> query,
            Action<IStreamQuery, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor> newCursor = null)
        {
            return new AnonymousStream<TData>(
                id,
                async q => StreamBatch.Create(query(q), q.Cursor),
                advanceCursor,
                newCursor);
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

                    IEnumerable<TTo> mappedItems = map(sourceBatch);

                    ICursor mappedCursor = Cursor.Create(sourceBatch.StartsAtCursorPosition);

                    IStreamBatch<TTo> mappedBatch = StreamBatch.Create(mappedItems,
                                                                       mappedCursor);

                    return mappedBatch;
                },
                advanceCursor: async (query, batch) =>
                {
                    // don't advance the cursor in the map operation, since sourceStream.Fetch will already have done it
                },
                newCursor: sourceStream.NewCursor);
        }

        public static IStream<IStream<TDownstream>> Requery<TUpstream, TDownstream>(
            this IStream<TUpstream> upstream,
            Func<TUpstream, IStream<TDownstream>> queryDownstream)
        {
            return Create(
                id: upstream.Id + ".Requery",
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
                },
                newCursor: upstream.NewCursor);
        }

        public static async Task<TProjection> Aggregate<TProjection, TData>(
            this IStream<TData> stream,
            IStreamAggregator<TProjection, TData> aggregator,
            TProjection projection = null)
            where TProjection : class
        {
            // QUESTION: (ProjectWith) better name? this can also be used for side effects, where TProjection is used to track the state of the work

            var cursor = (projection as ICursor) ??
                         stream.NewCursor();

            var query = stream.CreateQuery(cursor);

            var data = await query.NextBatch();

            if (data.Any())
            {
                projection = await aggregator.Aggregate(projection, data);
            }

            return projection;
        }

        public static IStream<TData> Trace<TData>(
            this IStream<TData> stream,
            Action<IStreamQuery> onSendQuery = null,
            Action<IStreamQuery, IStreamBatch<TData>> onResults = null)
        {
            onSendQuery = onSendQuery ??
                          (q => trace.WriteLine(
                              string.Format("Query: stream {0} @ cursor position {1}",
                                            stream.Id,
                                            (object) q.Cursor.Position)));

            onResults = onResults ??
                        ((q, streamBatch) => trace.WriteLine(
                            string.Format("Fetched: stream {0} batch of {1}, now @ cursor position {2}",
                                          stream.Id,
                                          streamBatch.Count,
                                          (object) q.Cursor.Position)));

            return Create<TData>(
                id: stream.Id,
                query: async q =>
                {
                    onSendQuery(q);

                    var streamBatch = await stream.Fetch(q);

                    onResults(q, streamBatch);

                    return streamBatch;
                },
                advanceCursor: (q, b) =>
                {
                },
                newCursor: stream.NewCursor);
        }
    }
}