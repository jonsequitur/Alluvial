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
        public static IStream<TData, TData> AsStream<TData>(
            this IEnumerable<TData> source)
        {
            return Create(string.Format("{0}({1})", typeof (TData), source.GetHashCode()),
                          query => source.SkipWhile(x => query.Cursor.HasReached(x))
                                         .Take(query.BatchCount ?? StreamBatch.MaxBatchCount),
                          advanceCursor: (q, b) =>
                          {
                              var last = b.LastOrDefault();
                              if (last != null)
                              {
                                  q.Cursor.AdvanceTo(last);
                              }
                          },
                          newCursor: () => Cursor.New<TData>(),
                          source: source);
        }

        /// <summary>
        /// Creates a stream based on an enumerable sequence.
        /// </summary>
        public static IStream<TData, TCursor> AsStream<TData, TCursor>(
            this IEnumerable<TData> source,
            Func<TData, TCursor> cursorPosition,
            string id = null)
        {
            if (cursorPosition == null)
            {
                throw new ArgumentNullException("cursorPosition");
            }

            return Create(
                id: id ?? string.Format("{0}({1})", typeof (TData), source.GetHashCode()),
                query: query => source.SkipWhile(x =>
                                                     query.Cursor.HasReached(cursorPosition(x)))
                                      .Take(query.BatchCount ?? StreamBatch.MaxBatchCount),
                advanceCursor: (q, b) =>
                {
                    var last = b.LastOrDefault();
                    if (last != null)
                    {
                        q.Cursor.AdvanceTo(cursorPosition(last));
                    }
                },
                newCursor: () => Cursor.New<TCursor>(),
                source: source);
        }

        /// <summary>
        /// Creates a stream based on an enumerable sequence, whose cursor is an integer.
        /// </summary>
        public static IStream<TData, int> AsSequentialStream<TData>(
            this IEnumerable<TData> source)
        {
            return Create(string.Format("{0}({1})", typeof (TData), source.GetHashCode()),
                          query => source.Skip(query.Cursor.Position)
                                         .Take(query.BatchCount ?? StreamBatch.MaxBatchCount),
                          newCursor: () => Cursor.New<int>());
        }

        public static IStream<TData, TCursorPosition> Create<TData, TCursorPosition>(
            Func<IStreamQuery<TCursorPosition>, Task<IEnumerable<TData>>> query,
            Action<IStreamQuery<TCursorPosition>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursorPosition>> newCursor = null)
        {
            return Create(string.Format("{0}({1} by {2})", typeof (TData).Name, typeof(TCursorPosition).Name, query.GetHashCode()), 
                query, 
                advanceCursor, 
                newCursor);
        }

        public static IStream<TData, TCursorPosition> Create<TData, TCursorPosition>(
            string id,
            Func<IStreamQuery<TCursorPosition>, Task<IEnumerable<TData>>> query,
            Action<IStreamQuery<TCursorPosition>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursorPosition>> newCursor = null)
        {
            return new AnonymousStream<TData, TCursorPosition>(
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

        public static IStream<TData, TCursorPosition> Create<TData, TCursorPosition>(
            Func<IStreamQuery<TCursorPosition>, IEnumerable<TData>> query,
            Action<IStreamQuery<TCursorPosition>, IStreamBatch<TData>> advanceCursor,
            Func<ICursor<TCursorPosition>> newCursor = null)
        {
            return Create(
                string.Format("{0}({1})", typeof (TData), query.GetHashCode()),
                query,
                advanceCursor,
                newCursor);
        }

        public static IStream<TData, TData> Create<TData>(
            Func<IStreamQuery<TData>, IEnumerable<TData>> query,
            Action<IStreamQuery<TData>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TData>> newCursor = null)
        {
            return Create(
                string.Format("{0}({1})", typeof (TData), query.GetHashCode()),
                query,
                advanceCursor ?? ((q, batch) =>
                {
                    var last = batch.LastOrDefault();
                    if (last != null)
                    {
                        q.Cursor.AdvanceTo(last);
                    }
                }),
                newCursor);
        }

        internal static IStream<TData, TCursorPosition> Create<TData, TCursorPosition>(
            string id,
            Func<IStreamQuery<TCursorPosition>, IEnumerable<TData>> query,
            Action<IStreamQuery<TCursorPosition>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursorPosition>> newCursor = null,
            IEnumerable<TData> source = null)
        {
            return new AnonymousStream<TData, TCursorPosition>(
                id,
                async q => StreamBatch.Create(query(q), q.Cursor),
                advanceCursor,
                newCursor,
                source);
        }

        /// <summary>
        /// Maps data from a stream into a new form.
        /// </summary>
        public static IStream<TTo, TCursorPosition> Map<TFrom, TTo, TCursorPosition>(
            this IStream<TFrom, TCursorPosition> sourceStream,
            Func<IEnumerable<TFrom>, IEnumerable<TTo>> map,
            string id = null)
        {
            return Create<TTo, TCursorPosition>(
                id: id ?? sourceStream.Id + "->Map",
                query: async q =>
                {
                    var query = sourceStream.CreateQuery(q.Cursor, q.BatchCount);

                    var sourceBatch = await sourceStream.Fetch(query);

                    var mappedItems = map(sourceBatch);

                    var mappedCursor = Cursor.New<TCursorPosition>(sourceBatch.StartsAtCursorPosition);

                    var mappedBatch = StreamBatch.Create(mappedItems, mappedCursor);

                    return mappedBatch;
                },
                advanceCursor: (query, batch) =>
                {
                    // don't advance the cursor in the map operation, since sourceStream.Fetch will already have done it
                },
                newCursor: sourceStream.NewCursor);
        }

        public static IStream<TDownstream, TUpstreamCursor> Then<TUpstream, TDownstream, TUpstreamCursor>(
            this IStream<TUpstream, TUpstreamCursor> upstream,
            Func<TUpstream, TUpstreamCursor, TUpstreamCursor, Task<TDownstream>> queryDownstream)
        {
            // FIX: (Then) rename
            return Create(
                id: upstream.Id + "->Then",
                query: async upstreamQuery =>
                {
                    var upstreamBatch = await upstream.Fetch(
                        upstream.CreateQuery(upstreamQuery.Cursor,
                                             upstreamQuery.BatchCount));

                    var streams = upstreamBatch.Select(
                        async x =>
                        {
                            TUpstreamCursor startingCursor = upstreamBatch.StartsAtCursorPosition;

                            return await queryDownstream(x,
                                                         startingCursor,
                                                         upstreamQuery.Cursor.Position);
                        });

                    return await streams.AwaitAll();
                },
                advanceCursor: (query, batch) =>
                {
                    // we're passing the cursor through to the upstream query, so we don't want downstream queries to overwrite it
                },
                newCursor: upstream.NewCursor);
        }

        public static async Task<TProjection> Aggregate<TProjection, TData, TCursorPosition>(
            this IStream<TData, TCursorPosition> stream,
            IStreamAggregator<TProjection, TData> aggregator,
            TProjection projection = null)
            where TProjection : class
        {
            var cursor = (projection as ICursor<TCursorPosition>) ??
                         stream.NewCursor();

            var query = stream.CreateQuery(cursor);

            var data = await query.NextBatch();

            if (data.Any())
            {
                projection = await aggregator.Aggregate(projection, data);
            }

            return projection;
        }

        public static IStream<TData, TCursorPosition> Trace<TData, TCursorPosition>(
            this IStream<TData, TCursorPosition> stream,
            Action<IStreamQuery<TCursorPosition>> onSendQuery = null,
            Action<IStreamQuery<TCursorPosition>, IStreamBatch<TData>> onResults = null)
        {
            onSendQuery = onSendQuery ??
                          (q => trace.WriteLine(
                              string.Format("[Query] stream {0} @ cursor {1}",
                                            stream.Id,
                                            q.Cursor.Position)));

            onResults = onResults ??
                        ((q, streamBatch) =>
                        {
                            trace.WriteLine(
                                string.Format("      [Fetched] stream {0} batch of {1}, now @ cursor {2}",
                                              stream.Id,
                                              streamBatch.Count,
                                              q.Cursor.Position));
                        });

            return Create<TData, TCursorPosition>(
                id: stream.Id,
                query: async q =>
                {
                    onSendQuery(q);

                    var streamBatch = await stream.Fetch(q);

                    onResults(q, streamBatch);

                    return streamBatch;
                },
                advanceCursor: (q, b) => { },
                newCursor: stream.NewCursor);
        }
    }
}