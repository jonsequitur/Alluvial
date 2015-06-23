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

        /// <summary>
        /// Creates a stream based on a queryable data source.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="advanceCursor">A delegate that advances the cursor after a batch is pulled from the stream.</param>
        /// <param name="newCursor">A delegate that returns a new cursor.</param>
        public static IStream<TData, TCursor> Create<TData, TCursor>(
            Func<IStreamQuery<TCursor>, Task<IEnumerable<TData>>> query,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null)
        {
            return Create(string.Format("{0}({1} by {2})", typeof (TData).Name, typeof (TCursor).Name, query.GetHashCode()),
                          query,
                          advanceCursor,
                          newCursor);
        }

        /// <summary>
        /// Creates a stream based on a queryable data source.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="id">The stream identifier.</param>
        /// <param name="query">The query.</param>
        /// <param name="advanceCursor">A delegate that advances the cursor after a batch is pulled from the stream.</param>
        /// <param name="newCursor">A delegate that returns a new cursor.</param>
        /// <returns></returns>
        public static IStream<TData, TCursor> Create<TData, TCursor>(
            string id,
            Func<IStreamQuery<TCursor>, Task<IEnumerable<TData>>> query,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null)
        {
            return new AnonymousStream<TData, TCursor>(
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

        /// <summary>
        /// Creates a stream based on a queryable data source.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="advanceCursor">A delegate that advances the cursor after a batch is pulled from the stream.</param>
        /// <param name="newCursor">A delegate that returns a new cursor.</param>
        public static IStream<TData, TCursor> Create<TData, TCursor>(
            Func<IStreamQuery<TCursor>, IEnumerable<TData>> query,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor,
            Func<ICursor<TCursor>> newCursor = null)
        {
            return Create(
                string.Format("{0}({1})", typeof (TData), query.GetHashCode()),
                query,
                advanceCursor,
                newCursor);
        }

        /// <summary>
        /// Creates a stream based on a queryable data source where the stream data and cursor are of the same type.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="advanceCursor">A delegate that advances the cursor after a batch is pulled from the stream.</param>
        /// <param name="newCursor">A delegate that returns a new cursor.</param>
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

        internal static IStream<TData, TCursor> Create<TData, TCursor>(
            string id,
            Func<IStreamQuery<TCursor>, IEnumerable<TData>> query,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null,
            IEnumerable<TData> source = null)
        {
            return new AnonymousStream<TData, TCursor>(
                id,
                async q => StreamBatch.Create(query(q), q.Cursor),
                advanceCursor,
                newCursor,
                source);
        }

        /// <summary>
        /// Maps data from a stream into a new form.
        /// </summary>
        public static IStream<TTo, TCursor> Map<TFrom, TTo, TCursor>(
            this IStream<TFrom, TCursor> sourceStream,
            Func<IEnumerable<TFrom>, IEnumerable<TTo>> map,
            string id = null)
        {
            return Create<TTo, TCursor>(
                id: id ?? sourceStream.Id + "->Map",
                query: async q =>
                {
                    var query = sourceStream.CreateQuery(q.Cursor, q.BatchCount);

                    var sourceBatch = await sourceStream.Fetch(query);

                    var mappedItems = map(sourceBatch);

                    var mappedCursor = Cursor.New<TCursor>(sourceBatch.StartsAtCursorPosition);

                    var mappedBatch = StreamBatch.Create(mappedItems, mappedCursor);

                    return mappedBatch;
                },
                advanceCursor: (query, batch) =>
                {
                    // don't advance the cursor in the map operation, since sourceStream.Fetch will already have done it
                },
                newCursor: sourceStream.NewCursor);
        }

        /// <summary>
        /// Splits a stream into many streams that can be independently caught up.
        /// </summary>
        /// <typeparam name="TUpstream">The type of the upstream stream.</typeparam>
        /// <typeparam name="TDownstream">The type of the downstream stream.</typeparam>
        /// <typeparam name="TUpstreamCursor">The type of the upstream cursor.</typeparam>
        /// <param name="upstream">The upstream.</param>
        /// <param name="queryDownstream">The query downstream.</param>
        /// <returns></returns>
        public static IStream<TDownstream, TUpstreamCursor> IntoMany<TUpstream, TDownstream, TUpstreamCursor>(
            this IStream<TUpstream, TUpstreamCursor> upstream,
            Func<TUpstream, TUpstreamCursor, TUpstreamCursor, Task<TDownstream>> queryDownstream)
        {
            return Create(
                id: upstream.Id + "->IntoMany",
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

        public static async Task<TProjection> Aggregate<TProjection, TData, TCursor>(
            this IStream<TData, TCursor> stream,
            IStreamAggregator<TProjection, TData> aggregator,
            TProjection projection = null)
            where TProjection : class
        {
            var cursor = (projection as ICursor<TCursor>) ??
                         stream.NewCursor();

            var query = stream.CreateQuery(cursor);

            var data = await query.NextBatch();

            if (data.Any())
            {
                projection = await aggregator.Aggregate(projection, data);
            }

            return projection;
        }

        public static IStreamPartitionGroup<TData, TCursor, TPartition> Partition<TData, TCursor, TPartition>(
            Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>, Task<IEnumerable<TData>>> query, 
            string id = null, 
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null, 
            Func<ICursor<TCursor>> newCursor = null)
        {
            return new AnonymousStreamPartitionGroup<TData, TCursor, TPartition>(
                id: id ?? string.Format("{0}({1})", typeof (TData), query.GetHashCode()), // TODO: (Partition) a better default id
                fetch: async (q, partition) =>
                {
                    q.BatchCount = q.BatchCount ?? StreamBatch.MaxBatchCount;
                    var batch = await query(q, partition);
                    return StreamBatch.Create(batch, q.Cursor);
                },
                advanceCursor: advanceCursor,
                newCursor: newCursor);
        }

        /// <summary>
        /// Traces queries sent and and data received on a stream.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="onSendQuery">Specifies how to trace information about queries sent to the stream.</param>
        /// <param name="onResults">Specifies how to trace data received from the stream.</param>
        public static IStream<TData, TCursor> Trace<TData, TCursor>(
            this IStream<TData, TCursor> stream,
            Action<IStreamQuery<TCursor>> onSendQuery = null,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> onResults = null)
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

            return Create<TData, TCursor>(
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