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
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            return Create(query => source.SkipWhile(x => query.Cursor.HasReached(x))
                                         .Take(query.BatchSize ?? StreamBatch.MaxSize),
                          advanceCursor: (q, b) =>
                          {
                              var last = b.LastOrDefault();
                              if (last != null)
                              {
                                  q.Cursor.AdvanceTo(last);
                              }
                          },
                          newCursor: () => Cursor.New<TData>(), source: source);
        }

        /// <summary>
        /// Creates a stream based on an enumerable sequence.
        /// </summary>
        public static IStream<TData, TCursor> AsStream<TData, TCursor>(
            this IEnumerable<TData> source,
            Func<TData, TCursor> cursorPosition,
            string id = null)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (cursorPosition == null)
            {
                throw new ArgumentNullException(nameof(cursorPosition));
            }

            return Create(query: query => source.SkipWhile(x =>
                                                           query.Cursor.HasReached(cursorPosition(x)))
                                                .Take(query.BatchSize ?? StreamBatch.MaxSize),
                          id: id,
                          advanceCursor: (q, b) =>
                          {
                              var last = b.LastOrDefault();
                              if (last != null)
                              {
                                  q.Cursor.AdvanceTo(cursorPosition(last));
                              }
                          },
                          newCursor: () => Cursor.New<TCursor>(), source: source);
        }

        /// <summary>
        /// Creates a stream based on an enumerable sequence, whose cursor is an integer.
        /// </summary>
        public static IStream<TData, int> AsSequentialStream<TData>(
            this IEnumerable<TData> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            return Create(query => source.Skip(query.Cursor.Position)
                                         .Take(query.BatchSize ?? StreamBatch.MaxSize),
                          $"{typeof (TData)}({source.GetHashCode()})", newCursor: () => Cursor.New<int>());
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
            Func<ICursor<TCursor>> newCursor = null) =>
                Create(null,
                       query,
                       advanceCursor,
                       newCursor);

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
            Func<ICursor<TCursor>> newCursor = null) =>
                new AnonymousStream<TData, TCursor>(
                    id,
                    async q =>
                    {
                        var cursor = q.Cursor.Clone();
                        var data = await query(q);
                        return data as IStreamBatch<TData> ?? StreamBatch.Create(data, cursor);
                    },
                    advanceCursor,
                    newCursor);

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
            Func<ICursor<TCursor>> newCursor = null) =>
                Create(query,
                       null,
                       advanceCursor,
                       newCursor);

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
            if (query == null)
            {
                throw new ArgumentNullException(nameof(query));
            }

            return Create<TData, TData>(
                query,
                advanceCursor ?? ((q, batch) =>
                {
                    var last = batch.LastOrDefault();
                    if (last != null)
                    {
                        q.Cursor.AdvanceTo(last);
                    }
                }), newCursor);
        }

        private static IStream<TData, TCursor> Create<TData, TCursor>(
            Func<IStreamQuery<TCursor>, IEnumerable<TData>> query,
            string id = null,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null,
            IEnumerable<TData> source = null)
        {
            if (query == null)
            {
                throw new ArgumentNullException(nameof(query));
            }

            return new AnonymousStream<TData, TCursor>(
                id,
                q => StreamBatch.Create(query(q), q.Cursor).CompletedTask(),
                advanceCursor,
                newCursor,
                source);
        }

        /// <summary>
        /// Maps data from a stream into a new form.
        /// </summary>
        public static IStream<TTo, TCursor> Map<TFrom, TTo, TCursor>(
            this IStream<TFrom, TCursor> source,
            Func<IEnumerable<TFrom>, IEnumerable<TTo>> map,
            string id = null)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (map == null)
            {
                throw new ArgumentNullException(nameof(map));
            }

            return Create<TTo, TCursor>(
                id: id ?? $"{source.Id}->Map(d:{typeof (TTo).ReadableName()})",
                query: async q =>
                {
                    var query = source.CreateQuery(q.Cursor, q.BatchSize);

                    var sourceBatch = await source.Fetch(query);

                    var mappedItems = map(sourceBatch);

                    var mappedCursor = Cursor.New<TCursor>(sourceBatch.StartsAtCursorPosition);

                    var mappedBatch = StreamBatch.Create(mappedItems, mappedCursor);

                    return mappedBatch;
                },
                advanceCursor: (query, batch) =>
                {
                    // don't advance the cursor in the map operation, since sourceStream.Fetch will already have done it
                },
                newCursor: source.NewCursor);
        }

        /// <summary>
        /// Maps data from a stream into a new form.
        /// </summary>
        public static IPartitionedStream<TTo, TCursor, TPartition> Map<TFrom, TTo, TCursor, TPartition>(
            this IPartitionedStream<TFrom, TCursor, TPartition> source,
            Func<IEnumerable<TFrom>, IEnumerable<TTo>> map,
            string id = null)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return new AnonymousPartitionedStream<TTo, TCursor, TPartition>(
                id: id ?? $"{source.Id}->Map(d:{typeof (TTo).ReadableName()})",
                getStream: async partition =>
                {
                    var stream = await source.GetStream(partition);
                    return stream.Map(map);
                });
        }

        /// <summary>
        /// Splits a stream into many streams that can be independently caught up.
        /// </summary>
        /// <typeparam name="TUpstream">The type of the upstream stream.</typeparam>
        /// <typeparam name="TDownstream">The type of the downstream streams.</typeparam>
        /// <typeparam name="TUpstreamCursor">The type of the upstream cursor.</typeparam>
        /// <param name="upstream">The upstream stream.</param>
        /// <param name="queryDownstream">The downstream query.</param>
        /// <returns></returns>
        public static IStream<TDownstream, TUpstreamCursor> IntoMany<TUpstream, TDownstream, TUpstreamCursor>(
            this IStream<TUpstream, TUpstreamCursor> upstream,
            QueryDownstream<TUpstream, TDownstream, TUpstreamCursor> queryDownstream)
        {
            if (upstream == null)
            {
                throw new ArgumentNullException(nameof(upstream));
            }

            return Create(
                id: $"{upstream.Id}->IntoMany(d:{typeof (TDownstream).ReadableName()})",
                query: async upstreamQuery =>
                {
                    var upstreamBatch = await upstream.Fetch(
                        upstream.CreateQuery(upstreamQuery.Cursor,
                                             upstreamQuery.BatchSize));

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

        /// <summary>
        /// Splits a partitioned stream into many streams that can be independently caught up.
        /// </summary>
        /// <typeparam name="TUpstream">The type of the upstream, partitioned stream.</typeparam>
        /// <typeparam name="TDownstream">The type of the downstream streams.</typeparam>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <typeparam name="TUpstreamCursor">The type of the partitionedStream cursor.</typeparam>
        /// <param name="partitionedStream">The partitioned stream.</param>
        /// <param name="queryDownstream">The query downstream.</param>
        /// <returns></returns>
        public static IPartitionedStream<TDownstream, TUpstreamCursor, TPartition> IntoMany<TUpstream, TDownstream, TUpstreamCursor, TPartition>(
            this IPartitionedStream<TUpstream, TUpstreamCursor, TPartition> partitionedStream,
            QueryDownstream<TUpstream, TDownstream, TUpstreamCursor, TPartition> queryDownstream)
        {
            if (partitionedStream == null)
            {
                throw new ArgumentNullException(nameof(partitionedStream));
            }
            if (queryDownstream == null)
            {
                throw new ArgumentNullException(nameof(queryDownstream));
            }

            return Partitioned<TDownstream, TUpstreamCursor, TPartition>(
                id: $"{partitionedStream.Id}->IntoMany(d:{typeof (TDownstream).ReadableName()})",
                query: async (upstreamQuery, partition) =>
                {
                    var upstreamStream = await partitionedStream.GetStream(partition);

                    var upstreamBatch = await upstreamStream.Fetch(
                        upstreamStream.CreateQuery(upstreamQuery.Cursor,
                                                   upstreamQuery.BatchSize));

                    var streams = upstreamBatch.Select(
                        async x =>
                        {
                            TUpstreamCursor startingCursor = upstreamBatch.StartsAtCursorPosition;

                            return await queryDownstream(x,
                                                         startingCursor,
                                                         upstreamQuery.Cursor.Position,
                                                         partition);
                        });

                    return await streams.AwaitAll();
                },
                advanceCursor: (query, batch) =>
                {
                    // we're passing the cursor through to the upstream query, so we don't want downstream queries to overwrite it
                });
        }

        /// <summary>
        /// Aggregates a single batch of data from a stream using the specified aggregator and projection.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="aggregator">The aggregator.</param>
        /// <param name="projection">The projection.</param>
        /// <returns>The updated state of the projection.</returns>
        /// <remarks>This method can be used to create on-demand projections. It does not do any projection persistence.</remarks>
        public static async Task<TProjection> Aggregate<TProjection, TData, TCursor>(
            this IStream<TData, TCursor> stream,
            IStreamAggregator<TProjection, TData> aggregator,
            TProjection projection = null)
            where TProjection : class
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
            if (aggregator == null)
            {
                throw new ArgumentNullException(nameof(aggregator));
            }

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

        /// <summary>
        /// Creates a partitioned stream.
        /// </summary>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="id">The base identifier for the partitioned stream.</param>
        /// <param name="advanceCursor">A delegate that advances the cursor after a batch is pulled from the stream.</param>
        /// <param name="newCursor">A delegate that returns a new cursor.</param>
        public static IPartitionedStream<TData, TCursor, TPartition> Partitioned<TData, TCursor, TPartition>(
            Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>, Task<IEnumerable<TData>>> query,
            string id = null,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null)
        {
            if (query == null)
            {
                throw new ArgumentNullException(nameof(query));
            }

            return new AnonymousPartitionedStream<TData, TCursor, TPartition>(
                id: id,
                fetch: async (q, partition) =>
                {
                    q.BatchSize = q.BatchSize ?? StreamBatch.MaxSize;
                    var batch = await query(q, partition);
                    return StreamBatch.Create(batch, q.Cursor);
                },
                advanceCursor: advanceCursor,
                newCursor: newCursor);
        }

        /// <summary>
        /// Creates a partitioned stream, partitioned by query ranges.
        /// </summary>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="id">The base identifier for the partitioned stream.</param>
        /// <param name="advanceCursor">A delegate that advances the cursor after a batch is pulled from the stream.</param>
        /// <param name="newCursor">A delegate that returns a new cursor.</param>
        public static IPartitionedStream<TData, TCursor, TPartition> PartitionedByRange<TData, TCursor, TPartition>(
            Func<IStreamQuery<TCursor>, IStreamQueryRangePartition<TPartition>, Task<IEnumerable<TData>>> query,
            string id = null,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null)
        {
            if (query == null)
            {
                throw new ArgumentNullException(nameof(query));
            }
            return new AnonymousPartitionedStream<TData, TCursor, TPartition>(
                id: id,
                fetch: async (q, partition) =>
                {
                    q.BatchSize = q.BatchSize ?? StreamBatch.MaxSize;
                    var batch = await query(q, (IStreamQueryRangePartition<TPartition>) partition);
                    return StreamBatch.Create(batch, q.Cursor);
                },
                advanceCursor: advanceCursor,
                newCursor: newCursor);
        }

        /// <summary>
        /// Creates a partitioned stream.
        /// </summary>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="id">The base identifier for the partitioned stream.</param>
        /// <param name="advanceCursor">A delegate that advances the cursor after a batch is pulled from the stream.</param>
        /// <param name="newCursor">A delegate that returns a new cursor.</param>
        public static IPartitionedStream<TData, TCursor, TPartition> PartitionedByValue<TData, TCursor, TPartition>(
            Func<IStreamQuery<TCursor>, IStreamQueryValuePartition<TPartition>, Task<IEnumerable<TData>>> query,
            string id = null,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null)
        {
            if (query == null)
            {
                throw new ArgumentNullException(nameof(query));
            }

            return new AnonymousPartitionedStream<TData, TCursor, TPartition>(
                id: id,
                fetch: async (q, partition) =>
                {
                    q.BatchSize = q.BatchSize ?? StreamBatch.MaxSize;
                    var batch = await query(q, (IStreamQueryValuePartition<TPartition>) partition);
                    return StreamBatch.Create(batch, q.Cursor);
                },
                advanceCursor: advanceCursor,
                newCursor: newCursor);
        }

        /// <summary>
        /// Traces queries sent and and data received on a stream.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the stream's cursor.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="onSendQuery">Specifies how to trace information about queries sent to the stream.</param>
        /// <param name="onResults">Specifies how to trace data received from the stream.</param>
        public static IStream<TData, TCursor> Trace<TData, TCursor>(
            this IStream<TData, TCursor> stream,
            Action<IStreamQuery<TCursor>> onSendQuery = null,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> onResults = null)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            onSendQuery = onSendQuery ??
                          (q => trace.WriteLine(
                              $"[Query] stream {stream.Id} @ cursor {q.Cursor.Position}"));

            onResults = onResults ??
                        ((q, streamBatch) =>
                        {
                            trace.WriteLine(
                                $"      [Fetched] stream {stream.Id} batch of {streamBatch.Count}, now @ cursor {q.Cursor.Position}");
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

        /// <summary>
        /// Traces queries sent and and data received on a partitioned stream.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the stream's cursor.</typeparam>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="onSendQuery">Specifies how to trace information about queries sent to the stream.</param>
        /// <param name="onResults">Specifies how to trace data received from the stream.</param>
        public static IPartitionedStream<TData, TCursor, TPartition> Trace<TData, TCursor, TPartition>(
            this IPartitionedStream<TData, TCursor, TPartition> stream,
            Action<IStreamQuery<TCursor>> onSendQuery = null,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> onResults = null)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            return new AnonymousPartitionedStream<TData, TCursor, TPartition>(
                stream.Id,
                async p => (await stream.GetStream(p)).Trace(onSendQuery, onResults));
        }
    }
}