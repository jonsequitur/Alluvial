using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    public static class DataStream
    {
        public static IDataStream<TData> AsDataStream<TData>(
            this IEnumerable<TData> source)
            where TData : IComparable<TData>
        {
            return Create<TData>(Guid.NewGuid().ToString(),
                                 query => source.SkipWhile(x => query.Cursor.HasReached(x))
                                                .Take(query.BatchCount ?? int.MaxValue));
        }

        public static IDataStream<TData> Create<TData>(
            Func<IStreamQuery<TData>, Task<IEnumerable<TData>>> query,
            Action<IStreamQuery<TData>, IStreamQueryBatch<TData>> advanceCursor = null)
        {
            return Create(Guid.NewGuid().ToString(), query, advanceCursor);
        }

        public static IDataStream<TData> Create<TData>(
            string id,
            Func<IStreamQuery<TData>, Task<IEnumerable<TData>>> query,
            Action<IStreamQuery<TData>, IStreamQueryBatch<TData>> advanceCursor = null)
        {
            return new AnonymousDataStream<TData>(
                id,
                async q => StreamQueryBatch.Create(await query(q), q),
                advanceCursor);
        }

        public static IDataStream<TData> Create<TData>(
            Func<IStreamQuery<TData>, IEnumerable<TData>> query,
            Action<IStreamQuery<TData>, IStreamQueryBatch<TData>> advanceCursor = null)
        {
            return Create(Guid.NewGuid().ToString(), query, advanceCursor);
        }

        public static IDataStream<TData> Create<TData>(
            string id,
            Func<IStreamQuery<TData>, IEnumerable<TData>> query,
            Action<IStreamQuery<TData>, IStreamQueryBatch<TData>> advanceCursor = null)
        {
            return new AnonymousDataStream<TData>(
                id,
                async q => StreamQueryBatch.Create(query(q), q),
                advanceCursor);
        }

        public static ICursor CreateCursor<TData>(this IDataStream<TData> stream)
        {
            return Cursor.New();
        }

        public static IDataStream<TTo> Map<TFrom, TTo>(
            this IDataStream<TFrom> sourceStream,
            Func<IEnumerable<TFrom>, IEnumerable<TTo>> map, 
            string id= null)
        {
            return Create<TTo>(
                id: id ?? (sourceStream.Id + " map ->" + typeof(TTo).Name),
                query: async q =>
                {
                    var sourceBatch = await sourceStream.Fetch(
                        sourceStream.CreateQuery(q.Cursor, q.BatchCount));

                    var mappedBatch = map(sourceBatch);

                    return StreamQueryBatch.Create(mappedBatch, q);
                },
                advanceCursor: async (query, tos) =>
                {
                    // don't advance the cursor in the map operation, since sourceStream.Fetch will already have done it
                    await Task.Yield();
                });
        }

        public static IDataStream<IDataStream<TTo>> Requery<TFrom, TTo>(
            this IDataStream<TFrom> sourceStream,
            Func<TFrom, IDataStream<TTo>> query)
        {
            return Create<IDataStream<TTo>>(
                query: async q =>
                {
                    var sourceBatch = await sourceStream.Fetch(
                        sourceStream.CreateQuery(Cursor.New()));

                    var batches = sourceBatch.Select(query);

                    return batches;
                });
        }

        public static async Task<TProjection> ProjectWith<TProjection, TData>(
            this IDataStream<TData> dataStream,
            IDataStreamAggregator<TProjection, TData> projector,
            TProjection projection = null)
            where TProjection : class
        {
            // QUESTION: (ProjectWith) better name? this can also be used for side effects, where TProjection is used to track the state of the work

            var cursor = (projection as ICursor) ??
                         Cursor.New();

            var query = dataStream.CreateQuery(cursor);

            var data = await query.NextBatch();

            if (data.Any())
            {
                projection = projector.Aggregate(projection, data);
            }

            return projection;
        }
    }
}