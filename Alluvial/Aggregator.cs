using System;
using System.Linq;

namespace Alluvial
{
    public delegate TProjection Aggregate<TProjection, in TData>(TProjection initial, IStreamQueryBatch<TData> events);

    public static class Aggregator
    {
        public static IDataStreamAggregator<TProjection, TData> After<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> first,
            Aggregate<TProjection, TData> then)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                projection = first.Aggregate(projection, batch);
                projection = then(projection, batch);
                return projection;
            });
        }

        public static IDataStreamAggregator<TProjection, TData> After<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> first,
            Action<TProjection, IStreamQueryBatch<TData>> then)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                projection = first.Aggregate(projection, batch);
                then(projection, batch);
                return projection;
            });
        }

        public static IDataStreamAggregator<TProjection, TData> Before<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> then,
            Aggregate<TProjection, TData> first)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                projection = first(projection, batch);
                return then.Aggregate(projection, batch);
            });
        }

        public static IDataStreamAggregator<TProjection, TData> Before<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> then,
            Action<TProjection, IStreamQueryBatch<TData>> first)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                first(projection, batch);
                return then.Aggregate(projection, batch);
            });
        }

        public static IDataStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> then,
            Action<TProjection, IStreamQueryBatch<TData>, Action<TProjection, IStreamQueryBatch<TData>>> first)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                first(projection, batch, (p, xs) => then.Aggregate(p, xs));
            });
        }

        public static IDataStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> then,
            Func<TProjection, IStreamQueryBatch<TData>, Aggregate<TProjection, TData>, TProjection> first)
        {
            return Create<TProjection, TData>((projection, batch) => first(projection, batch,  then.Aggregate));
        }

        public static IDataStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Aggregate<TProjection, TData> aggregate)
        {
            return new AnonymousDataStreamAggregator<TProjection, TData>(
                aggregate);
        }

        public static IDataStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Action<TProjection, IStreamQueryBatch<TData>> aggregate)
        {
            return new AnonymousDataStreamAggregator<TProjection, TData>(
                (projection, batch) =>
                {
                    aggregate(projection, batch);
                    return projection;
                });
        }
    }
}