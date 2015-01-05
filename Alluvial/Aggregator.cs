using System;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial
{
    public delegate TProjection Aggregate<TProjection, in TData>(TProjection initial, IEnumerable<TData> events);

    public static class Aggregator
    {
        public static IDataStreamAggregator<TProjection, TData> After<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> first,
            Action<TProjection, IEnumerable<TData>> then)
        {
            return Create<TProjection, TData>((projection, data) =>
            {
                projection = first.Aggregate(projection, data);
                then(projection, data);
                return projection;
            });
        }

        public static IDataStreamAggregator<TProjection, TData> Before<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> then,
            Aggregate<TProjection, TData> first)
        {
            return Create<TProjection, TData>((projection, data) =>
            {
                projection = first(projection, data);
                return then.Aggregate(projection, data);
            });
        }

        public static IDataStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Aggregate<TProjection, TData> aggregate)
        {
            return new AnonymousDataStreamAggregator<TProjection, TData>(
                e => default(TProjection),
                aggregate);
        }

        public static IDataStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Action<TProjection, IEnumerable<TData>> aggregate)
        {
            return new AnonymousDataStreamAggregator<TProjection, TData>(
                e => default(TProjection),
                (t, us) =>
                {
                    aggregate(t, us);
                    return t;
                });
        }
    }
}