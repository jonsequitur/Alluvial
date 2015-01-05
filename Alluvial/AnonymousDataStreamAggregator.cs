using System;
using System.Collections.Generic;

namespace Alluvial
{
    internal class AnonymousDataStreamAggregator<TProjection, TEvent> : IDataStreamAggregator<TProjection, TEvent>
    {
        private readonly Func<TEvent, TProjection> initial;
        private readonly Aggregate<TProjection, TEvent> aggregate;

        public AnonymousDataStreamAggregator(
            Func<TEvent, TProjection> initial,
            Aggregate<TProjection, TEvent> aggregate)
        {
            if (initial == null)
            {
                throw new ArgumentNullException("initial");
            }
            if (aggregate == null)
            {
                throw new ArgumentNullException("aggregate");
            }
            this.initial = initial;
            this.aggregate = aggregate;
        }

        public TProjection Aggregate(TProjection projection, IEnumerable<TEvent> events)
        {
            return aggregate(projection, events);
        }
    }
}