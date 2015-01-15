using System;

namespace Alluvial
{
    internal class AnonymousStreamAggregator<TProjection, TEvent> : IStreamAggregator<TProjection, TEvent>
    {
        private readonly Aggregate<TProjection, TEvent> aggregate;

        public AnonymousStreamAggregator(
            Aggregate<TProjection, TEvent> aggregate)
        {
            if (aggregate == null)
            {
                throw new ArgumentNullException("aggregate");
            }
            this.aggregate = aggregate;
        }

        public TProjection Aggregate(TProjection projection, IStreamBatch<TEvent> events)
        {
            return aggregate(projection, events);
        }
    }
}