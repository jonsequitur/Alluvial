using System;

namespace Alluvial
{
    internal class AnonymousDataStreamAggregator<TProjection, TEvent> : IDataStreamAggregator<TProjection, TEvent>
    {
        private readonly Aggregate<TProjection, TEvent> aggregate;

        public AnonymousDataStreamAggregator(
            Aggregate<TProjection, TEvent> aggregate)
        {
            if (aggregate == null)
            {
                throw new ArgumentNullException("aggregate");
            }
            this.aggregate = aggregate;
        }

        public TProjection Aggregate(TProjection projection, IStreamQueryBatch<TEvent> events)
        {
            return aggregate(projection, events);
        }
    }
}