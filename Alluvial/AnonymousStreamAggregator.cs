using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousStreamAggregator<TProjection, TEvent> : IStreamAggregator<TProjection, TEvent>
    {
        private readonly AggregateAsync<TProjection, TEvent> aggregate;

        public AnonymousStreamAggregator(
            AggregateAsync<TProjection, TEvent> aggregate)
        {
            if (aggregate == null)
            {
                throw new ArgumentNullException("aggregate");
            }
            this.aggregate = aggregate;
        }

        public async Task<TProjection> Aggregate(TProjection projection, IStreamBatch<TEvent> events)
        {
            return await aggregate(projection, events);
        }
    }
}