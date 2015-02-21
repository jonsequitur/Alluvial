using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousStreamAggregator<TProjection, TData> : IStreamAggregator<TProjection, TData>
    {
        private readonly AggregateAsync<TProjection, TData> aggregate;

        public AnonymousStreamAggregator(
            AggregateAsync<TProjection, TData> aggregate)
        {
            if (aggregate == null)
            {
                throw new ArgumentNullException("aggregate");
            }
            this.aggregate = aggregate;
        }

        public Task<TProjection> Aggregate(TProjection projection, IStreamBatch<TData> events)
        {
            return aggregate(projection, events);
        }
    }
}