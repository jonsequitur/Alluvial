using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AggregatorSubscription<TProjection, TData> : IAggregatorSubscription
    {
        public AggregatorSubscription(
            IStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            if (aggregator == null)
            {
                throw new ArgumentNullException("aggregator");
            }
            ProjectionStore = projectionStore ??
                              new SingleInstanceProjectionCache<string, TProjection>();
            Aggregator = aggregator;
            IsCursor = typeof (ICursor).IsAssignableFrom(typeof (TProjection));
        }

        public IStreamAggregator<TProjection, TData> Aggregator { get; private set; }

        public IProjectionStore<string, TProjection> ProjectionStore { get; private set; }

        public bool IsCursor { get; protected set; }

        public Type ProjectionType
        {
            get
            {
                return typeof (TProjection);
            }
        }

        public Type StreamDataType
        {
            get
            {
                return typeof (TData);
            }
        }

        public async Task<object> GetProjection(string streamId)
        {
            return await ProjectionStore.Get(streamId);
        }
    }
}