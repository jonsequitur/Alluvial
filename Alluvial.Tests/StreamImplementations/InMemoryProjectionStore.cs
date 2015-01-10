using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Alluvial.Tests
{
    public class InMemoryProjectionStore<TProjection> : 
        IProjectionStore<string, TProjection>,
        IEnumerable<TProjection> 
        where TProjection : IMapProjection, new()
    {
        private readonly ConcurrentDictionary<string, TProjection> store = new ConcurrentDictionary<string, TProjection>();

        public async Task Put(TProjection projection)
        {
            store[projection.AggregateId] = projection;
        }

        public async Task<TProjection> Get(string streamId)
        {
            TProjection projection;
            if (store.TryGetValue(streamId, out projection))
            {
                return projection;
            }
            projection = new TProjection
            {
                AggregateId = streamId
            };
            return projection;
        }

        public IEnumerator<TProjection> GetEnumerator()
        {
            return store.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}