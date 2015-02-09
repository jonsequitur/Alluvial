using System;
using System.Threading.Tasks;
using trace = System.Diagnostics.Trace;

namespace Alluvial.Tests
{
    public static class ProjectionStore
    {
        public static IProjectionStore<TKey, TProjection> Create<TKey, TProjection>(
            Func<TKey, Task<TProjection>> get,
            Func<TKey, TProjection, Task> put)
        {
            return new AnonymousProjectionStore<TKey, TProjection>(get, put);
        }

        public static IProjectionStore<TKey, TProjection> Trace<TKey, TProjection>(this IProjectionStore<TKey, TProjection> store)
        {
            return Create<TKey, TProjection>(
                get: async key =>
                {
                    var projection = await store.Get(key);

                    if (projection == null)
                    {
                        trace.WriteLine("Get: no projection for stream " + key);
                    }
                    else
                    {
                        trace.WriteLine(string.Format("Get: projection {0} for stream {1}",
                                                      projection,
                                                      key));
                    }

                    return projection;
                },
                put: async (key, projection) =>
                {
                    trace.WriteLine(string.Format("Put: projection {0} for stream {1}",
                        projection, 
                        key));

                    await store.Put(key, projection);
                });
        }
    }
}