using System;
using System.Threading.Tasks;
using trace = System.Diagnostics.Trace;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with projection stores.
    /// </summary>
    public static class ProjectionStore
    {
        public static IProjectionStore<TKey, TProjection> Create<TKey, TProjection>(
            Func<TKey, Task<TProjection>> get,
            Func<TKey, TProjection, Task> put)
        {
            return new AnonymousProjectionStore<TKey, TProjection>(get, put);
        }

        public static IProjectionStore<TKey, TProjection> Trace<TKey, TProjection>(
            this IProjectionStore<TKey, TProjection> store,
            Action<TKey, TProjection> get = null,
            Action<TKey, TProjection> put = null)
        {
           var traceGet = get ?? TraceGet;
           var tracePut = put ?? TracePut;

            return Create<TKey, TProjection>(
                get: async key =>
                {
                    var projection = await store.Get(key);

                    traceGet(key, projection);

                    return projection;
                },
                put: async (key, projection) =>
                {
                    tracePut(key, projection);

                    await store.Put(key, projection);
                });
        }

        private static void TracePut<TKey, TProjection>(TKey key, TProjection projection)
        {
            trace.WriteLine(string.Format("[Put] {0} for stream {1}",
                                          projection,
                                          key));
        }

        private static void TraceGet<TKey, TProjection>(TKey key, TProjection projection)
        {
            if (projection == null)
            {
                trace.WriteLine("[Get] no projection for stream " + key);
            }
            else
            {
                trace.WriteLine(string.Format("[Get] {0} for stream {1}",
                                              projection,
                                              key));
            }
        }

        public static FetchAndSaveProjection<TProjection> AsHandler<TProjection>(this IProjectionStore<string, TProjection> store)
        {
            store = store ?? new SingleInstanceProjectionCache<string, TProjection>();

            return async (key, update) =>
            {
                var projection = await store.Get(key);

                projection = await update(projection);

                if (Projection<TProjection>.WasUpdated(projection))
                {
                    await store.Put(key, projection);
                }
            };
        }

        private static class Projection<T>
        {
            static Projection()
            {
                if (typeof (ITrackCursorPosition).IsAssignableFrom(typeof (T)))
                {
                    WasUpdated = t => ((ITrackCursorPosition) t).CursorWasAdvanced;
                }
                else if (typeof (T).IsClass)
                {
                    WasUpdated = t => t != null;
                }
                else
                {
                    WasUpdated = t => !t.Equals(default(T));
                }
            }

            public static readonly Func<T, bool> WasUpdated;
        }
    }
}