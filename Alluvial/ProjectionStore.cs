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
        /// <summary>
        /// Creates a projection store.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <param name="get">The operation specifying how to persist a projection to storage.</param>
        /// <param name="put">The operation specifying how to retrieve a projection from storage.</param>
        /// <returns></returns>
        public static IProjectionStore<TKey, TProjection> Create<TKey, TProjection>(
            Func<TKey, Task<TProjection>> get,
            Func<TKey, TProjection, Task> put) =>
                new AnonymousProjectionStore<TKey, TProjection>(get, put);

        /// <summary>
        /// Traces calls to a projection store instance.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to the store.</typeparam>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <param name="store">The store.</param>
        /// <param name="get">An optional delegate to write projections after they are retrieved from the store and before new data is aggregated.</param>
        /// <param name="put">An optional delegate to write projections before they are saved to the store, after new data is aggregated..</param>
        /// <returns></returns>
        public static IProjectionStore<TKey, TProjection> Trace<TKey, TProjection>(
            this IProjectionStore<TKey, TProjection> store,
            Action<TKey, TProjection> get = null,
            Action<TKey, TProjection> put = null)
        {
            if (get == null && put == null)
            {
                get = TraceGet;
                put = TracePut;
            }
            else
            {
                get = get ?? ((k, p) => { });
                put = put ?? ((k, p) => { });
            }

            return Create<TKey, TProjection>(
                get: async key =>
                {
                    var projection = await store.Get(key);

                    get(key, projection);

                    return projection;
                },
                put: async (key, projection) =>
                {
                    put(key, projection);

                    await store.Put(key, projection);
                });
        }

        private static void TracePut<TKey, TProjection>(TKey key, TProjection projection) =>
            trace.WriteLine($"[Store.Put] {projection} for stream {key}");

        private static void TraceGet<TKey, TProjection>(TKey key, TProjection projection) =>
            trace.WriteLine(projection == null
                                ? $"[Store.Get] no projection for stream {key}"
                                : $"[Store.Get] {projection} for stream {key}");

        /// <summary>
        /// Returns a fetch and save function wrapping the specified projection store's <see cref="IProjectionStore{TKey,TProjection}.Get" /> and <see cref="IProjectionStore{TKey,TProjection}.Put" /> operations.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <param name="store">The store.</param>
        public static FetchAndSave<TProjection> AsHandler<TProjection>(this IProjectionStore<string, TProjection> store)
        {
            store = store ?? new InMemoryProjectionStore<TProjection>();

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