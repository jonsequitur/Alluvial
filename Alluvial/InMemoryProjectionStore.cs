using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// A projection store that keeps references to projections in memory but has no out-of-process persistence.
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    public class InMemoryProjectionStore<TProjection> :
        IProjectionStore<string, TProjection>,
        IEnumerable<TProjection>
    {
        private readonly ConcurrentDictionary<string, TProjection> store = new ConcurrentDictionary<string, TProjection>();
        private readonly Func<string, TProjection> createProjection;

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryProjectionStore{TProjection}"/> class.
        /// </summary>
        /// <param name="createProjection">The create projection.</param>
        public InMemoryProjectionStore(Func<string, TProjection> createProjection = null)
        {
            this.createProjection = createProjection ?? (_ => Activator.CreateInstance<TProjection>());
        }

        /// <summary>
        /// Puts the specified projection in the store, overwriting any previous projection having the same key.
        /// </summary>
        /// <param name="streamId">The key under which to store the projection.</param>
        /// <param name="projection">The projection.</param>
        public async Task Put(string streamId, TProjection projection)
        {
            await Task.Yield();

            if (streamId == null)
            {
                throw new ArgumentNullException(nameof(streamId));
            }
            if (projection == null)
            {
                throw new ArgumentNullException(nameof(projection));
            }
            store[streamId] = projection;
        }

        /// <summary>
        /// Gets a projection having the specified key, or null if it does not exist.
        /// </summary>
        public async Task<TProjection> Get(string streamId)
        {
            await Task.Yield();

            TProjection projection;
            if (store.TryGetValue(streamId, out projection))
            {
                return projection;
            }
            projection = createProjection(streamId);
            return projection;
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// An enumerator that can be used to iterate through the collection.
        /// </returns>
        public IEnumerator<TProjection> GetEnumerator() => store.Values.GetEnumerator();

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}