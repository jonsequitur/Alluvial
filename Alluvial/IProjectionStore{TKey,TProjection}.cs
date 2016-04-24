using System;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Stores and retrieves projections.
    /// </summary>
    /// <typeparam name="TKey">The type of the key by which projections are looked up.</typeparam>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    public interface IProjectionStore<in TKey, TProjection>
    {
        /// <summary>
        /// Puts the specified projection in the store, overwriting any previous projection having the same key.
        /// </summary>
        /// <param name="key">The key under which to store the projection.</param>
        /// <param name="projection">The projection.</param>
        /// <returns></returns>
        Task Put(TKey key, TProjection projection);

        /// <summary>
        /// Gets a projection having the specified key, or null if it does not exist.
        /// </summary>
        Task<TProjection> Get(TKey key);
    }
}