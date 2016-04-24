using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousProjectionStore<TKey, TProjection> : IProjectionStore<TKey, TProjection>
    {
        private readonly Func<TKey, TProjection, Task> put;
        private readonly Func<TKey, Task<TProjection>> @get;

        public AnonymousProjectionStore(Func<TKey, Task<TProjection>> get, Func<TKey, TProjection, Task> put)
        {
            if (put == null)
            {
                throw new ArgumentNullException(nameof(put));
            }
            if (get == null)
            {
                throw new ArgumentNullException(nameof(get));
            }
            this.put = put;
            this.get = get;
        }

        public async Task Put(TKey key, TProjection projection) => await put(key, projection);

        public async Task<TProjection> Get(TKey key) => await @get(key);
    }
}