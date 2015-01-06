using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class SingleInstanceProjectionCache<TKey, TProjection> : IProjectionStore<TKey, TProjection>
    {
        private TProjection instance;
        private readonly Func<TProjection> create;
        private bool instantiated;

        public SingleInstanceProjectionCache(Func<TProjection> create = null)
        {
            this.create = () =>
            {
                if (create != null)
                {
                    return create();
                }
                return Activator.CreateInstance<TProjection>();
            };
        }

        public async Task Put(TProjection projection)
        {
            instantiated = true;
            instance = projection;
        }

        public async Task<TProjection> Get(TKey key)
        {
            if (!instantiated)
            {
                Put(create());
            }
            return instance;
        }
    }
}