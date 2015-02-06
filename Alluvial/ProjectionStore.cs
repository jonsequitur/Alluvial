using System;
using System.Threading.Tasks;

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
    }
}