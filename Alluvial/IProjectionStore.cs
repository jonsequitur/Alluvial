using System;
using System.Threading.Tasks;

namespace Alluvial
{
    public interface IProjectionStore<in TKey, TProjection>
    {
        Task Put(TProjection projection);

        Task<TProjection> Get(TKey key);
    }
}