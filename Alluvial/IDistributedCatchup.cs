using System;

namespace Alluvial
{
    internal interface IDistributedCatchup<TData>
    {
        void ConfigureChildCatchup(Func<IStreamCatchup<TData>, IStreamCatchup<TData>> configure);
    }
}