using System;
using System.Linq;

namespace Alluvial.ForItsCqrs
{
    public class DisposableQueryable<T> : IDisposable
    {
        private readonly Action dispose;

        public DisposableQueryable(Action dispose, IQueryable<T> queryable)
        {
            if (dispose == null) throw new ArgumentNullException("dispose");
            if (queryable == null) throw new ArgumentNullException("queryable");
            this.dispose = dispose;
            Queryable = queryable;
        }

        public IQueryable<T> Queryable { get; private set; }

        public void Dispose()
        {
            dispose();
        }
    }
}