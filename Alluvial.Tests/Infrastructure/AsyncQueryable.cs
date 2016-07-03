using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial.Tests
{
    /// <summary>
    /// Used as a test double for EntityFramework aync queryable use cases.
    /// </summary>
    public static class AsyncQueryable
    {
        public static IQueryable<T> AsAsyncQueryable<T>(this IEnumerable<T> source)
        {
            return new InMemoryAsyncQueryable<T>(source);
        }

        public class InMemoryDbSet<T> : DbSet<T> where T : class
        {
            private IQueryable<T> source;

            protected InMemoryDbSet(IEnumerable<T> source)
            {
                if (source == null)
                {
                    throw new ArgumentNullException(nameof(source));
                }

                this.source = source.AsAsyncQueryable();
            }
        }

        private class InMemoryAsyncQueryable<T> :
            IQueryable<T>,
            IDbAsyncEnumerable<T>
        {
            private readonly IQueryable<T> inner;

            public InMemoryAsyncQueryable(IEnumerable<T> events)
            {
                if (events == null)
                {
                    throw new ArgumentNullException(nameof(events));
                }
                inner = events.AsQueryable();
            }

            public IEnumerator<T> GetEnumerator() => inner.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public Expression Expression => inner.Expression;

            public Type ElementType => inner.ElementType;

            public IQueryProvider Provider => new InMemoryAsyncQueryProvider<T>(inner.Provider);

            public IDbAsyncEnumerator<T> GetAsyncEnumerator() => new InMemoryAsyncEnumerator<T>(inner.GetEnumerator());

            IDbAsyncEnumerator IDbAsyncEnumerable.GetAsyncEnumerator() => GetAsyncEnumerator();
        }

        internal class InMemoryAsyncEnumerator<T> :
            IDbAsyncEnumerator<T>
        {
            private readonly IEnumerator<T> inner;

            public InMemoryAsyncEnumerator(IEnumerator<T> inner)
            {
                if (inner == null)
                {
                    throw new ArgumentNullException(nameof(inner));
                }
                this.inner = inner;
            }

            public void Dispose() => inner.Dispose();

            public Task<bool> MoveNextAsync(CancellationToken cancellationToken) =>
                inner.MoveNext().CompletedTask();

            T IDbAsyncEnumerator<T>.Current => inner.Current;

            public object Current => inner.Current;
        }

        internal class InMemoryAsyncQueryProvider<TEntity> : IQueryProvider
        {
            private readonly IQueryProvider _inner;

            internal InMemoryAsyncQueryProvider(IQueryProvider inner)
            {
                _inner = inner;
            }

            public IQueryable CreateQuery(Expression expression) =>
                new InMemoryAsyncEnumerable<TEntity>(expression);

            public IQueryable<TElement> CreateQuery<TElement>(Expression expression) =>
                new InMemoryAsyncEnumerable<TElement>(expression);

            public object Execute(Expression expression) =>
                _inner.Execute(expression);

            public TResult Execute<TResult>(Expression expression) => _inner.Execute<TResult>(expression);

            public Task<object> ExecuteAsync(Expression expression, CancellationToken cancellationToken) =>
                Execute(expression).CompletedTask();

            public Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken) =>
                Execute<TResult>(expression).CompletedTask();
        }

        internal class InMemoryAsyncEnumerable<T> : EnumerableQuery<T>, IDbAsyncEnumerable<T>, IQueryable<T>
        {
            public InMemoryAsyncEnumerable(IEnumerable<T> enumerable)
                : base(enumerable)
            {
            }

            public InMemoryAsyncEnumerable(Expression expression)
                : base(expression)
            {
            }

            public IDbAsyncEnumerator<T> GetAsyncEnumerator() =>
                new InMemoryAsyncEnumerator<T>(this.AsEnumerable().GetEnumerator());

            IDbAsyncEnumerator IDbAsyncEnumerable.GetAsyncEnumerator() =>
                GetAsyncEnumerator();

            IQueryProvider IQueryable.Provider =>
                new InMemoryAsyncQueryProvider<T>(this);
        }
    }
}