using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousDistributor<T> : IDistributor<T>
    {
        private readonly Func<Task> start;
        private readonly Action<DistributorPipeAsync<T>> onReceive;
        private readonly Action<Action<Exception, Lease<T>>> onException;
        private readonly Func<Task> stop;
        private readonly Func<int, Task<IEnumerable<T>>> distribute;

        public AnonymousDistributor(
            Func<Task> start,
            Action<DistributorPipeAsync<T>> onReceive,
            Func<Task> stop,
            Func<int, Task<IEnumerable<T>>> distribute, Action<Action<Exception, Lease<T>>> onException)
        {
            if (start == null)
            {
                throw new ArgumentNullException(nameof(start));
            }
            if (onReceive == null)
            {
                throw new ArgumentNullException(nameof(onReceive));
            }

            if (stop == null)
            {
                throw new ArgumentNullException(nameof(stop));
            }
            if (distribute == null)
            {
                throw new ArgumentNullException(nameof(distribute));
            }

            this.start = start;
            this.onReceive = onReceive;
            this.stop = stop;
            this.distribute = distribute;
            this.onException = onException;
        }

        public void OnReceive(DistributorPipeAsync<T> receive) => 
            onReceive(receive);

        public void OnException(Action<Exception, Lease<T>> notify) =>
            onException(notify);
     

        public Task Start() => start();

        public Task<IEnumerable<T>> Distribute(int count) => distribute(count);

        public Task Stop() => stop();

        public void Dispose() => Stop();
    }
}