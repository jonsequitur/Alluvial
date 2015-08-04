using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    internal class AnonymousDistributor<T> : IDistributor<T>
    {
        private readonly Func<Task> start;
        private readonly Action<Func<Lease<T>, Task>> onReceived;
        private readonly Func<Task> stop;
        private readonly Func<int, Task<IEnumerable<T>>> distribute;

        public AnonymousDistributor(
            Func<Task> start,
            Action<Func<Lease<T>, Task>> onReceived,
            Func<Task> stop, Func<int, Task<IEnumerable<T>>> distribute)
        {
            if (start == null)
            {
                throw new ArgumentNullException("start");
            }
            if (onReceived == null)
            {
                throw new ArgumentNullException("onReceived");
            }
            if (stop == null)
            {
                throw new ArgumentNullException("stop");
            }
            if (distribute == null)
            {
                throw new ArgumentNullException("distribute");
            }
            this.start = start;
            this.onReceived = onReceived;
            this.stop = stop;
            this.distribute = distribute;
        }

        public void OnReceive(Func<Lease<T>, Task> onReceive)
        {
            onReceived(onReceive);
        }

        public Task Start()
        {
            return start();
        }

        public Task<IEnumerable<T>> Distribute(int count)
        {
            return distribute(count);
        }

        public Task Stop()
        {
            return stop();
        }

        public void Dispose()
        {
            Stop();
        }
    }
}