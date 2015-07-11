using System;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    internal class AnonymousStreamQueryDistributor : IStreamQueryDistributor
    {
        private readonly Func<Task> start;
        private readonly Action<Func<Lease, Task>> onReceived;
        private readonly Func<Task> stop;
        private readonly Func<int, Task> distribute;

        public AnonymousStreamQueryDistributor(
            Func<Task> start,
            Action<Func<Lease, Task>> onReceived,
            Func<Task> stop, Func<int, Task> distribute)
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

        public void OnReceive(Func<Lease, Task> onReceive)
        {
            onReceived(onReceive);
        }

        public Task Start()
        {
            return start();
        }

        public Task Distribute(int count)
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