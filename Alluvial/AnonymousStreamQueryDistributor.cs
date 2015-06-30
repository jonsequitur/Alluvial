using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousStreamQueryDistributor : IStreamQueryDistributor
    {
        private readonly Func<Task> start;
        private readonly Action<Func<DistributorUnitOfWork, Task>> onReceived;
        private readonly Func<Task> stop;

        public AnonymousStreamQueryDistributor(
            Func<Task> start,
            Action<Func<DistributorUnitOfWork, Task>> onReceived,
            Func<Task> stop)
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
            this.start = start;
            this.onReceived = onReceived;
            this.stop = stop;
        }

        public void OnReceive(Func<DistributorUnitOfWork, Task> onReceive)
        {
            onReceived(onReceive);
        }

        public Task Start()
        {
            return start();
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