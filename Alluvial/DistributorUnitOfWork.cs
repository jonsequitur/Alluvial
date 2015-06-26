using System;
using System.Threading.Tasks;

namespace Alluvial
{
    public static class StreamQueryDistributor
    {
        public static IStreamQueryDistributor Trace(this IStreamQueryDistributor distributor)
        {
            return Create(
                start: () =>
                {
                    System.Diagnostics.Trace.WriteLine("[Distribute] Start");
                    return distributor.Start();
                },
                doWork: doWork =>
                {
                    distributor.OnReceive(async unitOfWork =>
                    {
                        System.Diagnostics.Trace.WriteLine("[Distribute] " + unitOfWork);
                        await doWork(unitOfWork);
                    });
                }, stop: () =>
                {
                    System.Diagnostics.Trace.WriteLine("[Distribute] Stop");
                    return distributor.Stop();
                });
        }

        private static IStreamQueryDistributor Create(Func<Task> start, Action<Func<DistributorUnitOfWork, Task>> doWork, Func<Task> stop)
        {
            return new AnonymousStreamQueryDistributor(start, doWork, stop);
        }
    }

    public class AnonymousStreamQueryDistributor : IStreamQueryDistributor
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

    public interface IStreamQueryDistributor : IDisposable
    {
        void OnReceive(Func<DistributorUnitOfWork, Task> onReceive);
        Task Start();
        Task Stop();
    }

    public class Lease
    {
        public Lease(string name, TimeSpan duration)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (duration == new TimeSpan())
            {
                throw new ArgumentException("duration cannot be zero.");
            }
            Name = name;
            Duration = duration;
        }

        public TimeSpan Duration { get; private set; }

        public string Name { get; private set; }

        public DateTimeOffset LastGranted { get; set; }

        public DateTimeOffset LastReleased { get; set; }
    }

    public struct DistributorUnitOfWork
    {
        private readonly Lease lease;

        internal DistributorUnitOfWork(Lease lease)
        {
            if (lease == null)
            {
                throw new ArgumentNullException("lease");
            }
            this.lease = lease;
        }

        public Lease Lease
        {
            get
            {
                return lease;
            }
        }

        public async Task Extend()
        {
        }

        public override string ToString()
        {
            return "distributor:" + lease.Name;
        }
    }
}