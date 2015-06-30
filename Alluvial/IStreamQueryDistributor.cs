using System;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    public interface IStreamQueryDistributor : IDisposable
    {
        void OnReceive(Func<Lease, Task> onReceive);
        Task Start();
        Task Stop();
    }
}