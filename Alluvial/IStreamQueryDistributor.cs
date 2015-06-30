using System;
using System.Threading.Tasks;

namespace Alluvial
{
    public interface IStreamQueryDistributor : IDisposable
    {
        void OnReceive(Func<DistributorUnitOfWork, Task> onReceive);
        Task Start();
        Task Stop();
    }
}