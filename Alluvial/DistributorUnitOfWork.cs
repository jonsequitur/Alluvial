using System;
using System.Threading.Tasks;

namespace Alluvial
{
    public struct DistributorUnitOfWork
    {
        private readonly DistributorLease lease;

        public DistributorUnitOfWork(DistributorLease lease)
        {
            if (lease == null)
            {
                throw new ArgumentNullException("lease");
            }
            this.lease = lease;
        }

        public DistributorLease Lease
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