using System;
using System.Threading.Tasks;

namespace Alluvial.Distributors
{
    public struct Lease
    {
        private readonly LeasableResource leasableResource;

        public Lease(LeasableResource leasableResource)
        {
            if (leasableResource == null)
            {
                throw new ArgumentNullException("leasableResource");
            }
            this.leasableResource = leasableResource;
        }

        public LeasableResource LeasableResource
        {
            get
            {
                return leasableResource;
            }
        }

        public async Task Extend(TimeSpan by)
        {
        }

        public override string ToString()
        {
            return LeasableResource.ToString();
        }
    }
}