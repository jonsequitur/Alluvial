using System;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    /// <summary>
    /// A link in the distributor delegator chain that is called when a lease becomes available. 
    /// </summary>
    /// <typeparam name="T">The type of the leased resource.</typeparam>
    /// <param name="lease">The lease.</param>
    /// <param name="next">A delegate to call the next handler in the pipeline.</param>
    public delegate Task DistributorPipeAsync<T>(
        Lease<T> lease,
        Func<Lease<T>, Task> next);
}