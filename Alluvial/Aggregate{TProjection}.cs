using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Updates the state of a projection.
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    /// <param name="initial">The initial state of the projection.</param>
    /// <returns>The updated state of the projection.</returns>
    public delegate Task<TProjection> Aggregate<TProjection>(TProjection initial);
}