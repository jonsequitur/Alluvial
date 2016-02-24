using System;

namespace Alluvial
{
    /// <summary>
    /// Represents some resource for which a lease can be acquired for exclusive access.
    /// </summary>
    /// <typeparam name="T">The type of the resource.</typeparam>
    public class Leasable<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Leasable{T}"/> class.
        /// </summary>
        /// <param name="resource">The resource.</param>
        /// <param name="name">The name.</param>
        /// <exception cref="System.ArgumentNullException">name</exception>
        public Leasable(
            T resource,
            string name)
        {
            if (resource == null)
            {
                throw new ArgumentNullException(nameof(resource));
            }
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }
            Resource = resource;
            Name = name;
        }

        /// <summary>
        /// Gets the name of the resource.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets or sets the time at which the lease was last granted.
        /// </summary>
        public DateTimeOffset LeaseLastGranted { get; set; }

        /// <summary>
        /// Gets or sets the time at which the lease was last released.
        /// </summary>
        public DateTimeOffset LeaseLastReleased { get; set; }

        /// <summary>
        /// Gets the resource.
        /// </summary>
        public T Resource { get; private set; }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return $"leasable resource:{Name} (last granted @ {LeaseLastGranted}, last released @ {LeaseLastReleased})";
        }
    }
}