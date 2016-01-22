using System.Diagnostics;

namespace Alluvial
{
    /// <summary>
    /// A projection.
    /// </summary>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    [DebuggerDisplay("{ToString()}")]
    public class Projection<TValue>
    {
        private static readonly string projectionName = typeof (Projection<TValue>).ReadableName();

        /// <summary>
        /// Gets or sets the value of the projection.
        /// </summary>
        /// <value>
        /// The value of the projection.
        /// </value>
        public TValue Value { get; set; }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            string valueString;

            var v = Value;
            if (v != null)
            {
                valueString = v.ToString();
            }
            else
            {
                valueString = "null";
            }

            return $"{ProjectionName}: {valueString}";
        }

        /// <summary>
        /// Gets the name of the projection.
        /// </summary>
        /// <value>
        /// The name of the projection.
        /// </value>
        protected virtual string ProjectionName => projectionName;
    }
}