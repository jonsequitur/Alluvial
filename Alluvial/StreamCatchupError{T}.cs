using System;

namespace Alluvial
{
    /// <summary>
    /// Provides information about an error that occurs while running a stream catchup.
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    public class StreamCatchupError<TProjection> : StreamCatchupError
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamCatchupError{TProjection}"/> class.
        /// </summary>
        /// <param name="exception">The exception.</param>
        /// <param name="projection">The projection.</param>
        public StreamCatchupError(Exception exception, TProjection projection) : base(exception)
        {
            Projection = projection;
        }

        /// <summary>
        /// The projection being aggregated when the error occurred.
        /// </summary>
        public TProjection Projection { get; }
    }
}