using System;

namespace Alluvial
{
    /// <summary>
    /// Provides information about an error that occurs while running a stream catchup.
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    public class StreamCatchupError<TProjection>
    {
        /// <summary>
        /// A caught exception.
        /// </summary>
        public Exception Exception { get; internal set; }

        /// <summary>
        /// The projection being aggregated when the error occurred.
        /// </summary>
        public TProjection Projection { get; internal set; }

        /// <summary>
        /// Gets a value indicating whether the catchup should continue.
        /// </summary>
        internal bool ShouldContinue { get; private set; }

        /// <summary>
        /// Notified the catchup that the it should continue despite the error.
        /// </summary>
        public void Continue()
        {
            ShouldContinue = true;
        }
    }
}