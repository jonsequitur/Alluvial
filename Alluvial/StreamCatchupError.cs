using System;

namespace Alluvial
{
    /// <summary>
    /// Provides information about an error that occurs while running a stream catchup.
    /// </summary>
    public class StreamCatchupError
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamCatchupError"/> class.
        /// </summary>
        /// <param name="exception">The exception.</param>
        /// <exception cref="System.ArgumentNullException"></exception>
        public StreamCatchupError(Exception exception)
        {
            if (exception == null)
            {
                throw new ArgumentNullException(nameof(exception));
            }
            Exception = exception;
        }

        /// <summary>
        /// A caught exception.
        /// </summary>
        public Exception Exception { get; }

        /// <summary>
        /// Gets a value indicating whether the catchup should continue.
        /// </summary>
        internal bool ShouldContinue { get; private set; }

        /// <summary>
        /// Notified the catchup that the it should continue despite the error.
        /// </summary>
        public void Continue() => ShouldContinue = true;

        /// <summary>
        /// Creates a <see cref="StreamCatchupError{TProjection}" /> instance.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <param name="exception">The exception.</param>
        /// <param name="projection">The projection.</param>
        /// <returns></returns>
        public static StreamCatchupError<TProjection> Create<TProjection>(Exception exception, TProjection projection) =>
            new StreamCatchupError<TProjection>(exception, projection);
    }
}