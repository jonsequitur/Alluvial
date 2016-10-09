using System;

namespace Alluvial.Fluent
{
    /// <summary>
    /// Defines cursor behaviors for a stream.
    /// </summary>
    public class CursorBuilder
    {
        internal CursorBuilder()
        {
        }

        /// <summary>
        /// Specifies the type of the cursor.
        /// </summary>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <returns></returns>
        public CursorBuilder<TCursor> By<TCursor>() =>
            new CursorBuilder<TCursor>
            {
                NewCursor = () => Cursor.New<TCursor>()
            };

        /// <summary>
        /// Specifies the starting value for new cursors for the stream.
        /// </summary>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="new">A delegate called when creating new cursors.</param>
        /// <returns></returns>
        public CursorBuilder<TCursor> StartsAt<TCursor>(Func<ICursor<TCursor>> @new) =>
            new CursorBuilder<TCursor>
            {
                NewCursor = @new
            };

        /// <summary>
        /// Specifies the starting value for new cursors for the stream.
        /// </summary>
        /// <summary>
        /// Startses at.
        /// </summary>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="position">The position for new cursors created for the stream.</param>
        /// <returns></returns>
        public CursorBuilder<TCursor> StartsAt<TCursor>(TCursor position) =>
            new CursorBuilder<TCursor>
            {
                NewCursor = () => Cursor.New(position)
            };
    }
}