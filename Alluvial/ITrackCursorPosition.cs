namespace Alluvial
{
    internal interface ITrackCursorPosition
    {
        /// <summary>
        /// Gets a value indicating whether the cursor was advanced from its initial position at the time the instance was created.
        /// </summary>
        /// <value>
        ///   <c>true</c> if the cursor was advanced; otherwise, <c>false</c>.
        /// </value>
        bool CursorWasAdvanced { get;}
    }
}