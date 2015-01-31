namespace Alluvial.Tests.BankDomain
{
    public class ProjectionBase : IMapProjection, ICursor
    {
        public string AggregateId { get; set; }

        public int CursorPosition { get; set; }

        dynamic ICursor.Position
        {
            get
            {
                return CursorPosition;
            }
        }

        public bool Ascending
        {
            get
            {
                return true;
            }
        }

        public void AdvanceTo(dynamic position)
        {
            CursorPosition = position;
        }

        public bool HasReached(dynamic point)
        {
            return Cursor.HasReached(
                CursorPosition.CompareTo(point.StreamRevision),
                Ascending);
        }
    }
}