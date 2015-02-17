namespace Alluvial
{
    internal class NoCursor : ICursor
    {
        public dynamic Position
        {
            get
            {
                return Cursor.StartOfStream;
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
        }

        public bool HasReached(dynamic point)
        {
            return false;
        }
    }
}