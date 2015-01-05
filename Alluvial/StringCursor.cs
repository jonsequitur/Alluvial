namespace Alluvial
{
    internal class StringCursor : ICursor
    {
        private string position;

        public StringCursor(string position = "",
                            bool ascending = true)
        {
            Ascending = ascending;
            this.position = position;
        }

        public bool Ascending { get; private set; }

        public dynamic Position
        {
            get
            {
                return position;
            }
        }

        public void AdvanceTo(dynamic position)
        {
            this.position = position;
        }
    }
}