using System;
using System.Linq;

namespace Alluvial.Tests.BankDomain
{
    public class BalanceProjection : IMapProjection, ICursor
    {
        public decimal Balance { get; set; }

        public string AggregateId { get; set; }

        public int SequenceNumber { get; set; }

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
    }
}