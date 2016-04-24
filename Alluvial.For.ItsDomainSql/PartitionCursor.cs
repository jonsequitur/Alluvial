using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Alluvial.For.ItsDomainSql
{
    [Table("PartitionCursors", Schema = "Alluvial")]
    public class PartitionCursor :
        ICursor<long>
    {
        [Key]
        [Column(Order = 1)]
        [MaxLength(100)]
        public string StreamId { get; set; }

        [Key]
        [Column(Order = 2)]
        [MaxLength(100)]
        public string PartitionId { get; set; }

        public void AdvanceTo(long point)
        {
            Position = point;
        }

        public bool HasReached(long point)
        {
            return Position >= point;
        }

        public long Position { get; set; }
    }
}