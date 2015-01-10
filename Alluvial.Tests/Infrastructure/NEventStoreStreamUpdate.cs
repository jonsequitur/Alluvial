namespace Alluvial.Tests
{
    public class NEventStoreStreamUpdate
    {
        public string StreamId { get; set; }
        public string CheckpointToken { get; set; }
        public int StreamRevision { get; set; }
    }
}