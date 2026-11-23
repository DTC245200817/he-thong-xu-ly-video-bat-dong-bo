using MediaProcessing.Shared.Models;

namespace MediaProcessing.Shared.Messaging;

public class VideoJobMessage
{
    public Guid JobId { get; set; }
    public Guid VideoId { get; set; }
    public JobType JobType { get; set; }
    public string FilePath { get; set; } = string.Empty;
    public string CorrelationId { get; set; } = string.Empty;
    public int RetryCount { get; set; } = 0;
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
