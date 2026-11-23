namespace MediaProcessing.Shared.Models;

public class VideoJob
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public Guid VideoId { get; set; }
    public Video? Video { get; set; }
    public JobType JobType { get; set; }
    public JobStatus Status { get; set; } = JobStatus.Pending;
    public int Progress { get; set; } = 0;
    public string? ResultData { get; set; }
    public string? ErrorMessage { get; set; }
    public string CorrelationId { get; set; } = Guid.NewGuid().ToString();
    public int RetryCount { get; set; } = 0;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
}
