using System.Text.Json.Serialization;

namespace MediaProcessing.Shared.Models;

public class Video
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Title { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public string StoragePath { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public double? DurationSeconds { get; set; }
    public string? Resolution { get; set; }
    public string? Format { get; set; }
    public VideoStatus Status { get; set; } = VideoStatus.Uploaded;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? UpdatedAt { get; set; }

    [JsonIgnore]
    public List<VideoJob> Jobs { get; set; } = new();
}
