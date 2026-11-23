using MediaProcessing.Shared.Models;

namespace MediaProcessing.Shared.DTOs;

public record CreateJobRequest(JobType JobType);

public record JobResponse(
    Guid Id,
    Guid VideoId,
    string JobType,
    string Status,
    int Progress,
    string? ResultData,
    string? ErrorMessage,
    string CorrelationId,
    int RetryCount,
    DateTime CreatedAt,
    DateTime? StartedAt,
    DateTime? CompletedAt
);

public record VideoResponse(
    Guid Id,
    string Title,
    string FileName,
    long FileSize,
    double? DurationSeconds,
    string? Resolution,
    string? Format,
    string Status,
    DateTime CreatedAt,
    DateTime? UpdatedAt,
    string? DownloadUrl,
    string? ThumbnailUrl,
    List<JobResponse> Jobs
);
