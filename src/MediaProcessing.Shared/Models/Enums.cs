namespace MediaProcessing.Shared.Models;

public enum JobType
{
    ExtractMetadata = 1,
    GenerateThumbnail = 2,
    Transcode480p = 3,
    Transcode720p = 4,
    Transcode1080p = 5
}

public enum JobStatus
{
    Pending = 0,
    Processing = 1,
    Completed = 2,
    Failed = 3
}

public enum VideoStatus
{
    Uploaded = 0,
    Processing = 1,
    Ready = 2,
    Error = 3
}
