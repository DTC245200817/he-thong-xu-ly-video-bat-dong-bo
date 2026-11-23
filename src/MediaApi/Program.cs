using System.Diagnostics;
using System.Text.Json;
using MediaProcessing.Shared.Data;
using MediaProcessing.Shared.DTOs;
using MediaProcessing.Shared.Messaging;
using MediaProcessing.Shared.Models;
using MediaProcessing.Shared.Storage;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

var builder = WebApplication.CreateBuilder(args);

// Configure max request body size for video uploads (up to 500MB)
builder.Services.Configure<FormOptions>(options =>
{
    options.MultipartBodyLengthLimit = 524_288_000;
});

// Configure Services
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new() { Title = "Media Processing API", Version = "v1", Description = "Distributed Asynchronous Video Processing API with RabbitMQ" });
});

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy => policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod());
});

// File Storage Service
var storageBasePath = StoragePaths.ResolveRootStorage();
builder.Services.AddSingleton<IFileStorageService>(_ => new LocalFileStorageService(storageBasePath));

// Database Context Setup: Prefer PostgreSQL if USE_POSTGRES=true, otherwise fallback to SQLite
var pgConnStr = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING") 
    ?? builder.Configuration.GetConnectionString("Postgres");
var sqliteConnStr = $"Data Source={Path.Combine(storageBasePath, "media.db")}";

builder.Services.AddDbContext<MediaDbContext>(options =>
{
    var usePostgres = Environment.GetEnvironmentVariable("USE_POSTGRES") == "true";
    
    if (usePostgres && !string.IsNullOrEmpty(pgConnStr))
    {
        options.UseNpgsql(pgConnStr);
    }
    else
    {
        options.UseSqlite(sqliteConnStr);
    }
});

// RabbitMQ Publisher with Graceful Fallback
builder.Services.AddSingleton<IRabbitMqPublisher>(sp =>
{
    var logger = sp.GetRequiredService<ILogger<Program>>();
    var publisherLogger = sp.GetRequiredService<ILogger<RabbitMqPublisher>>();
    var hostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") 
        ?? builder.Configuration["RabbitMQ:HostName"] 
        ?? "localhost";
    var port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT") 
        ?? builder.Configuration["RabbitMQ:Port"], out var p) ? p : 5672;
    var user = Environment.GetEnvironmentVariable("RABBITMQ_USER") 
        ?? builder.Configuration["RabbitMQ:UserName"] 
        ?? "guest";
    var pass = Environment.GetEnvironmentVariable("RABBITMQ_PASS") 
        ?? builder.Configuration["RabbitMQ:Password"] 
        ?? "guest";

    IConnection? connection = null;
    try
    {
        var factory = new ConnectionFactory
        {
            HostName = hostName,
            Port = port,
            UserName = user,
            Password = pass,
            RequestedConnectionTimeout = TimeSpan.FromSeconds(2),
            DispatchConsumersAsync = true,
            AutomaticRecoveryEnabled = true
        };

        logger.LogInformation("Connecting to RabbitMQ at {Host}:{Port}...", hostName, port);
        connection = factory.CreateConnection();
    }
    catch (Exception ex)
    {
        logger.LogWarning("RabbitMQ connection failed: {Message}. Running in standalone mode with In-Memory fallback channel.", ex.Message);
    }

    return new RabbitMqPublisher(connection, publisherLogger);
});

builder.Services.AddHostedService<MediaApi.FallbackJobProcessor>();

var app = builder.Build();

// Ensure Database & Directory Created
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<MediaDbContext>();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    try
    {
        db.Database.EnsureCreated();
        logger.LogInformation("Database verified and ready.");
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Database initialization error: {Message}", ex.Message);
    }
}

app.UseCors();
app.UseSwagger();
app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "Media Processing API v1"));
app.UseDefaultFiles();
app.UseStaticFiles();

// Correlation ID Middleware
app.Use(async (context, next) =>
{
    var correlationId = context.Request.Headers["X-Correlation-ID"].FirstOrDefault() ?? Guid.NewGuid().ToString();
    context.Response.Headers["X-Correlation-ID"] = correlationId;
    context.Items["CorrelationId"] = correlationId;
    await next();
});

// Helper URL Generator
string GetBaseUrl(HttpRequest req) => $"{req.Scheme}://{req.Host}";

// ---------------------------------------------------------------------
// 1. POST /videos - Upload or Register a Video
// ---------------------------------------------------------------------
app.MapPost("/videos", async (
    HttpRequest request,
    MediaDbContext db,
    IFileStorageService storage,
    ILogger<Program> logger) =>
{
    var correlationId = (string)request.HttpContext.Items["CorrelationId"]!;
    logger.LogInformation("[{CorrelationId}] Processing video upload/registration...", correlationId);

    string title = "Untitled Video";
    string fileName;
    string relativePath;
    long fileSize = 0;

    if (request.HasFormContentType && request.Form.Files.Count > 0)
    {
        var file = request.Form.Files[0];
        fileName = file.FileName;
        title = request.Form["title"].FirstOrDefault() ?? Path.GetFileNameWithoutExtension(fileName);
        fileSize = file.Length;

        using var stream = file.OpenReadStream();
        relativePath = await storage.SaveFileAsync("uploads", fileName, stream);
    }
    else
    {
        // JSON body or empty upload fallback (create a simulated video file)
        using var reader = new StreamReader(request.Body);
        var body = await reader.ReadToEndAsync();
        if (!string.IsNullOrWhiteSpace(body))
        {
            try
            {
                using var jsonDoc = JsonDocument.Parse(body);
                if (jsonDoc.RootElement.TryGetProperty("title", out var titleProp))
                    title = titleProp.GetString() ?? title;
            }
            catch { }
        }

        fileName = $"{title.Replace(" ", "_")}.mp4";
        var simulatedContent = $"SIMULATED_VIDEO_DATA_FOR_{title}_{DateTime.UtcNow:O}";
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(simulatedContent));
        relativePath = await storage.SaveFileAsync("uploads", fileName, stream);
        fileSize = stream.Length;
    }

    var video = new Video
    {
        Id = Guid.NewGuid(),
        Title = title,
        FileName = fileName,
        StoragePath = relativePath,
        FileSize = fileSize,
        Format = Path.GetExtension(fileName).TrimStart('.').ToLowerInvariant(),
        Status = VideoStatus.Uploaded,
        CreatedAt = DateTime.UtcNow
    };

    db.Videos.Add(video);
    await db.SaveChangesAsync();

    logger.LogInformation("[{CorrelationId}] Video created with Id: {VideoId}, Path: {Path}", 
        correlationId, video.Id, relativePath);

    var baseUrl = GetBaseUrl(request);
    var response = ToVideoResponse(video, baseUrl);
    return Results.Created($"/videos/{video.Id}", response);
})
.WithName("CreateVideo")
.WithSummary("Upload or register a new video");

// ---------------------------------------------------------------------
// 2. POST /videos/{id}/jobs - Create a processing job for video
// ---------------------------------------------------------------------
app.MapPost("/videos/{id}/jobs", async (
    Guid id,
    CreateJobRequest jobRequest,
    HttpRequest request,
    MediaDbContext db,
    IRabbitMqPublisher publisher,
    IFileStorageService storage,
    ILogger<Program> logger) =>
{
    var correlationId = (string)request.HttpContext.Items["CorrelationId"]!;
    var video = await db.Videos.Include(v => v.Jobs).FirstOrDefaultAsync(v => v.Id == id);
    if (video == null)
    {
        return Results.NotFound(new { error = $"Video with ID '{id}' was not found." });
    }

    var job = new VideoJob
    {
        Id = Guid.NewGuid(),
        VideoId = video.Id,
        JobType = jobRequest.JobType,
        Status = JobStatus.Pending,
        Progress = 0,
        CorrelationId = correlationId,
        CreatedAt = DateTime.UtcNow
    };

    video.Status = VideoStatus.Processing;
    video.UpdatedAt = DateTime.UtcNow;

    db.VideoJobs.Add(job);
    await db.SaveChangesAsync();

    // Publish to RabbitMQ
    var message = new VideoJobMessage
    {
        JobId = job.Id,
        VideoId = video.Id,
        JobType = job.JobType,
        FilePath = video.StoragePath,
        CorrelationId = correlationId,
        RetryCount = 0,
        Timestamp = DateTime.UtcNow
    };

    await publisher.PublishJobAsync(message);
    logger.LogInformation("[{CorrelationId}] Dispatched job {JobId} ({JobType}) to queue for Video: {VideoId}",
        correlationId, job.Id, job.JobType, video.Id);

    var response = ToJobResponse(job);
    return Results.Accepted($"/jobs/{job.Id}", response);
})
.WithName("CreateJob")
.WithSummary("Create a processing job for a video and enqueue it");

// ---------------------------------------------------------------------
// 3. GET /videos - Get list of videos
// ---------------------------------------------------------------------
app.MapGet("/videos", async (
    HttpRequest request,
    MediaDbContext db) =>
{
    var videos = await db.Videos
        .Include(v => v.Jobs)
        .OrderByDescending(v => v.CreatedAt)
        .ToListAsync();

    var baseUrl = GetBaseUrl(request);
    var responses = videos.Select(v => ToVideoResponse(v, baseUrl)).ToList();
    return Results.Ok(responses);
})
.WithName("GetVideos")
.WithSummary("Get all videos and their processing statuses");

// ---------------------------------------------------------------------
// 4. GET /videos/{id} - Get a video by ID
// ---------------------------------------------------------------------
app.MapGet("/videos/{id}", async (
    Guid id,
    HttpRequest request,
    MediaDbContext db) =>
{
    var video = await db.Videos
        .Include(v => v.Jobs)
        .FirstOrDefaultAsync(v => v.Id == id);

    if (video == null)
    {
        return Results.NotFound(new { error = $"Video with ID '{id}' was not found." });
    }

    var baseUrl = GetBaseUrl(request);
    return Results.Ok(ToVideoResponse(video, baseUrl));
})
.WithName("GetVideoById")
.WithSummary("Get details of a specific video by ID");

// ---------------------------------------------------------------------
// 4b. POST /videos/{id}/process-all - Batch create standard jobs
// ---------------------------------------------------------------------
app.MapPost("/videos/{id}/process-all", async (
    Guid id,
    HttpRequest request,
    MediaDbContext db,
    IRabbitMqPublisher publisher,
    ILogger<Program> logger) =>
{
    var correlationId = (string)request.HttpContext.Items["CorrelationId"]!;
    var video = await db.Videos.Include(v => v.Jobs).FirstOrDefaultAsync(v => v.Id == id);
    if (video == null)
    {
        return Results.NotFound(new { error = $"Video with ID '{id}' was not found." });
    }

    var jobTypes = new[] { JobType.ExtractMetadata, JobType.GenerateThumbnail, JobType.Transcode720p };
    var createdJobs = new List<JobResponse>();

    foreach (var jobType in jobTypes)
    {
        var job = new VideoJob
        {
            Id = Guid.NewGuid(),
            VideoId = video.Id,
            JobType = jobType,
            Status = JobStatus.Pending,
            Progress = 0,
            CorrelationId = correlationId,
            CreatedAt = DateTime.UtcNow
        };

        db.VideoJobs.Add(job);
        await db.SaveChangesAsync();

        await publisher.PublishJobAsync(new VideoJobMessage
        {
            JobId = job.Id,
            VideoId = video.Id,
            JobType = job.JobType,
            FilePath = video.StoragePath,
            CorrelationId = correlationId,
            Timestamp = DateTime.UtcNow
        });

        createdJobs.Add(ToJobResponse(job));
    }

    video.Status = VideoStatus.Processing;
    video.UpdatedAt = DateTime.UtcNow;
    await db.SaveChangesAsync();

    logger.LogInformation("[{CorrelationId}] Batch created 3 jobs for Video: {VideoId}", correlationId, video.Id);
    return Results.Accepted($"/videos/{video.Id}", new { message = "Batch processing jobs created", jobs = createdJobs });
})
.WithName("ProcessAllJobs")
.WithSummary("Batch dispatch all standard jobs (Metadata, Thumbnail, 720p)");

// ---------------------------------------------------------------------
// 4c. DELETE /videos/{id} - Delete video and all related jobs
// ---------------------------------------------------------------------
app.MapDelete("/videos/{id}", async (
    Guid id,
    MediaDbContext db,
    ILogger<Program> logger) =>
{
    var video = await db.Videos.Include(v => v.Jobs).FirstOrDefaultAsync(v => v.Id == id);
    if (video == null)
    {
        return Results.NotFound(new { error = $"Video with ID '{id}' was not found." });
    }

    db.Videos.Remove(video);
    await db.SaveChangesAsync();

    logger.LogInformation("Deleted video with ID {VideoId} and related jobs", id);
    return Results.Ok(new { message = $"Video '{video.Title}' and all associated jobs deleted successfully." });
})
.WithName("DeleteVideo")
.WithSummary("Delete a video and its processing jobs");

// ---------------------------------------------------------------------
// 5. GET /jobs/{id} - Get a job by ID
// ---------------------------------------------------------------------
app.MapGet("/jobs/{id}", async (
    Guid id,
    MediaDbContext db) =>
{
    var job = await db.VideoJobs.FirstOrDefaultAsync(j => j.Id == id);
    if (job == null)
    {
        return Results.NotFound(new { error = $"Job with ID '{id}' was not found." });
    }

    return Results.Ok(ToJobResponse(job));
})
.WithName("GetJobById")
.WithSummary("Get status and progress of a processing job");

// ---------------------------------------------------------------------
// 6. GET /storage/{folder}/{fileName} - File Download/Streaming Endpoint
// ---------------------------------------------------------------------
app.MapGet("/storage/{folder}/{fileName}", (
    string folder,
    string fileName,
    IFileStorageService storage) =>
{
    var relativePath = $"{folder}/{fileName}";
    if (!storage.FileExists(relativePath))
    {
        return Results.NotFound(new { error = "File not found." });
    }

    var stream = storage.OpenRead(relativePath);
    if (stream == null) return Results.NotFound();

    var ext = Path.GetExtension(fileName).ToLowerInvariant();
    var contentType = ext switch
    {
        ".mp4" => "video/mp4",
        ".webm" => "video/webm",
        ".mkv" => "video/x-matroska",
        ".jpg" or ".jpeg" => "image/jpeg",
        ".png" => "image/png",
        ".svg" => "image/svg+xml",
        ".json" => "application/json",
        _ => "application/octet-stream"
    };

    return Results.File(stream, contentType, enableRangeProcessing: true);
})
.WithName("GetStoredFile")
.WithSummary("Download or stream a stored file (video, thumbnail, etc.)");

// ---------------------------------------------------------------------
// 7. GET /stats - System metrics and stats
// ---------------------------------------------------------------------
app.MapGet("/stats", async (MediaDbContext db, IRabbitMqPublisher publisher) =>
{
    var totalVideos = await db.Videos.CountAsync();
    var pendingJobs = await db.VideoJobs.CountAsync(j => j.Status == JobStatus.Pending);
    var processingJobs = await db.VideoJobs.CountAsync(j => j.Status == JobStatus.Processing);
    var completedJobs = await db.VideoJobs.CountAsync(j => j.Status == JobStatus.Completed);
    var failedJobs = await db.VideoJobs.CountAsync(j => j.Status == JobStatus.Failed);

    return Results.Ok(new
    {
        totalVideos,
        pendingJobs,
        processingJobs,
        completedJobs,
        failedJobs,
        brokerConnected = publisher.IsConnected,
        brokerMode = publisher.IsConnected ? "RabbitMQ Distributed" : "Standalone In-Memory Fallback"
    });
})
.WithName("GetSystemStats")
.WithSummary("Get system stats and queue counts");

// ---------------------------------------------------------------------
// 8. GET /healthz - System Health Check
// ---------------------------------------------------------------------
app.MapGet("/healthz", async (MediaDbContext db, IRabbitMqPublisher publisher) =>
{
    var dbOk = await db.Database.CanConnectAsync();
    var rmqOk = publisher.IsConnected;

    var status = dbOk && rmqOk ? "Healthy" : (dbOk ? "Degraded (Standalone Mode)" : "Unhealthy");
    var result = new
    {
        status,
        timestamp = DateTime.UtcNow,
        components = new
        {
            database = dbOk ? "Healthy" : "Unreachable",
            rabbitMQ = rmqOk ? "Healthy" : "Offline / Standalone Fallback Active"
        }
    };

    return Results.Ok(result);
})
.WithName("HealthCheck")
.WithSummary("Service Health Check");

app.Run();

// DTO Mapping Helpers
static VideoResponse ToVideoResponse(Video v, string baseUrl)
{
    var downloadUrl = string.IsNullOrEmpty(v.StoragePath) ? null : $"{baseUrl}/storage/{v.StoragePath}";
    
    // Find thumbnail from completed thumbnail jobs if any
    string? thumbnailUrl = null;
    var thumbJob = v.Jobs.FirstOrDefault(j => j.JobType == JobType.GenerateThumbnail && j.Status == JobStatus.Completed);
    if (thumbJob?.ResultData != null)
    {
        try
        {
            using var doc = JsonDocument.Parse(thumbJob.ResultData);
            if (doc.RootElement.TryGetProperty("thumbnailPath", out var pathProp))
            {
                thumbnailUrl = $"{baseUrl}/storage/{pathProp.GetString()}";
            }
        }
        catch { }
    }

    return new VideoResponse(
        v.Id,
        v.Title,
        v.FileName,
        v.FileSize,
        v.DurationSeconds,
        v.Resolution,
        v.Format,
        v.Status.ToString(),
        v.CreatedAt,
        v.UpdatedAt,
        downloadUrl,
        thumbnailUrl,
        v.Jobs.Select(ToJobResponse).ToList()
    );
}

static JobResponse ToJobResponse(VideoJob j) => new(
    j.Id,
    j.VideoId,
    j.JobType.ToString(),
    j.Status.ToString(),
    j.Progress,
    j.ResultData,
    j.ErrorMessage,
    j.CorrelationId,
    j.RetryCount,
    j.CreatedAt,
    j.StartedAt,
    j.CompletedAt
);
