using System.Net.Http.Json;
using System.Text.Json;
using MediaProcessing.Shared.DTOs;
using MediaProcessing.Shared.Models;

Console.OutputEncoding = System.Text.Encoding.UTF8;

Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("===================================================================");
Console.WriteLine("   HỆ THỐNG XỬ LÝ VIDEO BẤT ĐỒNG BỘ - CONSOLE CLIENT TESTER        ");
Console.WriteLine("   Đề tài 14 - Phát triển ứng dụng phân tán (.NET 10 Minimal API)  ");
Console.WriteLine("===================================================================");
Console.ResetColor();

var apiUrl = Environment.GetEnvironmentVariable("MEDIA_API_URL") ?? "http://localhost:5000";
Console.WriteLine($"\n[1] Đang kết nối tới Media API tại: {apiUrl}...");

using var httpClient = new HttpClient { BaseAddress = new Uri(apiUrl), Timeout = TimeSpan.FromSeconds(30) };

// Health check
try
{
    var healthRes = await httpClient.GetAsync("/healthz");
    if (healthRes.IsSuccessStatusCode)
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("    ✓ Kết nối thành công! Media API đang hoạt động bình thường.");
        Console.ResetColor();
    }
    else
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"    ! Cảnh báo: API phản hồi status {healthRes.StatusCode}. Vẫn tiếp tục thử nghiệm.");
        Console.ResetColor();
    }
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"    ✗ Không thể kết nối tới Media API: {ex.Message}");
    Console.WriteLine("    Vui lòng đảm bảo MediaApi đã được khởi chạy.");
    Console.ResetColor();
    return;
}

// 1. Tạo Video mới
Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("\n[2] Đang tải lên / đăng ký video mới...");
Console.ResetColor();

var videoPayload = new { title = "Demo Video Phân Tán Đề Tài 14" };
var createVideoRes = await httpClient.PostAsJsonAsync("/videos", videoPayload);
createVideoRes.EnsureSuccessStatusCode();

var video = await createVideoRes.Content.ReadFromJsonAsync<VideoResponse>();
Console.WriteLine($"    ✓ Video đã được tạo thành công!");
Console.WriteLine($"      - Video ID   : {video!.Id}");
Console.WriteLine($"      - Tiêu đề    : {video.Title}");
Console.WriteLine($"      - File       : {video.FileName}");
Console.WriteLine($"      - Kích thước : {video.FileSize} bytes");
Console.WriteLine($"      - Trạng thái : {video.Status}");

// 2. Tạo các Jobs xử lý: Metadata, Thumbnail, Transcode720p
Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("\n[3] Đang gửi các yêu cầu xử lý (Jobs) vào RabbitMQ...");
Console.ResetColor();

var jobsToRun = new[]
{
    JobType.ExtractMetadata,
    JobType.GenerateThumbnail,
    JobType.Transcode720p
};

var createdJobIds = new List<Guid>();

foreach (var jobType in jobsToRun)
{
    var req = new CreateJobRequest(jobType);
    var jobRes = await httpClient.PostAsJsonAsync($"/videos/{video.Id}/jobs", req);
    if (jobRes.IsSuccessStatusCode)
    {
        var jobData = await jobRes.Content.ReadFromJsonAsync<JobResponse>();
        createdJobIds.Add(jobData!.Id);
        Console.WriteLine($"    ✓ Đã tạo Job: {jobType,-18} [JobId: {jobData.Id}] -> Trạng thái: {jobData.Status}");
    }
}

// 3. Polling theo dõi tiến trình
Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("\n[4] Đang theo dõi tiến trình xử lý từ Worker qua RabbitMQ (Live Polling)...");
Console.ResetColor();

var allDone = false;
var pollCount = 0;

while (!allDone && pollCount < 40)
{
    pollCount++;
    await Task.Delay(1500);

    var videoDetailRes = await httpClient.GetAsync($"/videos/{video.Id}");
    if (!videoDetailRes.IsSuccessStatusCode) continue;

    var currentVideo = await videoDetailRes.Content.ReadFromJsonAsync<VideoResponse>();
    if (currentVideo == null) continue;

    Console.Write($"\r[Lần check {pollCount:D2}] ");
    var completedCount = 0;

    foreach (var j in currentVideo.Jobs)
    {
        var icon = j.Status switch
        {
            "Completed" => "✓",
            "Processing" => "⚡",
            "Failed" => "✗",
            _ => "…"
        };
        Console.Write($"| {j.JobType}: {icon} {j.Progress,3}% ({j.Status}) ");
        if (j.Status == "Completed" || j.Status == "Failed") completedCount++;
    }

    if (completedCount == currentVideo.Jobs.Count && currentVideo.Jobs.Count > 0)
    {
        allDone = true;
    }
}

Console.WriteLine();

// 4. In kết quả cuối cùng
Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine("\n===================================================================");
Console.WriteLine("                   KẾT QUẢ XỬ LÝ TOÀN DIỆN                         ");
Console.WriteLine("===================================================================");
Console.ResetColor();

var finalRes = await httpClient.GetAsync($"/videos/{video.Id}");
var finalVideo = await finalRes.Content.ReadFromJsonAsync<VideoResponse>();

if (finalVideo != null)
{
    Console.WriteLine($"  * Video ID     : {finalVideo.Id}");
    Console.WriteLine($"  * Tiêu đề      : {finalVideo.Title}");
    Console.WriteLine($"  * Trạng thái   : {finalVideo.Status}");
    Console.WriteLine($"  * Độ phân giải : {finalVideo.Resolution ?? "N/A"}");
    Console.WriteLine($"  * Thời lượng   : {finalVideo.DurationSeconds}s");
    Console.WriteLine($"  * Thumbnail URL: {finalVideo.ThumbnailUrl ?? "N/A"}");
    Console.WriteLine($"  * File URL     : {finalVideo.DownloadUrl ?? "N/A"}");
    Console.WriteLine("\n  Chi tiết các jobs đã thực hiện:");

    foreach (var job in finalVideo.Jobs)
    {
        Console.WriteLine($"    ---------------------------------------------------------------");
        Console.WriteLine($"    + Loại Job     : {job.JobType}");
        Console.WriteLine($"    + Trạng thái   : {job.Status} (Tiến trình: {job.Progress}%)");
        Console.WriteLine($"    + CorrelationId: {job.CorrelationId}");
        if (!string.IsNullOrEmpty(job.ResultData))
        {
            Console.WriteLine($"    + Kết quả JSON : {job.ResultData}");
        }
        if (!string.IsNullOrEmpty(job.ErrorMessage))
        {
            Console.WriteLine($"    + Lỗi          : {job.ErrorMessage}");
        }
    }
}

Console.WriteLine("===================================================================\n");
