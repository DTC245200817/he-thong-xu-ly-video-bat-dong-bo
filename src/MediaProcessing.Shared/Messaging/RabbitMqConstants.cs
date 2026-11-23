namespace MediaProcessing.Shared.Messaging;

public static class RabbitMqConstants
{
    public const string ExchangeName = "video.exchange";
    public const string DlxExchangeName = "video.exchange.dlq";

    public const string JobsQueueName = "video.jobs.queue";
    public const string DlqQueueName = "video.jobs.dlq";

    public const string JobRoutingKey = "video.job";
    public const string DlqRoutingKey = "video.job.dlq";
}
