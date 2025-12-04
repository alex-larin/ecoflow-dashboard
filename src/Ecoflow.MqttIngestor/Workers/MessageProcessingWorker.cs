using System;
using System.Threading.Channels;
using Ecoflow.MqttIngestor.Messaging;
using Ecoflow.MqttIngestor.Persistence;
using Ecoflow.MqttIngestor.Processing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Ecoflow.MqttIngestor.Workers;

public sealed class MessageProcessingWorker : BackgroundService
{
    private const int StoreRetryCount = 3;
    private static readonly TimeSpan StoreRetryDelay = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan SchemaRetryDelay = TimeSpan.FromSeconds(10);

    private readonly ChannelReader<MqttEnvelope> _channelReader;
    private readonly IMessageParser _messageParser;
    private readonly IMessageRepository _repository;
    private readonly ILogger<MessageProcessingWorker> _logger;

    public MessageProcessingWorker(
        ChannelReader<MqttEnvelope> channelReader,
        IMessageParser messageParser,
        IMessageRepository repository,
        ILogger<MessageProcessingWorker> logger)
    {
        _channelReader = channelReader;
        _messageParser = messageParser;
        _repository = repository;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await EnsureSchemaWithRetryAsync(stoppingToken);

        await foreach (var envelope in _channelReader.ReadAllAsync(stoppingToken))
        {
            if (IsQuotaTopic(envelope.Topic))
            {
                var payload = System.Text.Encoding.UTF8.GetString(envelope.Payload);
                _logger.LogInformation("Received message from {Topic}: {Payload}", envelope.Topic, payload);
                continue;
            }

            if (!_messageParser.TryParse(envelope, out var ecoflowEvent, out var error))
            {
                // var payload = envelope.Payload.Length > 0 ? System.Text.Encoding.UTF8.GetString(envelope.Payload) : "<empty>";
                // _logger.LogWarning("Discarding message from topic {Topic}: {Reason}. Payload:{Payload}", envelope.Topic, error ?? "unknown error", payload);
                _logger.LogWarning("Discarding message from topic {Topic}: {Reason}.", envelope.Topic, error ?? "unknown error");
                continue;
            }

            await StoreWithRetryAsync(ecoflowEvent!, stoppingToken);
        }
    }

    private static bool IsQuotaTopic(string topic)
    {
        return topic.EndsWith("quota", StringComparison.OrdinalIgnoreCase);
    }

    private async Task EnsureSchemaWithRetryAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _repository.EnsureSchemaAsync(stoppingToken);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(exception, "Database schema initialization failed. Retrying in {DelaySeconds}s...", SchemaRetryDelay.TotalSeconds);
            }

            try
            {
                await Task.Delay(SchemaRetryDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
        }

        stoppingToken.ThrowIfCancellationRequested();
    }

    private async Task StoreWithRetryAsync(EcoflowEvent ecoflowEvent, CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= StoreRetryCount; attempt++)
        {
            try
            {
                await _repository.StoreAsync(ecoflowEvent, cancellationToken);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                if (attempt >= StoreRetryCount)
                {
                    _logger.LogError(exception, "Failed to persist message for device {DeviceId} after {AttemptCount} attempts", ecoflowEvent.DeviceId, StoreRetryCount);
                    return;
                }

                var backoff = TimeSpan.FromMilliseconds(StoreRetryDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));
                _logger.LogWarning(exception, "Failed to persist message for device {DeviceId}. Retrying in {DelaySeconds}s (attempt {Attempt}/{Total})", ecoflowEvent.DeviceId, backoff.TotalSeconds, attempt + 1, StoreRetryCount);

                try
                {
                    await Task.Delay(backoff, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
            }
        }
    }
}
