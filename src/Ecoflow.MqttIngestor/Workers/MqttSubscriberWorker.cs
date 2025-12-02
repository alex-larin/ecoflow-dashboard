using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Ecoflow.MqttIngestor.Messaging;
using Ecoflow.MqttIngestor.Services;
using Ecoflow.MqttIngestor.Services.Models;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;

namespace Ecoflow.MqttIngestor.Workers;

public sealed class MqttSubscriberWorker : BackgroundService
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan HealthCheckDelay = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromMinutes(1);
    private const string HeartbeatProtocolVersion = "1.0";
    private const string HeartbeatOperateType = "TCP";
    private const string HeartbeatSource = "EcoflowDashboard";
    private const int HeartbeatCmdSet = 32;
    private const int HeartbeatCmdId = 39;
    private const int HeartbeatLcdTimeSeconds = 60;
    private static readonly JsonSerializerOptions HeartbeatSerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly IMqttClient _mqttClient;
    private readonly MqttClientFactory _mqttFactory;
    private readonly ChannelWriter<MqttEnvelope> _channelWriter;
    private readonly IAccountInventory _accountInventory;
    private readonly ILogger<MqttSubscriberWorker> _logger;
    private CancellationToken _executionToken;
    private IReadOnlyList<EcoflowDevice> _devices = Array.Empty<EcoflowDevice>();
    private CertificationData? _certification;
    private int _accountInventoryVersion;
    private DateTimeOffset _nextHeartbeatUtc = DateTimeOffset.MinValue;

    public MqttSubscriberWorker(
        IMqttClient mqttClient,
        MqttClientFactory mqttFactory,
        ChannelWriter<MqttEnvelope> channelWriter,
        IAccountInventory accountInventory,
        ILogger<MqttSubscriberWorker> logger)
    {
        _mqttClient = mqttClient;
        _mqttFactory = mqttFactory;
        _channelWriter = channelWriter;
        _accountInventory = accountInventory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _executionToken = stoppingToken;

        _logger.LogInformation("Waiting for EcoFlow account inventory before establishing MQTT connection...");
        await _accountInventory.WaitUntilReadyAsync(stoppingToken);

        await UpdateInventorySnapshotAsync(waitForNextUpdate: false, stoppingToken);
        var initialCertification = GetCertificationOrThrow();

        _logger.LogInformation(
            "EcoFlow account inventory ready with {DeviceCount} device(s) for account {Account}.",
            _devices.Count,
            initialCertification.CertificateAccount);

        _mqttClient.ApplicationMessageReceivedAsync += HandleApplicationMessageAsync;
        _mqttClient.ConnectedAsync += OnConnectedAsync;
        _mqttClient.DisconnectedAsync += OnDisconnectedAsync;

        while (!stoppingToken.IsCancellationRequested)
        {
            if (!_mqttClient.IsConnected)
            {
                var connected = await TryConnectAndSubscribeAsync(stoppingToken);
                if (!connected)
                {
                    _logger.LogInformation("Refreshing EcoFlow account inventory before the next MQTT connection attempt...");
                    await UpdateInventorySnapshotAsync(waitForNextUpdate: true, stoppingToken);
                    await Task.Delay(ReconnectDelay, stoppingToken);
                    continue;
                }
            }

            if (_mqttClient.IsConnected)
            {
                await TrySendHeartbeatAsync(stoppingToken);
            }

            await Task.Delay(HealthCheckDelay, stoppingToken);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _mqttClient.ApplicationMessageReceivedAsync -= HandleApplicationMessageAsync;
        _mqttClient.ConnectedAsync -= OnConnectedAsync;
        _mqttClient.DisconnectedAsync -= OnDisconnectedAsync;

        if (_mqttClient.IsConnected)
        {
            await _mqttClient.DisconnectAsync(cancellationToken: cancellationToken);
        }

        await base.StopAsync(cancellationToken);
    }

    private async Task<bool> TryConnectAndSubscribeAsync(CancellationToken cancellationToken)
    {
        var certification = _certification;
        if (certification is null)
        {
            _logger.LogError("Cannot connect to MQTT broker because certification data is unavailable.");
            return false;
        }

        try
        {
            var clientOptions = BuildClientOptions(certification);
            await _mqttClient.ConnectAsync(clientOptions, cancellationToken);

            var subscribeBuilder = _mqttFactory.CreateSubscribeOptionsBuilder();
            var addedTopic = false;
            foreach (var device in _devices)
            {
                var quotaTopic = $"/open/{certification.CertificateAccount}/{device.SerialNumber}/quota";
                var setReplyTopic = $"/open/{certification.CertificateAccount}/{device.SerialNumber}/set_reply";

                subscribeBuilder.WithTopicFilter(filter => filter
                    .WithTopic(quotaTopic)
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce));
                subscribeBuilder.WithTopicFilter(filter => filter
                    .WithTopic(setReplyTopic)
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce));
                addedTopic = true;
            }

            if (!addedTopic)
            {
                _logger.LogWarning("No MQTT topics were generated for subscription.");
                return false;
            }

            var subscribeOptions = subscribeBuilder.Build();
            await _mqttClient.SubscribeAsync(subscribeOptions, cancellationToken);
            _logger.LogInformation(
                "Subscribed to {TopicCount} topics for {DeviceCount} device(s).",
                subscribeOptions.TopicFilters.Count,
                _devices.Count);
            return true;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Failed to connect or subscribe to MQTT broker {Host}:{Port}", certification.Url, certification.Port);
            return false;
        }
    }

    private Task HandleApplicationMessageAsync(MqttApplicationMessageReceivedEventArgs args)
    {
        var payload = args.ApplicationMessage.Payload;
        var buffer = payload.IsEmpty ? Array.Empty<byte>() : CopyToByteArray(payload);
        var envelope = new MqttEnvelope(args.ApplicationMessage.Topic, buffer, DateTimeOffset.UtcNow);

        LogHeartbeatResponseIfApplicable(args.ApplicationMessage.Topic, buffer);

        return _channelWriter.WriteAsync(envelope, _executionToken).AsTask();
    }

    private Task OnConnectedAsync(MqttClientConnectedEventArgs _)
    {
        var certification = GetCertificationOrThrow();
        _logger.LogInformation("Connected to MQTT broker {Host}:{Port}", certification.Url, certification.Port);
        return Task.CompletedTask;
    }

    private Task OnDisconnectedAsync(MqttClientDisconnectedEventArgs args)
    {
        if (args.Exception is not null)
        {
            _logger.LogWarning(args.Exception, "MQTT connection lost. Reconnecting in {Delay}s", ReconnectDelay.TotalSeconds);
        }
        else
        {
            _logger.LogWarning("MQTT connection lost. Reconnecting in {Delay}s", ReconnectDelay.TotalSeconds);
        }

        return Task.CompletedTask;
    }

    private MqttClientOptions BuildClientOptions(CertificationData certification)
    {
        if (!int.TryParse(certification.Port, out var port))
        {
            throw new InvalidOperationException($"Invalid MQTT port value '{certification.Port}'.");
        }

        var builder = _mqttFactory.CreateClientOptionsBuilder()
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithTcpServer(certification.Url, port)
            .WithCleanStart(false)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(60))
            .WithCredentials(certification.CertificateAccount, certification.CertificatePassword)
            .WithTlsOptions(options => options.WithCertificateValidationHandler(_ => true));

        return builder.Build();
    }

    private static byte[] CopyToByteArray(ReadOnlySequence<byte> payload)
    {
        if (payload.IsSingleSegment)
        {
            return payload.FirstSpan.ToArray();
        }

        if (payload.Length > int.MaxValue)
        {
            throw new InvalidOperationException($"Payload too large to buffer ({payload.Length} bytes).");
        }

        var buffer = new byte[(int)payload.Length];
        payload.CopyTo(buffer);
        return buffer;
    }

    private CertificationData GetCertificationOrThrow()
    {
        return _certification ?? throw new InvalidOperationException("Certification data is not available.");
    }

    private async Task TrySendHeartbeatAsync(CancellationToken cancellationToken)
    {
        if (DateTimeOffset.UtcNow < _nextHeartbeatUtc)
        {
            return;
        }

        try
        {
            if (await SendHeartbeatAsync(cancellationToken))
            {
                _nextHeartbeatUtc = DateTimeOffset.UtcNow.Add(HeartbeatInterval);
            }
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Failed to send EcoFlow MQTT heartbeat.");
            _nextHeartbeatUtc = DateTimeOffset.UtcNow.Add(HeartbeatInterval);
        }
    }

    private async Task<bool> SendHeartbeatAsync(CancellationToken cancellationToken)
    {
        if (!_mqttClient.IsConnected)
        {
            return false;
        }

        var certification = _certification;
        if (certification is null)
        {
            return false;
        }

        if (_devices.Count == 0)
        {
            return false;
        }

        var heartbeatSent = false;
        foreach (var device in _devices)
        {
            var topic = $"/open/{certification.CertificateAccount}/{device.SerialNumber}/set";
            var messageId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var payload = BuildHeartbeatPayload(messageId);

            _logger.LogInformation(
                "Sending EcoFlow MQTT heartbeat set request {MessageId} for device {DeviceSerial} on topic {Topic}: {Payload}",
                messageId,
                device.SerialNumber,
                topic,
                payload);

            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await _mqttClient.PublishAsync(applicationMessage, cancellationToken);
            heartbeatSent = true;
        }

        return heartbeatSent;
    }

    private static string BuildHeartbeatPayload(long messageId)
    {
        var request = new HeartbeatRequest(
            messageId,
            HeartbeatProtocolVersion,
            HeartbeatOperateType,
            HeartbeatSource,
            new HeartbeatParams(HeartbeatCmdSet, HeartbeatCmdId, HeartbeatLcdTimeSeconds));

        return JsonSerializer.Serialize(request, HeartbeatSerializerOptions);
    }

    private void LogHeartbeatResponseIfApplicable(string topic, byte[] payload)
    {
        if (!topic.EndsWith("/set_reply", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var payloadText = payload.Length == 0 ? "<empty>" : Encoding.UTF8.GetString(payload);
        _logger.LogInformation("Received EcoFlow MQTT heartbeat set response on {Topic}: {Payload}", topic, payloadText);
    }

    private sealed record HeartbeatRequest(long Id, string Version, string OperateType, string From, HeartbeatParams Params);

    private sealed record HeartbeatParams(int CmdSet, int Id, int LcdTime);

    private async Task UpdateInventorySnapshotAsync(bool waitForNextUpdate, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var currentVersion = _accountInventory.Version;
            var currentDevices = _accountInventory.Devices;
            var currentCertification = _accountInventory.Certification;
            var versionChanged = currentVersion != _accountInventoryVersion;

            if ((!waitForNextUpdate || versionChanged) &&
                currentCertification is not null &&
                currentDevices.Count > 0)
            {
                _devices = currentDevices;
                _certification = currentCertification;
                _accountInventoryVersion = currentVersion;
                return;
            }

            if (currentCertification is null)
            {
                _logger.LogWarning("Certification data is missing. Waiting for account inventory refresh...");
            }
            else if (currentDevices.Count == 0)
            {
                _logger.LogWarning("EcoFlow account inventory does not contain devices yet. Waiting for account inventory refresh...");
            }
            else if (waitForNextUpdate)
            {
                _logger.LogInformation("Awaiting an updated EcoFlow account inventory snapshot before retrying MQTT connection.");
            }

            await _accountInventory.WaitForUpdateAsync(currentVersion, cancellationToken);
            waitForNextUpdate = true;
        }

        cancellationToken.ThrowIfCancellationRequested();
    }
}
