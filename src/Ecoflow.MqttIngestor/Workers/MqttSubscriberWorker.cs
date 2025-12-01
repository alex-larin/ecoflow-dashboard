using System.Buffers;
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

    private readonly IMqttClient _mqttClient;
    private readonly MqttClientFactory _mqttFactory;
    private readonly ChannelWriter<MqttEnvelope> _channelWriter;
    private readonly IAccountInventory _accountInventory;
    private readonly ILogger<MqttSubscriberWorker> _logger;
    private CancellationToken _executionToken;
    private IReadOnlyList<EcoflowDevice> _devices = Array.Empty<EcoflowDevice>();
    private CertificationData? _certification;
    private int _accountInventoryVersion;

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
                var statusTopic = $"/open/{certification.CertificateAccount}/{device.SerialNumber}/status";

                subscribeBuilder.WithTopicFilter(filter => filter
                    .WithTopic(quotaTopic)
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce));
                subscribeBuilder.WithTopicFilter(filter => filter
                    .WithTopic(statusTopic)
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
            .WithCleanSession()
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(20))
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
