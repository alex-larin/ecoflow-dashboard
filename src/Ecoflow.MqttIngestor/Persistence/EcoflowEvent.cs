namespace Ecoflow.MqttIngestor.Persistence;

public sealed record EcoflowEvent(
    string DeviceId,
    string Module,
    DateTimeOffset DeviceTimestamp,
    DateTimeOffset IngestTimestamp,
    string Payload);
