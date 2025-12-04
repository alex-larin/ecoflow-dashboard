namespace Ecoflow.MqttIngestor.Persistence;

public sealed record EcoflowEvent(
    string DeviceId,
    string Module,
    DateTimeOffset IngestTimestamp,
    string Payload);
