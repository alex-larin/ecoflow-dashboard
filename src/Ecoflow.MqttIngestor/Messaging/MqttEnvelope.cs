namespace Ecoflow.MqttIngestor.Messaging;

public sealed record MqttEnvelope(string Topic, byte[] Payload, DateTimeOffset ReceivedAt);
