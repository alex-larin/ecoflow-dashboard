using Ecoflow.MqttIngestor.Messaging;
using Ecoflow.MqttIngestor.Persistence;

namespace Ecoflow.MqttIngestor.Processing;

public interface IMessageParser
{
    bool TryParse(MqttEnvelope envelope, out EcoflowEvent? ecoflowEvent, out string? failureReason);
}
