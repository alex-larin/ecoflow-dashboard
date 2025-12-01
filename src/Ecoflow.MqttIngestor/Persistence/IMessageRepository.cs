namespace Ecoflow.MqttIngestor.Persistence;

public interface IMessageRepository
{
    Task EnsureSchemaAsync(CancellationToken cancellationToken);
    Task StoreAsync(EcoflowEvent ecoflowEvent, CancellationToken cancellationToken);
}
