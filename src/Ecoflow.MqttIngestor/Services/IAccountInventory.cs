using Ecoflow.MqttIngestor.Services.Models;

namespace Ecoflow.MqttIngestor.Services;

public interface IAccountInventory
{
    IReadOnlyList<EcoflowDevice> Devices { get; }
    CertificationData? Certification { get; }
    ValueTask WaitUntilReadyAsync(CancellationToken cancellationToken);
    int Version { get; }
    ValueTask WaitForUpdateAsync(int knownVersion, CancellationToken cancellationToken);
}
