namespace Ecoflow.MqttIngestor.Services.Models;

public sealed record DeviceSummary
{
    public string Sn { get; init; } = string.Empty;
    public string DeviceName { get; init; } = string.Empty;
    public int Online { get; init; }
    public string ProductName { get; init; } = string.Empty;
}
