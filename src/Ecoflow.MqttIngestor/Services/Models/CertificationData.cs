namespace Ecoflow.MqttIngestor.Services.Models;

public sealed record CertificationData
{
    public string CertificateAccount { get; init; } = string.Empty;
    public string CertificatePassword { get; init; } = string.Empty;
    public string Url { get; init; } = string.Empty;
    public string Port { get; init; } = string.Empty;
    public string Protocol { get; init; } = string.Empty;
}
