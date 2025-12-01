namespace Ecoflow.MqttIngestor.Services.Models;

public sealed record CertificationResponse
{
    public string Code { get; init; } = string.Empty;
    public string Message { get; init; } = string.Empty;
    public CertificationData Data { get; init; } = new();
    public string EagleEyeTraceId { get; init; } = string.Empty;
    public string Tid { get; init; } = string.Empty;
}
