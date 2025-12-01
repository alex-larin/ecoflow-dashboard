namespace Ecoflow.MqttIngestor.Services.Models;

public sealed record DeviceListResponse
{
    public string Code { get; init; } = string.Empty;
    public string Message { get; init; } = string.Empty;
    public IReadOnlyList<DeviceSummary> Data { get; init; } = Array.Empty<DeviceSummary>();
    public string EagleEyeTraceId { get; init; } = string.Empty;
    public string Tid { get; init; } = string.Empty;
}
