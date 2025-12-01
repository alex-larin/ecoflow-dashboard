namespace Ecoflow.MqttIngestor.Configuration;

public sealed class EcoflowApiOptions
{
    public const string SectionName = "EcoflowApi";

    public string AccessKey { get; init; } = string.Empty;
    public string SecretKey { get; init; } = string.Empty;
    public string RestHost { get; init; } = "https://api-e.ecoflow.com";
}
