namespace Ecoflow.MqttIngestor.Configuration;

public sealed class PostgresOptions
{
    public const string SectionName = "Database";

    public string ConnectionString { get; init; } = string.Empty;
    public string? Schema { get; init; } = "public";
    public string TableName { get; init; } = "mqtt_messages";
}
