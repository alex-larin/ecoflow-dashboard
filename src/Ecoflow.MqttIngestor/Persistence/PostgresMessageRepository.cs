using Ecoflow.MqttIngestor.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace Ecoflow.MqttIngestor.Persistence;

public sealed class PostgresMessageRepository : IMessageRepository
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgresOptions _options;
    private readonly ILogger<PostgresMessageRepository> _logger;

    private string QualifiedTableName => string.IsNullOrWhiteSpace(_options.Schema)
        ? $"\"{_options.TableName}\""
        : $"\"{_options.Schema}\".\"{_options.TableName}\"";

    public PostgresMessageRepository(
        NpgsqlDataSource dataSource,
        IOptions<PostgresOptions> options,
        ILogger<PostgresMessageRepository> logger)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _logger = logger;
    }

    public async Task EnsureSchemaAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);

        if (!string.IsNullOrWhiteSpace(_options.Schema))
        {
            var schemaCommand = $"CREATE SCHEMA IF NOT EXISTS \"{_options.Schema}\";";
            await using var schemaCmd = new NpgsqlCommand(schemaCommand, connection);
            await schemaCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var tableCommand = $@"
            CREATE TABLE IF NOT EXISTS {QualifiedTableName} (
                id BIGSERIAL PRIMARY KEY,
                device_id TEXT NOT NULL,
                module TEXT NOT NULL,
                device_ts TIMESTAMPTZ NOT NULL,
                ingest_ts TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL
            );
        ";

        await using (var tableCmd = new NpgsqlCommand(tableCommand, connection))
        {
            await tableCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var indexDeviceTs = $"CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_device_ts ON {QualifiedTableName} (device_id, device_ts);";
        await using (var indexCmd = new NpgsqlCommand(indexDeviceTs, connection))
        {
            await indexCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var indexModule = $"CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_module_device_ts ON {QualifiedTableName} (device_id, module, device_ts);";
        await using (var moduleIdxCmd = new NpgsqlCommand(indexModule, connection))
        {
            await moduleIdxCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async Task StoreAsync(EcoflowEvent ecoflowEvent, CancellationToken cancellationToken)
    {
        var sql = $@"
            INSERT INTO {QualifiedTableName} (device_id, module, device_ts, ingest_ts, payload)
            VALUES (@device_id, @module, @device_ts, @ingest_ts, @payload);
        ";

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add("device_id", NpgsqlDbType.Text).Value = ecoflowEvent.DeviceId;
        command.Parameters.Add("module", NpgsqlDbType.Text).Value = ecoflowEvent.Module;
        command.Parameters.Add("device_ts", NpgsqlDbType.TimestampTz).Value = ecoflowEvent.DeviceTimestamp.UtcDateTime;
        command.Parameters.Add("ingest_ts", NpgsqlDbType.TimestampTz).Value = ecoflowEvent.IngestTimestamp.UtcDateTime;
        command.Parameters.Add("payload", NpgsqlDbType.Jsonb).Value = ecoflowEvent.Payload;

        var rows = await command.ExecuteNonQueryAsync(cancellationToken);
        if (rows != 1)
        {
            _logger.LogWarning("Unexpected row count {RowCount} when inserting MQTT message", rows);
        }
    }
}
