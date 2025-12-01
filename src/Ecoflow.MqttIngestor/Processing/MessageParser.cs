using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Ecoflow.MqttIngestor.Messaging;
using Ecoflow.MqttIngestor.Persistence;
using Microsoft.Extensions.Logging;

namespace Ecoflow.MqttIngestor.Processing;

public sealed class MessageParser(ILogger<MessageParser> logger) : IMessageParser
{
    private static readonly Dictionary<string, string> ModuleMap = new(StringComparer.OrdinalIgnoreCase)
    {
        ["ems"] = "EMS",
        ["bmsMaster"] = "BMS",
        ["kit"] = "KIT",
        ["pd"] = "PD",
        ["inv"] = "INV",
        ["mppt"] = "MPPT"
    };

    public bool TryParse(MqttEnvelope envelope, out EcoflowEvent? ecoflowEvent, out string? failureReason)
    {
        ecoflowEvent = null;
        failureReason = null;

        if (envelope.Payload.Length == 0)
        {
            failureReason = "Payload is empty";
            return false;
        }

        var utf8Reader = new Utf8JsonReader(envelope.Payload);

        string deviceId = ExtractDeviceId(envelope.Topic);
        string? module = null;
        long? deviceTimestamp = null;

        try
        {
            while (utf8Reader.Read())
            {
                if (utf8Reader.TokenType != JsonTokenType.PropertyName)
                {
                    continue;
                }

                var propertyName = utf8Reader.GetString();
                if (propertyName is null)
                {
                    continue;
                }

                if (propertyName.Equals("params", StringComparison.OrdinalIgnoreCase))
                {
                    (module, deviceTimestamp) = ExtractFromParams(ref utf8Reader, module, deviceTimestamp);
                }
                else if (propertyName.Equals("timestamp", StringComparison.OrdinalIgnoreCase) && deviceTimestamp is null)
                {
                    utf8Reader.Read();
                    if (utf8Reader.TokenType == JsonTokenType.Number && utf8Reader.TryGetInt64(out var ts))
                    {
                        deviceTimestamp = ts;
                    }
                }
                else
                {
                    utf8Reader.Skip();
                }
            }
        }
        catch (JsonException exception)
        {
            failureReason = $"Invalid JSON payload: {exception.Message}";
            logger.LogWarning(exception, "Failed to parse MQTT payload for topic {Topic}", envelope.Topic);
            return false;
        }

        if (module is null)
        {
            failureReason = "Unable to determine module";
            return false;
        }

        if (deviceTimestamp is null)
        {
            failureReason = "Missing device timestamp";
            return false;
        }

        var deviceTs = ConvertDeviceTimestamp(deviceTimestamp.Value);
        var payload = Encoding.UTF8.GetString(envelope.Payload);
        ecoflowEvent = new EcoflowEvent(deviceId, module, deviceTs, envelope.ReceivedAt, payload);
        return true;
    }

    private static (string? module, long? deviceTimestamp) ExtractFromParams(
        ref Utf8JsonReader reader,
        string? currentModule,
        long? deviceTimestamp)
    {
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            reader.Skip();
            return (currentModule, deviceTimestamp);
        }

        string? module = currentModule;
        long? timestamp = deviceTimestamp;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                reader.Skip();
                continue;
            }

            var propertyName = reader.GetString();
            if (propertyName is null)
            {
                reader.Skip();
                continue;
            }

            if (module is null && propertyName.Contains('.'))
            {
                var separatorIndex = propertyName.IndexOf('.');
                var moduleCandidate = propertyName[..separatorIndex];
                module = NormalizeModuleName(moduleCandidate);
            }

            reader.Skip();
        }

        return (module, timestamp);
    }

    private static string NormalizeModuleName(string candidate)
    {
        if (ModuleMap.TryGetValue(candidate, out var mapped))
        {
            return mapped;
        }

        return candidate.ToUpperInvariant();
    }

    private static DateTimeOffset ConvertDeviceTimestamp(long timestamp)
    {
        try
        {
            return timestamp > 1_000_000_000_000
                ? DateTimeOffset.FromUnixTimeMilliseconds(timestamp)
                : DateTimeOffset.FromUnixTimeSeconds(timestamp);
        }
        catch (ArgumentOutOfRangeException)
        {
            return DateTimeOffset.UtcNow;
        }
    }

    private static string ExtractDeviceId(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            return "unknown";
        }

        var segments = topic.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return segments.Length switch
        {
            0 => topic,
            1 => segments[0],
            _ => segments[^2]
        };
    }
}
