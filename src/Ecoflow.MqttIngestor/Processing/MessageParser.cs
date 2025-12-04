using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Ecoflow.MqttIngestor.Messaging;
using Ecoflow.MqttIngestor.Persistence;

namespace Ecoflow.MqttIngestor.Processing;

public sealed class MessageParser(ILogger<MessageParser> logger) : IMessageParser
{
    // Identifier used when payload combines data from several modules.
    private const string AggregatedModuleName = "ALL";
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
        bool rootHasOnlyParams = true;
        string? firstParamsProperty = null;
        bool paramsEncountered = false;

        try
        {
            while (utf8Reader.Read())
            {
                if (utf8Reader.TokenType != JsonTokenType.PropertyName)
                {
                    continue;
                }

                var propertyName = utf8Reader.GetString();
                bool isRootProperty = utf8Reader.CurrentDepth == 1;
                bool isParams = propertyName?.Equals("params", StringComparison.OrdinalIgnoreCase) == true;

                if (isRootProperty && !isParams)
                {
                    rootHasOnlyParams = false;
                }

                if (isParams)
                {
                    if (!paramsEncountered)
                    {
                        firstParamsProperty ??= ExtractFirstParamsPropertyName(ref utf8Reader);
                        paramsEncountered = true;
                    }
                    else
                    {
                        utf8Reader.Skip();
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

        if (!paramsEncountered)
        {
            failureReason = "Unable to determine module";
            return false;
        }

        module = rootHasOnlyParams
            ? AggregatedModuleName
            : TryResolveModuleName(firstParamsProperty);

        if (module is null)
        {
            failureReason = "Unable to determine module";
            return false;
        }

        var payload = Encoding.UTF8.GetString(envelope.Payload);
        ecoflowEvent = new EcoflowEvent(deviceId, module, envelope.ReceivedAt, payload);
        return true;
    }

    private static string? ExtractFirstParamsPropertyName(ref Utf8JsonReader reader)
    {
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            reader.Skip();
            return null;
        }

        string? firstProperty = null;

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

            firstProperty ??= reader.GetString();
            reader.Skip();
        }

        return firstProperty;
    }

    private static string? TryResolveModuleName(string? propertyName)
    {
        if (string.IsNullOrWhiteSpace(propertyName))
        {
            return null;
        }

        var separatorIndex = propertyName.IndexOf('.');
        if (separatorIndex <= 0)
        {
            return null;
        }

        var candidate = propertyName[..separatorIndex];
        return NormalizeModuleName(candidate);
    }

    private static string NormalizeModuleName(string candidate)
    {
        if (ModuleMap.TryGetValue(candidate, out var mapped))
        {
            return mapped;
        }

        return candidate.ToUpperInvariant();
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
