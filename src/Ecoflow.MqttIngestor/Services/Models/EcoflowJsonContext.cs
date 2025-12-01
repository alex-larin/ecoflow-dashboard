using System.Text.Json.Serialization;

namespace Ecoflow.MqttIngestor.Services.Models;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, WriteIndented = false)]
[JsonSerializable(typeof(DeviceListResponse))]
[JsonSerializable(typeof(CertificationResponse))]
public partial class EcoflowJsonContext : JsonSerializerContext;
