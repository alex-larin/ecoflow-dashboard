using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Ecoflow.MqttIngestor.Configuration;
using Ecoflow.MqttIngestor.Services.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ecoflow.MqttIngestor.Services;

public sealed class EcoflowAccountClient
{
    private readonly HttpClient _httpClient;
    private readonly IOptions<EcoflowApiOptions> _options;
    private readonly ILogger<EcoflowAccountClient> _logger;

    public EcoflowAccountClient(
        HttpClient httpClient,
        IOptions<EcoflowApiOptions> options,
        ILogger<EcoflowAccountClient> logger)
    {
        _httpClient = httpClient;
        _options = options;
        _logger = logger;
    }

    public async Task<IReadOnlyList<EcoflowDevice>> GetDevicesAsync(CancellationToken cancellationToken)
    {
        var payload = await SendSignedRequestAsync("/iot-open/sign/device/list", cancellationToken);

        var deviceResponse = JsonSerializer.Deserialize(payload, EcoflowJsonContext.Default.DeviceListResponse);
        if (deviceResponse?.Data is null || deviceResponse.Data.Count == 0)
        {
            _logger.LogWarning("EcoFlow API returned an empty device list.");
            return Array.Empty<EcoflowDevice>();
        }

        var devices = deviceResponse.Data
            .Where(d => !string.IsNullOrWhiteSpace(d.Sn))
            .Select(d => new EcoflowDevice(d.Sn!, d.DeviceName))
            .ToArray();

        return devices.Length == 0 ? Array.Empty<EcoflowDevice>() : devices;
    }

    public async Task<CertificationData> GetCertificationAsync(CancellationToken cancellationToken)
    {
        var payload = await SendSignedRequestAsync("/iot-open/sign/certification", cancellationToken);
        var certificationResponse = JsonSerializer.Deserialize(payload, EcoflowJsonContext.Default.CertificationResponse);
        if (certificationResponse?.Data is null)
        {
            throw new InvalidOperationException("EcoFlow certification response is empty.");
        }

        return certificationResponse.Data;
    }

    private static string ComputeHmacSha256(string data, string secretKey)
    {
        var keyBytes = Encoding.UTF8.GetBytes(secretKey);
        var dataBytes = Encoding.UTF8.GetBytes(data);

        using var hmac = new HMACSHA256(keyBytes);
        var hashBytes = hmac.ComputeHash(dataBytes);
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }

    private async Task<string> SendSignedRequestAsync(string relativePath, CancellationToken cancellationToken)
    {
        EnsureConfiguration(out var restHost, out var accessKey, out var secretKey);

        var nonce = RandomNumberGenerator.GetInt32(100_000, 999_999);
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var signaturePayload = $"accessKey={accessKey}&nonce={nonce}&timestamp={timestamp}";
        var signature = ComputeHmacSha256(signaturePayload, secretKey);

        if (!Uri.TryCreate(restHost, UriKind.Absolute, out var baseUri))
        {
            throw new InvalidOperationException($"EcoFlow REST host '{restHost}' is not a valid absolute URI.");
        }

        var requestUri = new Uri(baseUri, relativePath);

        using var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
        request.Headers.Add("accessKey", accessKey);
        request.Headers.Add("timestamp", timestamp.ToString(CultureInfo.InvariantCulture));
        request.Headers.Add("nonce", nonce.ToString(CultureInfo.InvariantCulture));
        request.Headers.Add("sign", signature);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        var payload = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogWarning("EcoFlow API responded with status {StatusCode}: {Payload}", (int)response.StatusCode, payload);
            throw new InvalidOperationException($"EcoFlow API responded with {(int)response.StatusCode}");
        }

        return payload;
    }

    private void EnsureConfiguration(out string restHost, out string accessKey, out string secretKey)
    {
        accessKey = _options.Value.AccessKey;
        secretKey = _options.Value.SecretKey;
        restHost = _options.Value.RestHost;

        if (string.IsNullOrWhiteSpace(accessKey) || string.IsNullOrWhiteSpace(secretKey))
        {
            throw new InvalidOperationException("EcoFlow API credentials are not configured.");
        }

        if (string.IsNullOrWhiteSpace(restHost))
        {
            throw new InvalidOperationException("EcoFlow REST host is not configured.");
        }
    }
}
