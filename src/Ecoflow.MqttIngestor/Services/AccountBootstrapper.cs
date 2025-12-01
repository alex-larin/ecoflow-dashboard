using Ecoflow.MqttIngestor.Services.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Ecoflow.MqttIngestor.Services;

public sealed class AccountBootstrapper : BackgroundService
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMinutes(1);

    private readonly EcoflowAccountClient _accountClient;
    private readonly AccountInventory _accountInventory;
    private readonly ILogger<AccountBootstrapper> _logger;

    public AccountBootstrapper(
        EcoflowAccountClient accountClient,
        AccountInventory accountInventory,
        ILogger<AccountBootstrapper> logger)
    {
        _accountClient = accountClient;
        _accountInventory = accountInventory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var devices = await _accountClient.GetDevicesAsync(stoppingToken);
                if (devices.Count == 0)
                {
                    _logger.LogWarning("EcoFlow API returned zero devices. Retrying in {DelayMinutes} minute(s)...", RetryDelay.TotalMinutes);
                    throw new InvalidOperationException("Device list is empty");
                }

                var certification = await _accountClient.GetCertificationAsync(stoppingToken);

                _accountInventory.Update(devices, certification);

                _logger.LogInformation(
                    "Loaded {DeviceCount} device(s) and MQTT credentials for account {Account}",
                    devices.Count,
                    certification.CertificateAccount);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Failed to fetch EcoFlow device list. Retrying in {DelayMinutes} minute(s)...", RetryDelay.TotalMinutes);
            }

            try
            {
                await Task.Delay(RetryDelay, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
