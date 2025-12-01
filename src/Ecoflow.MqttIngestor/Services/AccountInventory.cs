using Ecoflow.MqttIngestor.Services.Models;

namespace Ecoflow.MqttIngestor.Services;

public sealed class AccountInventory : IAccountInventory
{
    private readonly TaskCompletionSource<bool> _readyTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly object _syncRoot = new();
    private TaskCompletionSource<bool> _updateSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private IReadOnlyList<EcoflowDevice> _devices = Array.Empty<EcoflowDevice>();
    private CertificationData? _certification;
    private int _version;

    public IReadOnlyList<EcoflowDevice> Devices => _devices;
    public CertificationData? Certification => _certification;
    public int Version => _version;

    public ValueTask WaitUntilReadyAsync(CancellationToken cancellationToken)
    {
        if (_readyTcs.Task.IsCompletedSuccessfully)
        {
            return ValueTask.CompletedTask;
        }

        return new(_readyTcs.Task.WaitAsync(cancellationToken));
    }

    public ValueTask WaitForUpdateAsync(int knownVersion, CancellationToken cancellationToken)
    {
        if (_version != knownVersion)
        {
            return ValueTask.CompletedTask;
        }

        Task waitTask;
        lock (_syncRoot)
        {
            if (_version != knownVersion)
            {
                return ValueTask.CompletedTask;
            }

            waitTask = _updateSignal.Task;
        }

        return new(waitTask.WaitAsync(cancellationToken));
    }

    public void Update(IReadOnlyList<EcoflowDevice> devices, CertificationData certification)
    {
        ArgumentNullException.ThrowIfNull(devices);
        ArgumentNullException.ThrowIfNull(certification);

        if (devices.Count == 0)
        {
            throw new InvalidOperationException("EcoFlow device list is empty.");
        }

        TaskCompletionSource<bool> previousSignal;

        lock (_syncRoot)
        {
            _devices = devices;
            _certification = certification;
            _version++;

            previousSignal = _updateSignal;
            _updateSignal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        previousSignal.TrySetResult(true);
        _readyTcs.TrySetResult(true);
    }
}
