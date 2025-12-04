using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Ecoflow.MqttIngestor.Messaging;
using Ecoflow.MqttIngestor.Persistence;
using Ecoflow.MqttIngestor.Processing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Ecoflow.MqttIngestor.Tests.Processing;

public sealed class MessageParserTests
{
    private static readonly string SamplePayloadPath = Path.Combine(AppContext.BaseDirectory, "SampleData", "mqtt-open-payload.json");
    private static readonly IReadOnlyList<string> ExpectedModules = new[] { "EMS", "BMS", "PD", "INV", "MPPT", "ALL" };

    [Fact]
    public void TryParse_MessageSamples_AssignsExpectedModules()
    {
        var parser = new MessageParser(NullLogger<MessageParser>.Instance);
        var payloads = LoadPayloads();

        Assert.Equal(ExpectedModules.Count, payloads.Count);

        for (var index = 0; index < payloads.Count; index++)
        {
            var payload = Encoding.UTF8.GetBytes(payloads[index]);
            var envelope = new MqttEnvelope("/open/demo/demo/sn/quota", payload, DateTimeOffset.UnixEpoch);

            var parsed = parser.TryParse(envelope, out EcoflowEvent? ecoflowEvent, out var failureReason);

            Assert.True(parsed, failureReason);
            Assert.NotNull(ecoflowEvent);
            Assert.Equal(ExpectedModules[index], ecoflowEvent!.Module);
        }
    }

    private static IReadOnlyList<string> LoadPayloads()
    {
        var document = JsonDocument.Parse(File.ReadAllText(SamplePayloadPath));
        var payloads = new List<string>();

        foreach (var element in document.RootElement.EnumerateArray())
        {
            payloads.Add(element.GetRawText());
        }

        return payloads;
    }
}
