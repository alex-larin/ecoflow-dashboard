using System.Threading.Channels;
using Ecoflow.MqttIngestor.Configuration;
using Ecoflow.MqttIngestor.Messaging;
using Ecoflow.MqttIngestor.Persistence;
using Ecoflow.MqttIngestor.Processing;
using Ecoflow.MqttIngestor.Services;
using Ecoflow.MqttIngestor.Workers;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Diagnostics;
using MQTTnet.Diagnostics.Logger;
using Npgsql;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration.AddEnvironmentVariables();

builder.Services.Configure<PostgresOptions>(builder.Configuration.GetSection(PostgresOptions.SectionName));
builder.Services.Configure<EcoflowApiOptions>(builder.Configuration.GetSection(EcoflowApiOptions.SectionName));

builder.Services.AddSingleton(sp =>
{
    var options = sp.GetRequiredService<IOptions<PostgresOptions>>().Value;
    if (string.IsNullOrWhiteSpace(options.ConnectionString))
    {
        throw new InvalidOperationException("Database connection string is not configured.");
    }

    var dataSourceBuilder = new NpgsqlDataSourceBuilder(options.ConnectionString);
    return dataSourceBuilder.Build();
});

builder.Services.AddSingleton(sp =>
{
    var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
    var mqttLogger = new MqttNetEventLogger();
    mqttLogger.LogMessagePublished += (_, args) =>
    {
        var log = args.LogMessage;
        if (ShouldSuppressMqttLog(log))
        {
            return;
        }

        var sourceLogger = loggerFactory.CreateLogger($"MQTTnet.{log.Source}");
        var level = MapLogLevel(log.Level);
        sourceLogger.Log(
            level,
            log.Exception,
            "[{Source}] (Thread {ThreadId}) {Message}",
            log.Source,
            log.ThreadId,
            log.Message);
    };

    return mqttLogger;
});
builder.Services.AddSingleton(sp => new MqttClientFactory(sp.GetRequiredService<MqttNetEventLogger>()));
builder.Services.AddSingleton<IMqttClient>(sp => sp.GetRequiredService<MqttClientFactory>().CreateMqttClient());

builder.Services.AddSingleton(Channel.CreateUnbounded<MqttEnvelope>(new UnboundedChannelOptions
{
    SingleReader = true,
    SingleWriter = false
}));
builder.Services.AddSingleton(sp => sp.GetRequiredService<Channel<MqttEnvelope>>().Reader);
builder.Services.AddSingleton(sp => sp.GetRequiredService<Channel<MqttEnvelope>>().Writer);

builder.Services.AddSingleton<IMessageRepository, PostgresMessageRepository>();
builder.Services.AddSingleton<IMessageParser, MessageParser>();
builder.Services.AddHttpClient<EcoflowAccountClient>();
builder.Services.AddSingleton<AccountInventory>();
builder.Services.AddSingleton<IAccountInventory>(sp => sp.GetRequiredService<AccountInventory>());

builder.Services.AddHostedService<AccountBootstrapper>();
builder.Services.AddHostedService<MqttSubscriberWorker>();
builder.Services.AddHostedService<MessageProcessingWorker>();

var host = builder.Build();
host.Run();

static bool ShouldSuppressMqttLog(MqttNetLogMessage log)
{
    return log.Level == MqttNetLogLevel.Verbose && string.Equals(log.Source, "MqttChannelAdapter", StringComparison.OrdinalIgnoreCase);
}

static LogLevel MapLogLevel(MqttNetLogLevel level)
{
    return level switch
    {
        MqttNetLogLevel.Error => LogLevel.Error,
        MqttNetLogLevel.Warning => LogLevel.Warning,
        MqttNetLogLevel.Info => LogLevel.Information,
        MqttNetLogLevel.Verbose => LogLevel.Debug,
        _ => LogLevel.Debug
    };
}
