using System.Threading.Channels;
using Ecoflow.MqttIngestor.Configuration;
using Ecoflow.MqttIngestor.Messaging;
using Ecoflow.MqttIngestor.Persistence;
using Ecoflow.MqttIngestor.Processing;
using Ecoflow.MqttIngestor.Services;
using Ecoflow.MqttIngestor.Workers;
using Microsoft.Extensions.Options;
using MQTTnet;
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

builder.Services.AddSingleton<MqttClientFactory>();
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
