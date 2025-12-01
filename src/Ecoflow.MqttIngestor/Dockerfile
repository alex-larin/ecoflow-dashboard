# syntax=docker/dockerfile:1.6

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Ecoflow.Dashboard.sln ./
COPY src/Ecoflow.MqttIngestor/Ecoflow.MqttIngestor.csproj src/Ecoflow.MqttIngestor/
RUN dotnet restore src/Ecoflow.MqttIngestor/Ecoflow.MqttIngestor.csproj

COPY . .
RUN dotnet publish src/Ecoflow.MqttIngestor/Ecoflow.MqttIngestor.csproj \
    --configuration Release \
    --runtime linux-arm64 \
    --output /app/publish \
    /p:PublishReadyToRun=true \
    /p:PublishSingleFile=true \
    /p:SelfContained=true \
    /p:StripSymbols=true

FROM mcr.microsoft.com/dotnet/runtime-deps:10.0 AS runtime
WORKDIR /app

COPY --from=build /app/publish ./
RUN chmod +x Ecoflow.MqttIngestor

USER 1000:1000
ENTRYPOINT ["./Ecoflow.MqttIngestor"]
