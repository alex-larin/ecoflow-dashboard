# Ecoflow Dashboard

Container-friendly MQTT ingestor that hydrates EcoFlow device telemetry into PostgreSQL.

## Build AOT/ReadyToRun image

```bash
# Default build (Linux/arm64, works for Apple Silicon + Raspberry Pi 4/5)
docker build -f src/Ecoflow.MqttIngestor/Dockerfile -t ecoflow-ingestor:latest .
```

The Dockerfile publishes a self-contained, single-file binary with ReadyToRun enabled, so the image is compact and cold-start friendly. Because the target runtime is Linux/arm64 by default, the same image runs on macOS (Apple Silicon) and Raspberry Pi 4/5. Use Buildx only if you need an additional AMD64 variant.

# Start only Postgres for local debugging

docker compose up -d postgres

````

Volumes ensure database files survive container restarts. By default the compose file binds `./postgres-data` in the repo root to `/var/lib/postgresql/data` inside the container, so you can inspect or back up the files directly. When targeting Raspberry Pi 4/5 you can keep that default or change it to any host path:

```yaml
volumes:
	- /home/pi/ecoflow-db:/var/lib/postgresql/data
````

## Debugging tips

- During local development, set `DOTNET_ENVIRONMENT=Development` and point `Database__ConnectionString` to `Host=host.docker.internal` if the worker runs on your machine while Postgres stays in the container.
- Use `docker compose logs -f mqtt-ingestor` to tail worker logs, or `docker compose exec mqtt-ingestor /bin/sh` for an interactive shell.
- Snapshot the database before upgrading with `docker compose exec postgres pg_dump -U ingestor ecoflow > backup.sql`; restore with `psql` against the same volume.

## Cleanup

```bash
docker compose down          # stop containers
docker compose down -v       # remove containers and the postgres-data volume
```
