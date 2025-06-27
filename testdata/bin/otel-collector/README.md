# OpenTelemetry Collector & Jaeger Integration

This directory contains scripts and configuration to run an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) and a [Jaeger](https://www.jaegertracing.io/) instance for collecting and visualizing telemetry, primarily for Impala development and testing.

---

## Contents

- [`start-otel-collector.sh`](./start-otel-collector.sh): Script to start the OpenTelemetry Collector and Jaeger using Docker.
- [`stop-otel-collector.sh`](./stop-otel-collector.sh): Script to stop and remove the containers and network.
- [`otel-config.yml`](./otel-config.yml): OpenTelemetry Collector configuration file.
- [`docker-compose.yml`](./docker-compose.yml): Alternative Docker Compose setup for both services.

---

## Quick Start

### Option 1: Using Provided Shell Scripts

1. **Start the Collector and Jaeger**

   ```bash
   ./start-otel-collector.sh
   ```

   - This script:
     - Stops and removes any existing `otel-collector` and `jaeger` containers and the custom Docker network.
     - Creates a dedicated Docker network (`otel-collector`).
     - Starts the OpenTelemetry Collector container with the provided config.
     - Starts the Jaeger container.
     - Waits for both containers to be running before exiting.

2. **Stop the Collector and Jaeger**

   ```bash
   ./stop-otel-collector.sh
   ```

   - This script gracefully stops and removes the containers and network.

### Option 2: Using Docker Compose

1. **Start the Stack**

   ```bash
   docker compose up -d
   ```

2. **Stop the Stack**

   ```bash
   docker compose down
   ```

---

## Configuration Details

- **OpenTelemetry Collector** listens for OTLP traces on port `55888` (HTTP).
- **Jaeger** is configured to receive OTLP traces on port `4317` and exposes its UI on port `16686`.

The collector forwards all received traces to Jaeger using OTLP/gRPC.

---

## Sending Traces from Impala

To send traces from an Impala cluster to this collector, start Impala with the following arguments:

```bash
./bin/start-impala-cluster.py \
  --cluster_size=2 \
  --num_coordinators=1 \
  --use_exclusive_coordinators \
  --impalad_args="-v=2 --otel_trace_enabled=true \
    --otel_trace_collector_url=http://localhost:55888/v1/traces
    --otel_trace_span_processor=simple \
    --cluster_id=local_cluster"
```

- Ensure the collector is running before starting Impala.
- Adjust `--otel_trace_collector_url` if running on a remote host.

---

## Viewing Traces

- Open the Jaeger UI in your browser: [http://localhost:16686/](http://localhost:16686/)
- If running remotely, use SSH port forwarding:

  ```bash
  ssh -L 16686:localhost:16686 <your-dev-machine>
  ```

---

## Notes

- The scripts and Docker Compose setup are idempotent and safe to run multiple times.
- All containers and the custom network are cleaned up on stop.
- The provided configuration is suitable for local development and testing only.

---

## Troubleshooting

- **Ports already in use:** Ensure no other services are using ports `55888`, `4317`, or `16686`.
- **Containers not starting:** Check Docker logs for `otel-collector` and `jaeger` for errors:

  ```bash
  docker logs otel-collector
  docker logs jaeger
  ```

- **Configuration changes:** Edit `otel-config.yml` as needed and restart the services.

---