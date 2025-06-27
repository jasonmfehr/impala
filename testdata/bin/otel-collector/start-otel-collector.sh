#!/bin/bash

##############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
##############################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/stop-otel-collector.sh"

docker network create \
    --driver bridge \
    --subnet 172.18.0.0/16 \
    --gateway 172.18.0.1 \
    --opt com.docker.network.enable_ipv4=true \
    --opt com.docker.network.enable_ipv6=false \
    otel-collector

echo "Starting otel-collector container"
docker run -d \
    --name otel-collector \
    --network otel-collector \
    -v "${SCRIPT_DIR}/otel-config.yml:/etc/otel/config.yml" \
    -p 55888:55888 \
    otel/opentelemetry-collector:latest \
    --config /etc/otel/config.yml

# Wait until the otel-collector container is running
until [ "$(
    docker inspect \
      -f '{{.State.Running}}' \
      otel-collector 2>/dev/null
    )" == "true" ]; do
  echo "Waiting for otel-collector container to be running..."
  sleep 1
done
echo "otel-collector is running."
echo

# Ports list:
# 16686: Jaeger UI
# 5778: Jaeger agent
# 4317: Ingestion
echo "Starting Jaeger container"
docker run -d \
    --name jaeger \
    --network otel-collector \
    -p 16686:16686 \
    -p 5778:5778 \
    -p 4317:4317 \
    -e COLLECTOR_OTLP_ENABLED=true \
    -e COLLECTOR_OTLP_HTTP_ENABLED=true \
    jaegertracing/jaeger:2.5.0

# Wait until the jaeger container is running
until [ "$(docker inspect -f '{{.State.Running}}' jaeger 2>/dev/null)" == "true" ]; do
  echo "Waiting for jaeger container to be running..."
  sleep 1
done
echo "jaeger is running."
