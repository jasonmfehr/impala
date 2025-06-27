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

if docker ps -a --format '{{.Names}}' | grep -q '^otel-collector$'; then
  echo "Removing otel-collector container"
  docker stop otel-collector
  docker rm otel-collector
else
  echo "otel-collector container does not exist, skipping removal."
fi

if docker ps -a --format '{{.Names}}' | grep -q '^jaeger$'; then
  echo "Removing jaeger container"
  docker stop jaeger
  docker rm jaeger
else
  echo "jaeger container does not exist, skipping removal."
fi

if docker network ls --format '{{.Name}}' | grep -q '^otel-collector$'; then
  echo "Removing otel-collector network"
  docker network rm otel-collector
else
  echo "otel-collector network does not exist, skipping removal."
fi
