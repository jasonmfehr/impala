// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>

#include "common/status.h"
#include "observe/span-manager.h"
#include "runtime/query-driver.h"

// Strings representing the supported OpenTelemetry span processor implementations.
const std::string SPAN_PROCESSOR_SIMPLE = "simple";
const std::string SPAN_PROCESSOR_BATCH = "batch";

namespace impala {

// Initializes the OpenTelemetry tracer with the configuration defined in the coordinator
// startup flags (see otel-flags.cc for the list).
Status init_otel_tracer();

// Returns true if OpenTelemetry is enabled, false otherwise. Uses the
// 'otel_collector_url' startup flag to determine if OpenTelemetry is enabled.
bool otel_enabled();

std::shared_ptr<SpanManager> build_span_manager(const QueryHandle* query_handle);

} // namespace impala
