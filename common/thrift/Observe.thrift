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

namespace py impala_thrift_gen.Observe
namespace cpp impala
namespace java org.apache.impala.thrift

// Identifying information for a single OpenTelemetry trace.
struct TOtelTrace {
  // OpenTelemetry assigned trace id.
  1: required string trace_id

  // OpenTelemetry assigned id of the current active span (tcan be used as the parent span
  // of new child spans).
  2: required string active_span_id

  // Time the trace started (in Unix Nanosecond format).
  3: required i64 trace_start_time
}

struct TOtelConfigs {
  1: required bool otel_debug

  2: required string otel_file_pattern

  3: required string otel_file_alias_pattern

  4: required i32 otel_file_flush_interval_us

  5: required i32 otel_file_flush_count

  6: required i32 otel_file_max_file_size

  7: required i32 otel_file_max_file_count
}

enum TOtelExporterType {
  OTLP_HTTP,
  FILE
}

enum TOtelSpanProcessorType {
  BATCH,
  SIMPLE
}

// Configuration parameters for OpenTelemetry tracing.
struct TOtelTraceConfigs {
  1: required TOtelConfigs base_configs

  2: required bool enabled

  3: required TOtelExporterType exporter

  4: required string collector_url

  5: required map<string, string> additional_headers

  6: required bool compression

  7: required i32 timeout_s

  8: required string ca_cert_path

  9: required string ca_cert_string

  10: required string tls_minimum_version

  11: required string ssl_ciphers

  12: required string tls_cipher_suites

  13: required bool tls_insecure_skip_verify

  14: required i32 retry_policy_max_attempts

  15: required double retry_policy_initial_backoff_s

  16: required i32 retry_policy_max_backoff_s

  17: required double retry_policy_backoff_multiplier

  18: required TOtelSpanProcessorType span_processor

  19: required i32 batch_queue_size

  20: required i32 batch_schedule_delay_ms

  21: required i32 batch_max_batch_size

  22: required bool tls_enabled

}
