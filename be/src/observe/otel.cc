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

#include "otel.h"

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gutil/strings/split.h>
#include <opentelemetry/exporters/otlp/otlp_http.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_runtime_options.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/batch_span_processor_runtime_options.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer.h>

#include "common/status.h"
#include "common/version.h"
#include "observe/otel-instrument.h"
#include "observe/span-manager.h"
#include "runtime/query-driver.h"
#include "util/openssl-util.h"

using namespace boost::algorithm;
using namespace opentelemetry;
using namespace opentelemetry::exporter::otlp;
using namespace opentelemetry::sdk::trace;
using namespace std;

// OTel related flags
DECLARE_string(otel_additional_headers);
DECLARE_int32(otel_batch_queue_size);
DECLARE_int32(otel_batch_max_batch_size);
DECLARE_int32(otel_batch_schedule_delay_ms);
DECLARE_string(otel_ca_cert_path);
DECLARE_string(otel_ca_cert_string);
DECLARE_string(otel_collector_url);
DECLARE_bool(otel_compression);
DECLARE_bool(otel_debug);
DECLARE_double(otel_retry_policy_backoff_multiplier);
DECLARE_double(otel_retry_policy_initial_backoff_s);
DECLARE_int32(otel_retry_policy_max_attempts);
DECLARE_int32(otel_retry_policy_max_backoff_s);
DECLARE_string(otel_span_processor);
DECLARE_string(otel_ssl_ciphers);
DECLARE_int32(otel_timeout_s);
DECLARE_string(otel_tls_cipher_suites);
DECLARE_bool(otel_tls_insecure_skip_verify);
DECLARE_string(otel_tls_minimum_version);

// Other flags
DECLARE_string(ssl_cipher_list);
DECLARE_string(tls_ciphersuites);
DECLARE_string(ssl_minimum_version);

namespace impala {

static nostd::shared_ptr<trace::Tracer> tracer_;
static bool otel_enabled_ = false;

bool otel_enabled() {
  return otel_enabled_;
} // function otel_enabled

Status init_otel_tracer() {
  if (FLAGS_otel_collector_url.empty()) {
    LOG(INFO) << "OpenTelemetry collector URL is not set, OTel tracing will be disabled.";
    return Status::OK();
  }

  // Configure OTLP HTTP exporter
  OtlpHttpExporterOptions opts;
  opts.url = FLAGS_otel_collector_url;
  opts.content_type = HttpRequestContentType::kJson;
  opts.timeout = chrono::seconds(FLAGS_otel_timeout_s);
  opts.console_debug = FLAGS_otel_debug;

  // Retry settings
  opts.retry_policy_max_attempts = FLAGS_otel_retry_policy_max_attempts;
  opts.retry_policy_initial_backoff =
      chrono::duration<float>(FLAGS_otel_retry_policy_initial_backoff_s);  
  if (FLAGS_otel_retry_policy_max_backoff_s > 0) {
    opts.retry_policy_max_backoff = chrono::duration<float>(
        chrono::seconds(FLAGS_otel_retry_policy_max_backoff_s));
  }
  opts.retry_policy_backoff_multiplier = FLAGS_otel_retry_policy_backoff_multiplier;

  // Compression Type
  if (FLAGS_otel_compression) {
    opts.compression = "zlib";
  }

  // TLS Configurations
  if (IsExternalTlsConfigured()) {
    if (FLAGS_otel_tls_minimum_version.empty()) {
      // Set minimum TLS version to the value of the global ssl_minimum_version flag.
      // Since this flag is in the format "tlv1.2" or "tlsv1.3", we need to
      // convert it to the format expected by OtlpHttpExporterOptions.
      const string min_ssl_ver = to_lower_copy(trim_copy(FLAGS_otel_tls_minimum_version));

      if (!min_ssl_ver.empty() && min_ssl_ver.rfind("tlsv", 0) != 0) {
        return Status("ssl_minimum_version must start with 'tlsv'");
      }

      opts.ssl_min_tls = min_ssl_ver.substr(4); // Remove "tlsv" prefix
    } else {
      opts.ssl_min_tls = FLAGS_otel_tls_minimum_version;
    }

    opts.ssl_insecure_skip_verify = FLAGS_otel_tls_insecure_skip_verify;
    opts.ssl_ca_cert_path = FLAGS_otel_ca_cert_path;
    opts.ssl_ca_cert_string = FLAGS_otel_ca_cert_string;
    opts.ssl_max_tls = "1.3";
    opts.ssl_cipher = FLAGS_otel_ssl_ciphers.empty() ? FLAGS_ssl_cipher_list :
        FLAGS_otel_ssl_ciphers;
    opts.ssl_cipher_suite = FLAGS_otel_tls_cipher_suites.empty() ?
        FLAGS_tls_ciphersuites : FLAGS_otel_tls_cipher_suites; 
  }

  // Additional HTTP headers
  if (!FLAGS_otel_additional_headers.empty()) {
    for (auto header : strings::Split(FLAGS_otel_additional_headers, ":::")) {
      auto pos = header.find('=');
      const string key = trim_copy(header.substr(0, pos).as_string());
      const string value = trim_copy(header.substr(pos + 1).as_string());

      VLOG(2) << "Adding additional OTel header: " << key << " = " << value;
      opts.http_headers.emplace(key, value);
    }
  }

  unique_ptr<SpanExporter> exporter;
  if (FLAGS_otel_debug) {
    opentelemetry::v1::exporter::otlp::OtlpHttpExporterRuntimeOptions runtime_opts;
    runtime_opts.thread_instrumentation =
        make_shared<LoggingInstrumentation>("http_exporter");
    exporter = OtlpHttpExporterFactory::Create(opts, runtime_opts);
  } else {
    exporter = OtlpHttpExporterFactory::Create(opts);
  }

  // Set up tracer provider
  unique_ptr<SpanProcessor> processor;

  if (boost::iequals(trim_copy(FLAGS_otel_span_processor), SPAN_PROCESSOR_BATCH)) {
    VLOG(2) << "Using BatchSpanProcessor for OTel spans";
    BatchSpanProcessorOptions batch_opts;

    batch_opts.max_queue_size = FLAGS_otel_batch_queue_size;
    batch_opts.max_export_batch_size = FLAGS_otel_batch_max_batch_size;
    batch_opts.schedule_delay_millis =
        chrono::milliseconds(FLAGS_otel_batch_schedule_delay_ms);

    if (FLAGS_otel_debug) {
      BatchSpanProcessorRuntimeOptions runtime_opts;
      runtime_opts.thread_instrumentation =
          make_shared<LoggingInstrumentation>("batch_span_processor");
      processor = BatchSpanProcessorFactory::Create(move(exporter), batch_opts,
          runtime_opts);
    } else {
      processor = BatchSpanProcessorFactory::Create(move(exporter), batch_opts);
    }
  } else {
    VLOG(2) << "Using SimpleSpanProcessor for OTel spans";
    processor = make_unique<SimpleSpanProcessor>(move(exporter));
  }

  shared_ptr<TracerProvider> provider = TracerProviderFactory::Create(move(processor),
      sdk::resource::Resource::Create({
        {"service.name", "Impala"},
        {"service.version", GetDaemonBuildVersion()}
      }));

  trace::Provider::SetTracerProvider(nostd::shared_ptr<trace::TracerProvider>(provider));

  // Get tracer
  tracer_ = provider->GetTracer("org.apache.impala.impalad", GetDaemonBuildVersion());

  otel_enabled_ = true;

  return Status::OK();
} // function init_otel_tracer

shared_ptr<SpanManager> build_span_manager(const QueryHandle* query_handle) {
  return make_shared<SpanManager>(tracer_, query_handle);
} // function build_span_manager

} // namespace impala