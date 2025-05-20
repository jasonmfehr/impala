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

#include <regex>
#include <string>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gutil/strings/split.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>

#include "observe/otel.h"

using namespace std;

static opentelemetry::sdk::trace::BatchSpanProcessorOptions batch_opts;

static const auto positive_validator = [](const char* flagname, int32_t value) {
  if (value <= 0) {
    LOG(ERROR) << "Flag '" << flagname << "' must be greater than 0.";
    return false;
  }
  return true;
};

DEFINE_string(otel_collector_url, "", "The URL of the OpenTelemetry collector to which "
    "observability data will be exported.");
DEFINE_validator(otel_collector_url, [](const char* flagname, const string& value) {
  if (value.empty()) {
    return true;
  }

  // Check if URL starts with http:// or https://
  if (!(value.rfind("http://", 0) == 0 || value.rfind("https://", 0) == 0)) {
    LOG(ERROR) << "Flag '" << flagname << "' must start with 'http://' or 'https://'";
    return false;
  }

  return true;
});

DEFINE_int32(otel_timeout_s, 10, "Export timeout in seconds.");
DEFINE_validator(otel_timeout_s, positive_validator);

DEFINE_bool(otel_debug, false, "If set to true, outputs additional debug info");

DEFINE_bool(otel_tls_insecure_skip_verify, false, "If set to true, skips verification of "
    "collector’s TLS certificate.");

// Helper function to validate a PEM bundle in a string.
static bool ValidatePemBundle(const char* flagname, const string& value) {
  if (!value.empty()) {
    BIO* bio = BIO_new_mem_buf(value.data(), value.size());
    if (!bio) {
      LOG(ERROR) << "Flag '" << flagname << "' failed to create BIO for PEM validation.";
      return false;
    }
    bool found_cert = false;
    while (true) {
      X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
      if (!cert) break;
      found_cert = true;
      X509_free(cert);
    }
    BIO_free(bio);
    if (!found_cert) {
      LOG(ERROR) << "Flag '" << flagname << "' must contain at least one valid PEM "
          "certificate.";
      return false;
    }
  }
  return true;
}

DEFINE_string(otel_ca_cert_path, "", "Path to a file containing CA certificates "
  "bundle. Combined with 'otel_ca_cert_string' if both are specified. ");
DEFINE_validator(otel_ca_cert_path, [](const char* flagname, const string& value) {
  if (!value.empty()) {
    FILE* file = fopen(value.c_str(), "r");
    if (!file) {
      LOG(ERROR) << "Flag '" << flagname << "' must point to a valid file: " << value;
      return false;
    }
    // Read file contents into a string
    fseek(file, 0, SEEK_END);
    long filesize = ftell(file);
    if (filesize < 0) {
      fclose(file);
      LOG(ERROR) << "Flag '" << flagname << "' failed to determine file size: " << value;
      return false;
    }
    rewind(file);
    string pem_content;
    pem_content.resize(filesize);
    size_t read_bytes = fread(&pem_content[0], 1, filesize, file);
    fclose(file);
    if (read_bytes != static_cast<size_t>(filesize)) {
      LOG(ERROR) << "Flag '" << flagname << "' failed to read file: " << value;
      return false;
    }
    // Validate the PEM bundle
    if (!ValidatePemBundle(flagname, pem_content)) {
      return false;
    }
  }
  return true;
});

DEFINE_string(otel_ca_cert_string, "", "String containing CA certificates bundle. "
  "Combined with 'otel_ca_cert_path' if both are specified.");
DEFINE_validator(otel_ca_cert_string, [](const char* flagname, const string& value) {
  return ValidatePemBundle(flagname, value);
});

DEFINE_string(otel_tls_minimum_version, "", "String containing the minimum allowed TLS "
  "version, if not specified, defaults to Impala’s overall minimum TLS version.");
DEFINE_validator(otel_tls_minimum_version, [](const char* flagname, const string& value) {
  if (value.empty() || value == "1.2" || value == "1.3") {
    return true;
  }
  LOG(ERROR) << "Flag '" << flagname << "' must be empty or one of: '1.2', '1.3'.";
  return false;
});

DEFINE_string(otel_ssl_ciphers, "", "List of allowed TLS cipher suites when using TLS "
    "1.2, default to the value of Impala’s ssl_cipher_list startup flag.");

DEFINE_string(otel_tls_cipher_suites, "", "List of allowed TLS cipher suites when using "
    "TLS 1.3, default to the value of Impala’s tls_ciphersuites startup flag.");

DEFINE_int32(otel_retry_policy_max_attempts, 5, "Maximum number of call attempts, "
    "including the original attempt.");
DEFINE_validator(otel_retry_policy_max_attempts, [](const char* flagname, int32_t value) {
  if (value < 0) {
    LOG(ERROR) << "Flag '" << flagname << "' must be greater than or equal to 0.";
    return false;
  }
  return true;
});

DEFINE_double(otel_retry_policy_initial_backoff_s, 1.0, "Initial backoff delay between "
  "retry attempts in seconds.");
DEFINE_validator(otel_retry_policy_initial_backoff_s, [](const char* flagname,
    double value) {
  if (value < 1.0) {
    LOG(ERROR) << "Flag '" << flagname << "' must be greater than or equal to 1.0.";
    return false;
  }
  return true;
});

DEFINE_int32(otel_retry_policy_max_backoff_s, 0, "Maximum backoff delay between retry "
    "attempts in seconds. Value of 0 or less indicates not set.");

DEFINE_double(otel_retry_policy_backoff_multiplier, 2.0, "Backoff will be multiplied by "
  "this value after each retry attempt.");
DEFINE_validator(otel_retry_policy_backoff_multiplier, [](const char* flagname,
    double value) {
  if (value < 1.0) {
  LOG(ERROR) << "Flag '" << flagname << "' must be at least 1.0.";
  return false;
  }
  return true;
});

DEFINE_string(otel_additional_headers, "", "List of additional HTTP headers to be sent "
    "with each call to the OTel Collector. Individual headers are separated by a "
    "delimeter of three colons. Format is 'key1=value1:::key2=value2:::key3=value3'.");
DEFINE_validator(otel_additional_headers, [](const char* flagname, const string& value) {
  bool valid = true;

  if (!value.empty()) {
    for (auto header : strings::Split(value, ":::")) {
      if (header.find('=') == string::npos) {
        LOG(ERROR) << "Flag '" << flagname << "' contains an invalid header (missing '='): "
            << header;
        valid = false;
      }
    }
  }

  return valid;
});

DEFINE_bool(otel_compression, true, "If set to true, uses ZLib compression for sending "
    "data to the OTel Collector. If set to false, sends data uncompressed.");

static const string SPAN_PROCESSOR_HELP = "The span processor implementation to use for "
    "exporting spans to the OTel Collector. Supported values: '" + SPAN_PROCESSOR_BATCH +
    "' and '" + SPAN_PROCESSOR_SIMPLE + "'.";
DEFINE_string(otel_span_processor, SPAN_PROCESSOR_BATCH.c_str(),
    SPAN_PROCESSOR_HELP.c_str());
DEFINE_validator(otel_span_processor, [](const char* flagname, const string& value) {
  const std::string trimmed = boost::algorithm::trim_copy(value);
  return boost::iequals(trimmed, SPAN_PROCESSOR_BATCH)
      || boost::iequals(trimmed, SPAN_PROCESSOR_SIMPLE);
});

DEFINE_int32(otel_batch_queue_size, batch_opts.max_queue_size, "The maximum buffer/queue "
    "size. After the size is reached, spans are dropped. Applicable when "
    "'otel_span_processor' is 'batch'.");
DEFINE_validator(otel_batch_queue_size, positive_validator);

DEFINE_int32(otel_batch_schedule_delay_ms, batch_opts.schedule_delay_millis.count(),
    "The delay interval in milliseconds between two consecutive batch exports. "
    "Applicable when 'otel_span_processor' is 'batch'.");
DEFINE_validator(otel_batch_schedule_delay_ms, positive_validator);

DEFINE_int32(otel_batch_max_batch_size, batch_opts.max_export_batch_size, "The maximum "
  "batch size of every export to OTel Collector. Applicable when 'otel_span_processor' "
  "is 'batch'.");
DEFINE_validator(otel_batch_max_batch_size, positive_validator);
