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

#include <unordered_map>
#include <string>

#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/nostd/variant.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_metadata.h>
#include <opentelemetry/trace/tracer.h>

namespace impala {

// Forward declaration to break cyclical imports.
class ClientRequestState;

typedef std::unordered_map<opentelemetry::nostd::string_view,
    opentelemetry::common::AttributeValue> OtelAttributesMap;

// Proxy class for an OpenTelemetry Span that automatically adds attributes for start
// time, end time, and total span duration.
class TimedSpan {
public:
  // Initializes and starts a new span with the given name and attribute names.
  // Parameters:
  //   tracer -- The OpenTelemetry tracer to use to create the span. Not stored, only used
  //             during construction.
  //   name -- The name of the span.
  //   start_time_attribute_name -- The name of the attribute that contains the span start
  //                                time.
  //   duration_attribute_name -- The name of the attribute that contains the span
  //                              duration.
  //   attributes -- A map of attributes to set on the span at creation time.
  //   span_kind -- The kind of span. Default is INTERNAL.
  TimedSpan(opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer,
      const std::string name,
      const std::string start_time_attribute_name,
      const std::string duration_attribute_name,
      OtelAttributesMap attributes,
      opentelemetry::trace::SpanKind span_kind =
      opentelemetry::trace::SpanKind::kInternal);

  // Ends the span and sets the "EndTime", and duration attributes.
  void End(const ClientRequestState* client_request_state_);

  // Set any attribute on the underlying span.
  void SetAttribute(opentelemetry::nostd::string_view key,
      const opentelemetry::common::AttributeValue& value) noexcept;

  // Adds an event with the given name to the underlying span.
  void AddEvent(opentelemetry::nostd::string_view name,
      OtelAttributesMap additional_attributes = {}) noexcept;

  // Provides a scope that sets this span as the currently active span.
  opentelemetry::trace::Scope SetActive();

  // Converts the span id of the underlying span to a string.
  std::string SpanId() const;

  // Converts the trace id of the underlying span to a string.
  std::string TraceId() const;

private:
  const std::string start_time_attribute_name_;
  const std::string duration_attribute_name_;
  const long long start_time_;
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;

  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;

}; // class TimedSpan

} // namespace impala
