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
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <opentelemetry/nostd/shared_ptr.h>

#include "observe/timed-span.h"

namespace impala {

// Forward declaration to break cyclical imports.
class ClientRequestState;

// Enum defining the child span types.
enum class ChildSpanType {
  NONE = 0,
  INIT = 1,
  SUBMITTED = 2,
  PLANNING = 3,
  ADMISSION_CONTROL = 4,
  QUERY_EXEC = 5,
  CLOSE = 6
}; // enum class ChildSpanType

std::string to_string(const ChildSpanType& val);
std::ostream& operator<<(std::ostream& out, const ChildSpanType& val);
// enum ChildSpanType

// Manages the root and child spans for a single query. Provides convenience methods to
// start each child span with the appropriate name/attributes and to end each child span.
// Only one child span can be active at a time.
//
// The root span is started when the SpanManager is constructed and ended when the
// SpanManager is destructed. The root span is made the active span for the duration of
// the SpanManager's lifetime.
class SpanManager {
public:
  SpanManager(
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer,
      const ClientRequestState* client_request_state);
  ~SpanManager();

  // Adds an event to the currently active child span. If no child span is active,
  // logs a warning and does nothing else. The event time is set to the current time.
  // Parameters:
  //   name -- The name of the event to add.
  void AddChildSpanEvent(const std::string& name);

  // Functions to start child spans. If another child span is active, it will be ended and
  // a warning will be logged.
  void StartChildSpanInit();
  void StartChildSpanSubmitted();
  void StartChildSpanPlanning();
  void StartChildSpanAdmissionControl();
  void StartChildSpanQueryExecution();
  void StartChildSpanClose();

  // Functions to end child spans. If no child span is active, logs a warning and does
  // nothing else.
  void EndChildSpanInit();
  void EndChildSpanSubmitted();
  void EndChildSpanPlanning();
  void EndChildSpanAdmissionControl();
  void EndChildSpanQueryExecution();
  void EndChildSpanClose();

private:
  // Tracer instance used to construct spans.
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;

  // Scope to make the root span active and to deactive the root span when it finishes.
  std::unique_ptr<opentelemetry::trace::Scope> scope_;

  // ClientRequestState for the query this SpanManager is tracking.
  const ClientRequestState* client_request_state_;

  // Convenience constant string the string representation of the Query ID for the query
  // this SpanManager is tracking.
  const std::string query_id_;

  // TimedSpan instances for the root span and the mutex to protected it.
  std::shared_ptr<TimedSpan> root_;
  std::mutex root_span_mu_;

  // TimedSpan instance for the current child span and the mutex to protect it.
  std::unique_ptr<TimedSpan> current_child_;
  std::mutex child_span_mu_;

  // Span type of the current childspan. Will be ChildSpanType::NONE if no child span is
  // active.
  ChildSpanType child_span_type_;

  // Helper method that builds a child span and populates it with common attributes plus
  // the specified additional attributes. Does not take ownership of the child span mutex.
  void ChildSpanBuilder(const ChildSpanType& span_type,
      OtelAttributesMap additional_attributes = {}, bool running = false);

  // Helper method to end a child span and populate it's common attributes. Does not take
  // ownership of the child span mutex.
  void EndChildSpan(const OtelAttributesMap& additional_attributes = {});
};  // class SpanManager

} // namespace impala
