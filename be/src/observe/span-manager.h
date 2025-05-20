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
#include <string>
#include <utility>

#include <opentelemetry/nostd/shared_ptr.h>

#include "observe/timed-span.h"

namespace impala {

// Forward declaration to break cyclical imports.
class ClientRequestState;

// Manages the root and child spans for a single query. Provides convenience methods to
// start each child span with the appropriate name/attributes and to end each child span.
// Only one child span can be active at a time.
class SpanManager {
public:
  SpanManager(
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer,
      const ClientRequestState* client_request_state);

  void StartRootSpan();

  void EndRootSpan();

  void AddChildSpanEvent(const std::string name);

  void StartChildSpanInit();
  void EndChildSpanInit();

  void StartChildSpanSubmitted();
  void EndChildSpanSubmitted();

  void StartChildSpanPlanning();
  void EndChildSpanPlanning();

  void StartChildSpanAdmissionControl();
  void EndChildSpanAdmissionControl();

  void StartChildSpanQueryExecution();
  void EndChildSpanQueryExecution();

  void StartChildSpanClose();
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
  std::unique_ptr<TimedSpan> root_;
  std::mutex root_span_mu_;

  // TimedSpan instance for the current child span and the mutex to protect it.
  std::unique_ptr<TimedSpan> current_child_;
  std::mutex child_span_mu_;

  // Helper method that builds a child span and populates it with common attributes plus
  // the specified additional attributes. Does not take ownership of the child span mutex.
  std::unique_ptr<TimedSpan> ChildSpanBuilder(const std::string& span_name,
      bool running = false, OtelAttributesMap additional_attributes = {}) const;

  // Helper method to end a child span and populate it's common attributes. Does not take
  // ownership of the child span mutex.
  void EndChildSpan(const OtelAttributesMap& additional_attributes = {});
};  // class SpanManager

} // namespace impala
