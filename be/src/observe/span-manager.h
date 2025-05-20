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
#include <opentelemetry/nostd/string_view.h>

#include "observe/timed-span.h"

namespace impala {

// Forward declaration to break cyclical imports.
class QueryHandle;

class SpanManager {
public:
  SpanManager(
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer,
      const QueryHandle* query_handle);

  void StartRootSpan();

  void EndRootSpan();

  void EndChildSpan();

  void AddChildSpanEvent(const opentelemetry::nostd::string_view name);

  void StartChildSpanInit();

  void StartChildSpanQuerySubmitted();

  void StartChildSpanPlanning();

  void StartChildSpanAdmissionControl();

  void StartChildSpanQueryExecution();

  void StartChildSpanClose();

private:
  // Constructor set variables
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;
  const QueryHandle* query_handle_;
  const std::string query_id_;

  std::shared_ptr<TimedSpan> root_;
  std::mutex root_span_mu_;
  std::shared_ptr<TimedSpan> current_child_;
  std::mutex child_span_mu_;
};  // class SpanManager

} // namespace impala
