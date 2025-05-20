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

#include "observe/timed-span.h"

#include <chrono>
#include <string>

#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/nostd/variant.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_metadata.h>
#include <opentelemetry/trace/tracer.h>

#include "runtime/query-driver.h"
#include "service/client-request-state.h"

using namespace opentelemetry;
using namespace std;

namespace impala {

  static const long long get_epoch_milliseconds() {
  auto now = chrono::system_clock::now();
  return chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();
}

TimedSpan::TimedSpan(nostd::shared_ptr<trace::Tracer> tracer, const string name,
    const string start_time_attribute_name, const string duration_attribute_name,
    OtelAttributesMap attributes,
    trace::SpanKind span_kind) : start_time_attribute_name_(start_time_attribute_name),
    duration_attribute_name_(duration_attribute_name),
    start_time_(get_epoch_milliseconds()) {

  trace::StartSpanOptions options;
  options.kind = span_kind;

  attributes.insert_or_assign(start_time_attribute_name_,
      static_cast<int64_t>(start_time_));

  span_ = tracer->StartSpan(
    name,
    attributes,
    options);
}

void TimedSpan::End(const QueryHandle* query_handle, const string status) {
  const long long end_time = get_epoch_milliseconds();

  span_->SetAttribute("EndTime", static_cast<int64_t>(end_time));
  span_->SetAttribute(duration_attribute_name_,
      static_cast<int64_t>(end_time - start_time_));
      span_->SetAttribute("Status", status);

  span_->End();
}

void TimedSpan::SetAttribute(nostd::string_view key,
    const common::AttributeValue& value) noexcept {
  span_->SetAttribute(key, value);
}

void TimedSpan::AddEvent(nostd::string_view name) noexcept {
  span_->AddEvent(name, {});
}

} // namespace impala