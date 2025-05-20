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
#include <sstream>
#include <string>

#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/variant.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_metadata.h>
#include <opentelemetry/trace/tracer.h>

#include "common/compiler-util.h"
#include "service/client-request-state.h"

using namespace opentelemetry;
using namespace std;

namespace impala {

// Helper function to get the current time in milliseconds since the epoch.
static long long get_epoch_milliseconds() {
  auto now = chrono::system_clock::now();
  return chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();
} // function get_epoch_milliseconds

TimedSpan::TimedSpan(const nostd::shared_ptr<trace::Tracer>& tracer, const string& name,
    const string& start_time_attribute_name, const string& duration_attribute_name,
    OtelAttributesMap attributes, trace::SpanKind span_kind,
    const std::shared_ptr<TimedSpan>& root) :
    start_time_attribute_name_(start_time_attribute_name),
    duration_attribute_name_(duration_attribute_name),
    start_time_(get_epoch_milliseconds()) {

  trace::StartSpanOptions options;
  options.kind = span_kind;
  if (root) {
    options.parent = root->span_->GetContext();
  } else {
    options.parent = context::Context().SetValue(trace::kIsRootSpanKey, true);
  }

  attributes.insert_or_assign(start_time_attribute_name_,
      static_cast<int64_t>(start_time_));

  span_ = tracer->StartSpan(
    name,
    attributes,
    options);
} // constructor TimedSpan

void TimedSpan::End(const ClientRequestState* client_request_state_) {
  const long long end_time = get_epoch_milliseconds();

  span_->SetAttribute("EndTime", static_cast<int64_t>(end_time));
  span_->SetAttribute(duration_attribute_name_,
      static_cast<int64_t>(end_time - start_time_));

  span_->End();
} // function End

void TimedSpan::SetAttribute(const string& key,
    const common::AttributeValue& value) noexcept {
  span_->SetAttribute(key, value);
} // function SetAttribute

void TimedSpan::SetAttributeEmpty(const string& key) noexcept {
  this->SetAttribute(key, "");
} // function SetAttributeEmpty

void TimedSpan::AddEvent(const string& name, const OtelAttributesMap&
    additional_attributes) noexcept {
  span_->AddEvent(name, additional_attributes);
} // function AddEvent

trace::Scope TimedSpan::SetActive() {
  return trace::Tracer::WithActiveSpan(span_);
} // function SetActive

// Helper function to convert an OpenTelemetry ID to a string representation.
template <size_t N>
static string id_to_string(const nostd::span<const uint8_t, N> id) {
  DCHECK(N == 8 || N == 16) << "id_to_string only supports 8 or 16 byte IDs";

  std::ostringstream oss;
  for (auto byte : id) {
    oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte);
  }

  return oss.str();
} // function id_to_string

string TimedSpan::SpanId() const {
  if (LIKELY(span_)) {
    return id_to_string(span_->GetContext().span_id().Id());
  }

  return "";
} // function SpanId

string TimedSpan::TraceId() const {
  if (LIKELY(span_)) {
    return id_to_string(span_->GetContext().trace_id().Id());
  }

  return "";
} // function TraceId

} // namespace impala