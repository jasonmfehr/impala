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

#include "observe/span-manager.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <opentelemetry/nostd/shared_ptr.h>

#include "common/compiler-util.h"
#include "gen-cpp/Types_types.h"
#include "observe/timed-span.h"
#include "service/client-request-state.h"
#include "util/debug-util.h"

using namespace opentelemetry;
using namespace std;

DECLARE_string(cluster_id);
DECLARE_int32(otel_trace_retry_policy_max_attempts);
DECLARE_int32(otel_trace_retry_policy_max_backoff_s);

namespace impala {

// Helper function to log the start and end of a span with debug information. Callers
// must hold the child_span_mu_ lock when calling this function.
static void debug_log_span(const TimedSpan* span, const string& span_name,
    const string query_id, bool started) {
  DCHECK_NOTNULL(span);

  VLOG(2) << (started ? "Started" : "Ended") << " " << span_name << " span query_id=\""
      << query_id << "\" trace_id=\"" << span->TraceId() << "\" span_id=\""
      << span->SpanId() << "\"";
} // function debug_log_span

SpanManager::SpanManager(nostd::shared_ptr<trace::Tracer> tracer,
    const ClientRequestState* client_request_state) : tracer_(std::move(tracer)),
    client_request_state_(client_request_state),
    query_id_(PrintId(client_request_state_->query_id())) {}

// Implementation of SpanManager methods
void SpanManager::StartRootSpan() {
  lock_guard<mutex> l(root_span_mu_);

  DCHECK(!root_) << "Cannot start a new root span while one is already active.";
  DCHECK(client_request_state_) << "Cannot start root span without a valid client "
      "request state.";

  root_ = make_shared<TimedSpan>(tracer_, query_id_, "QueryStartTime", "Runtime",
      OtelAttributesMap{
        {"ClusterId", FLAGS_cluster_id},
        {"OriginalQueryId", (client_request_state_->IsRetriedQuery() ?
            PrintId(client_request_state_->original_id()) : "")},
        {"QueryId", query_id_},
        {"RequestPool", client_request_state_->request_pool()},
        {"SessionId", PrintId(client_request_state_->session_id())},
        {"UserName", client_request_state_->effective_user()}
      },
      trace::SpanKind::kServer);

  scope_ = make_shared<trace::Scope>(root_->SetActive());
  debug_log_span(root_.get(), "root", query_id_, true);
} // function StartRootSpan

void SpanManager::EndRootSpan() {
  lock_guard<mutex> l(root_span_mu_);

  DCHECK(root_) << "Cannot end root span when one is not active.";
  DCHECK(client_request_state_) << "Cannot end root span without a valid client "
      "request state.";

  if (LIKELY(root_)) {
    root_->SetAttribute("QueryType",
        to_string(client_request_state_->exec_request().stmt_type));
    root_->SetAttribute("ErrorMessage", "TODO");
    root_->SetAttribute("Status", "TODO");
    root_->SetAttribute("State",
      ClientRequestState::ExecStateToString(client_request_state_->exec_state()));
    root_->End(client_request_state_);

    debug_log_span(root_.get(), "root", query_id_, false);
    LOG(INFO) << "Submitted OpenTelemetry trace query_id=\"" << query_id_
              << "\" trace_id=\"" << root_->TraceId() << "\" span_id=\""
              << root_->SpanId() << "\"";

    scope_.reset();
    root_.reset();

    tracer_->Close(chrono::seconds(FLAGS_otel_trace_retry_policy_max_backoff_s *
        FLAGS_otel_trace_retry_policy_max_attempts));
  }
} // function EndRootSpan

void SpanManager::EndChildSpan(const OtelAttributesMap& additional_attributes) {
  DCHECK(current_child_) << "Cannot end child span when one is not active.";
  DCHECK(client_request_state_) << "Cannot end child span without a valid client "
      "request state.";

  if (LIKELY(current_child_)) {
    current_child_->SetAttribute("Status", "TODO");
    current_child_->SetAttribute("ErrorMsg", "TODO");

    if (client_request_state_->query_status().ok()) {
      current_child_->SetAttribute("StatusMessage", "OK");
    } else {
      current_child_->SetAttribute("StatusMessage",
          client_request_state_->query_status().msg().msg());
    }

    for (auto a : additional_attributes) {
      current_child_->SetAttribute(a.first, a.second);
    }

    current_child_->End(client_request_state_);
    current_child_.reset();
  }
} // function EndChildSpan

void SpanManager::AddChildSpanEvent(const opentelemetry::nostd::string_view name) {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(current_child_) << "Cannot add event when child span is not active.";

  if (LIKELY(current_child_)) {
    current_child_->AddEvent(name);
    VLOG(2) << "Adding event event_name=\"" << name << "\" query_id=\"" << query_id_
        << "\" trace_id=\"" << root_->TraceId() << "\" span_id=\""
        << root_->SpanId() << "\"";
  } else {
    LOG(WARNING) << "Attempted to add event '" << name << "' to a non-active child span.";
  }
} // function AddChildSpanEvent

void SpanManager::StartChildSpanInit() {
  lock_guard<mutex> l(child_span_mu_);
  current_child_ = ChildSpanBuilder("Init", false,
      OtelAttributesMap{
          {"ClusterId", FLAGS_cluster_id},
          {"DefaultDb", client_request_state_->default_db()},
          {"OriginalQueryId", (client_request_state_->IsRetriedQuery() ?
              PrintId(client_request_state_->original_id()) : "")},
          {"QueryId", query_id_},
          {"QueryString", client_request_state_->redacted_sql()},
          {"RequestPool", client_request_state_->request_pool()},
          {"SessionId", PrintId(client_request_state_->session_id())},
          {"UserName", client_request_state_->effective_user()}
      });

  debug_log_span(current_child_.get(), "Init", query_id_, true);
} // function StartChildSpanInit

void SpanManager::EndChildSpanInit() {
  lock_guard<mutex> l(child_span_mu_);

  debug_log_span(current_child_.get(), "Init", query_id_, false);
  EndChildSpan();
} // function EndChildSpanInit

void SpanManager::StartChildSpanSubmitted() {
  lock_guard<mutex> l(child_span_mu_);
  current_child_ = ChildSpanBuilder("Submitted");

  debug_log_span(current_child_.get(), "Submitted", query_id_, true);
} // function StartChildSpanSubmitted

void SpanManager::EndChildSpanSubmitted() {
  lock_guard<mutex> l(child_span_mu_);

  debug_log_span(current_child_.get(), "Submitted", query_id_, false);

  EndChildSpan();
} // function EndChildSpanSubmitted

void SpanManager::StartChildSpanPlanning() {
  lock_guard<mutex> l(child_span_mu_);
  current_child_ = ChildSpanBuilder("Planning", false);

  debug_log_span(current_child_.get(), "Planning", query_id_, true);
} // function StartChildSpanPlanning

void SpanManager::EndChildSpanPlanning() {
  lock_guard<mutex> l(child_span_mu_);

  debug_log_span(current_child_.get(), "Planning", query_id_, false);
  EndChildSpan(
      OtelAttributesMap{
          {"QueryType", to_string(client_request_state_->exec_request().stmt_type)}
      });
} // function EndChildSpanPlanning

void SpanManager::StartChildSpanAdmissionControl() {
  lock_guard<mutex> l(child_span_mu_);
  current_child_ = ChildSpanBuilder("AdmissionControl", false,
      OtelAttributesMap{
          {"RequestPool", client_request_state_->request_pool()}
      });
} // function StartChildSpanAdmissionControl

void SpanManager::EndChildSpanAdmissionControl() {
  lock_guard<mutex> l(child_span_mu_);
  EndChildSpan(
      OtelAttributesMap{
          {"Queued", false},
          {"QueuedReason", "TODO"}
      });
}

void SpanManager::StartChildSpanQueryExecution() {
  lock_guard<mutex> l(child_span_mu_);
  current_child_ = ChildSpanBuilder("QueryExecution", true);
} // function StartChildSpanQueryExecution

void SpanManager::EndChildSpanQueryExecution() {
  lock_guard<mutex> l(child_span_mu_);
  EndChildSpan(
      OtelAttributesMap{
          {"NumDeletedRows", 0},
          {"NumModifiedRows", 0},
          {"NumRowsFetched", 0},
          {"NumRowsFetchedFromCache", 0}
      });
} // function EndChildSpanQueryExecution

void SpanManager::StartChildSpanClose() {
  lock_guard<mutex> l(child_span_mu_);

  current_child_ = ChildSpanBuilder("Close");
} // function StartChildSpanClose

void SpanManager::EndChildSpanClose() {
  lock_guard<mutex> l(child_span_mu_);
  EndChildSpan();
} // function EndChildSpanClose

std::shared_ptr<TimedSpan> SpanManager::ChildSpanBuilder(const std::string& span_name,
    bool running, OtelAttributesMap additional_attributes) const {

  DCHECK(!current_child_) << "Cannot start a new child span while one is already active.";
  DCHECK(client_request_state_) << "Cannot start child span without a valid client "
      " request state.";
  DCHECK(!span_name.empty()) << "Span name cannot be empty.";

  const string full_span_name = query_id_ + " - " + span_name;

  additional_attributes.insert_or_assign("Name", full_span_name);
  additional_attributes.insert_or_assign("Running", running);

  return make_shared<TimedSpan>(tracer_, full_span_name, "BeginTime", "ElapsedTime",
      additional_attributes);
} // function ChildSpanBuilder

} // namespace impala