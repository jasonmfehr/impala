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

// Constants declaring the names of span attributes.
static constexpr char const* ATTR_BEGIN_TIME = "BeginTime";
static constexpr char const* ATTR_CLUSTER_ID = "ClusterId";
static constexpr char const* ATTR_DEFAULT_DB = "DefaultDb";
static constexpr char const* ATTR_ELAPSED_TIME = "ElapsedTime";
static constexpr char const* ATTR_ERROR_MESSAGE = "ErrorMessage";
static constexpr char const* ATTR_ERROR_MSG = "ErrorMsg";
static constexpr char const* ATTR_NAME = "Name";
static constexpr char const* ATTR_NUM_DELETED_ROWS = "NumDeletedRows";
static constexpr char const* ATTR_NUM_MODIFIED_ROWS = "NumModifiedRows";
static constexpr char const* ATTR_NUM_ROWS_FETCHED = "NumRowsFetched";
static constexpr char const* ATTR_NUM_ROWS_FETCHED_FROM_CACHE = "NumRowsFetchedFromCache";
static constexpr char const* ATTR_ORIGINAL_QUERY_ID = "OriginalQueryId";
static constexpr char const* ATTR_QUERY_ID = "QueryId";
static constexpr char const* ATTR_QUERY_START_TIME = "QueryStartTime";
static constexpr char const* ATTR_QUERY_STRING = "QueryString";
static constexpr char const* ATTR_QUERY_TYPE = "QueryType";
static constexpr char const* ATTR_QUEUED = "Queued";
static constexpr char const* ATTR_QUEUED_REASON = "QueuedReason";
static constexpr char const* ATTR_REQUEST_POOL = "RequestPool";
static constexpr char const* ATTR_RETRIED_QUERY_ID = "RetriedQueryId";
static constexpr char const* ATTR_RUNNING = "Running";
static constexpr char const* ATTR_RUNTIME = "Runtime";
static constexpr char const* ATTR_SESSION_ID = "SessionId";
static constexpr char const* ATTR_STATE = "State";
static constexpr char const* ATTR_STATUS = "Status";
static constexpr char const* ATTR_USER_NAME = "UserName";

// Constants defining the names of the child spans.
static constexpr char const* CHILD_SPAN_NAMES[] = {
    "None", "Init", "Submitted", "Planning", "AdmissionControl", "QueryExecution",
    "Close"};

#define DCHECK_CHILD_SPAN_TYPE(expected_type) \
  DCHECK(child_span_type_ == expected_type) << "Expected child span type '" \
      << expected_type << "' but received '" << child_span_type_ << "' instead."
// macro DCHECK_CHILD_SPAN_TYPE
namespace impala {

std::string to_string(const ChildSpanType& val) {
  return CHILD_SPAN_NAMES[static_cast<int>(val)];
}

std::ostream& operator<<(std::ostream& out, const ChildSpanType& val) {
  out << to_string(val);
  return out;
}

// Helper function to log the start and end of a span with debug information. Callers
// must hold the child_span_mu_ lock when calling this function.
static inline void debug_log_span(const TimedSpan* span, const string& span_name,
    const string& query_id, bool started) {
  DCHECK(span != nullptr) << "Cannot log null span.";

  if (LIKELY(span != nullptr)) {
    VLOG(2) << (started ? "Started" : "Ended") << " '" << span_name << "' span "
        "trace_id=\"" << span->TraceId() << "\" span_id=\"" << span->SpanId() << "\" "
        "query_id=\"" << query_id << "\"";
  } else {
    LOG(WARNING) << "Attempted to log span '" << span_name << "' but provided span is "
        "null.";
  }
} // function debug_log_span

SpanManager::SpanManager(nostd::shared_ptr<trace::Tracer> tracer,
    const ClientRequestState* client_request_state) : tracer_(move(tracer)),
    client_request_state_(client_request_state),
    query_id_(PrintId(client_request_state_->query_id())) {
  child_span_type_ = ChildSpanType::NONE;

  DCHECK(!root_) << "Cannot start a new root span while one is already active.";
  DCHECK(client_request_state_ != nullptr) << "Cannot start root span without a valid "
      "client request state.";

  root_ = make_shared<TimedSpan>(tracer_, query_id_, ATTR_QUERY_START_TIME, ATTR_RUNTIME,
      OtelAttributesMap{
        {ATTR_CLUSTER_ID, FLAGS_cluster_id},
        {ATTR_QUERY_ID, query_id_},
        {ATTR_REQUEST_POOL, client_request_state_->request_pool()},
        {ATTR_SESSION_ID, PrintId(client_request_state_->session_id())},
        {ATTR_USER_NAME, client_request_state_->effective_user()}
      },
      trace::SpanKind::kServer);

  scope_ = make_unique<trace::Scope>(root_->SetActive());
  debug_log_span(root_.get(), "Root", query_id_, true);
} // ctor

SpanManager::~SpanManager() {
  lock_guard<mutex> l(root_span_mu_);

  DCHECK(client_request_state_ != nullptr) << "Cannot end root span without a valid "
      "client request state.";

  // End child span here if needed.
  if (UNLIKELY(current_child_)) {
    EndChildSpan();
  }

  root_->SetAttribute(ATTR_QUERY_TYPE,
      to_string(client_request_state_->exec_request().stmt_type));

  if (client_request_state_->query_status().ok()) {
    root_->SetAttributeEmpty(ATTR_ERROR_MESSAGE);
  } else {
    string error_msg = client_request_state_->query_status().msg().msg();

    for (const auto& detail : client_request_state_->query_status().msg().details()) {
      error_msg += "\n" + detail;
    }

    root_->SetAttribute(ATTR_ERROR_MESSAGE, error_msg);
  }

  if (UNLIKELY(client_request_state_->WasRetried())) {
    root_->SetAttribute(ATTR_STATE, ClientRequestState::RetryStateToString(
        client_request_state_->retry_state()));
    root_->SetAttribute(ATTR_RETRIED_QUERY_ID,
        PrintId(client_request_state_->retried_id()));
  } else {
    root_->SetAttribute(ATTR_STATE,
      ClientRequestState::ExecStateToString(client_request_state_->exec_state()));
      root_->SetAttributeEmpty(ATTR_RETRIED_QUERY_ID);
  }

  if (UNLIKELY(client_request_state_->IsRetriedQuery())) {
    root_->SetAttribute(ATTR_ORIGINAL_QUERY_ID,
        PrintId(client_request_state_->original_id()));
  } else {
    root_->SetAttributeEmpty(ATTR_ORIGINAL_QUERY_ID);
  }

  root_->End(client_request_state_);

  debug_log_span(root_.get(), "Root", query_id_, false);
  LOG(INFO) << "Submitted OpenTelemetry trace with trace_id=\"" << root_->TraceId()
      << "\" span_id=\"" << root_->SpanId() << "\"";

  scope_.reset();
  root_.reset();

  tracer_->Close(chrono::seconds(FLAGS_otel_trace_retry_policy_max_backoff_s *
      FLAGS_otel_trace_retry_policy_max_attempts));
}

void SpanManager::AddChildSpanEvent(const string& name) {
  lock_guard<mutex> l(child_span_mu_);

  if (LIKELY(current_child_)) {
    current_child_->AddEvent(name);
    VLOG(2) << "Adding event event named '" << name << "' to child span '"
        << child_span_type_ << "' trace_id=\"" << current_child_->TraceId()
        << "\" span_id=\"" << root_->SpanId() << "\"";
  } else {
    LOG(WARNING) << "Attempted to add event '" << name << "' with no active child span "
        "trace_id=\"" << root_->TraceId() << "\" span_id=\"" << root_->SpanId() << "\"\n"
        << GetStackTrace();
    DCHECK(current_child_) << "Cannot add event when child span is not active.";
  }
} // function AddChildSpanEvent

void SpanManager::StartChildSpanInit() {
  ChildSpanBuilder(ChildSpanType::INIT,
      OtelAttributesMap{
        {ATTR_CLUSTER_ID, FLAGS_cluster_id},
        {ATTR_DEFAULT_DB, client_request_state_->default_db()},
        {ATTR_QUERY_ID, query_id_},
        {ATTR_QUERY_STRING, client_request_state_->redacted_sql()},
        {ATTR_REQUEST_POOL, client_request_state_->request_pool()},
        {ATTR_SESSION_ID, PrintId(client_request_state_->session_id())},
        {ATTR_USER_NAME, client_request_state_->effective_user()}
      });
} // function StartChildSpanInit

void SpanManager::EndChildSpanInit() {
  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::INIT);

  EndChildSpan(
    OtelAttributesMap{
      {ATTR_ORIGINAL_QUERY_ID, (client_request_state_->IsRetriedQuery() ?
          PrintId(client_request_state_->original_id()) : "")}
    }
  );
} // function EndChildSpanInit

void SpanManager::StartChildSpanSubmitted() {
  ChildSpanBuilder(ChildSpanType::SUBMITTED);
} // function StartChildSpanSubmitted

void SpanManager::EndChildSpanSubmitted() {
  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::SUBMITTED);
  EndChildSpan();
} // function EndChildSpanSubmitted

void SpanManager::StartChildSpanPlanning() {
  ChildSpanBuilder(ChildSpanType::PLANNING);
} // function StartChildSpanPlanning

void SpanManager::EndChildSpanPlanning() {
  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::PLANNING);
  EndChildSpan(
      OtelAttributesMap{
        {"QueryType", to_string(client_request_state_->exec_request().stmt_type)}
      });
} // function EndChildSpanPlanning

void SpanManager::StartChildSpanAdmissionControl() {
  ChildSpanBuilder(ChildSpanType::ADMISSION_CONTROL,
      OtelAttributesMap{
        {ATTR_REQUEST_POOL, client_request_state_->request_pool()}
      });
} // function StartChildSpanAdmissionControl

void SpanManager::EndChildSpanAdmissionControl() {
  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::ADMISSION_CONTROL);
  EndChildSpan(
      OtelAttributesMap{
        {ATTR_QUEUED, false},
        {ATTR_QUEUED_REASON,
            *client_request_state_->summary_profile()->GetInfoString("Admission result")}
      });
}

void SpanManager::StartChildSpanQueryExecution() {
  ChildSpanBuilder(ChildSpanType::QUERY_EXEC, {}, true);
} // function StartChildSpanQueryExecution

void SpanManager::EndChildSpanQueryExecution() {
  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::QUERY_EXEC);

  OtelAttributesMap attrs;

  if (client_request_state_->exec_request().stmt_type == TStmtType::QUERY) {
    attrs.emplace(ATTR_NUM_DELETED_ROWS, static_cast<int64_t>(0));
    attrs.emplace(ATTR_NUM_MODIFIED_ROWS, static_cast<int64_t>(0));
  } else {
    attrs.emplace(ATTR_NUM_DELETED_ROWS, static_cast<int64_t>(-1));
    attrs.emplace(ATTR_NUM_MODIFIED_ROWS, static_cast<int64_t>(-1));
  }

  attrs.emplace(ATTR_NUM_ROWS_FETCHED, client_request_state_->num_rows_fetched());
  attrs.emplace(ATTR_NUM_ROWS_FETCHED_FROM_CACHE,
      client_request_state_->num_rows_fetched_from_cache_counter());

  EndChildSpan(attrs);
} // function EndChildSpanQueryExecution

void SpanManager::StartChildSpanClose() {
  ChildSpanBuilder(ChildSpanType::CLOSE);
} // function StartChildSpanClose

void SpanManager::EndChildSpanClose() {
  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::CLOSE);
  EndChildSpan();
} // function EndChildSpanClose

void SpanManager::ChildSpanBuilder(const ChildSpanType& span_type,
    OtelAttributesMap additional_attributes, bool running) {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(client_request_state_ != nullptr) << "Cannot start child span without a valid "
      "client request state.";
  DCHECK(span_type != ChildSpanType::NONE) << "Span type cannot be " << span_type << ".";

  if (UNLIKELY(current_child_)) {
    LOG(WARNING) << "Attempted to start child span '" << to_string(span_type)
          << "' while another child span '" << to_string(child_span_type_)
          << "' is still active trace_id=\"" << root_->TraceId() << "\" span_id=\""
          << root_->SpanId() << "\"\n" << GetStackTrace();
    DCHECK(!current_child_) << "Cannot start a new child span while one is already "
        << "active.";

    EndChildSpan();
  }

  const string full_span_name = query_id_ + " - " + to_string(span_type);

  additional_attributes.insert_or_assign(ATTR_NAME, full_span_name);
  additional_attributes.insert_or_assign(ATTR_RUNNING, running);

  current_child_ = make_unique<TimedSpan>(tracer_, full_span_name, ATTR_BEGIN_TIME,
    ATTR_ELAPSED_TIME, additional_attributes, trace::SpanKind::kInternal, root_);
  child_span_type_ = span_type;

  debug_log_span(current_child_.get(), to_string(span_type), query_id_, true);
} // function ChildSpanBuilder

void SpanManager::EndChildSpan(const OtelAttributesMap& additional_attributes) {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(client_request_state_ != nullptr) << "Cannot end child span without a valid "
      "client request state.";

  if (LIKELY(current_child_)) {
    for (auto a : additional_attributes) {
      current_child_->SetAttribute(a.first, a.second);
    }

    current_child_->SetAttribute(ATTR_STATUS,
        ClientRequestState::ExecStateToString(client_request_state_->exec_state()));

    if (client_request_state_->query_status().ok()) {
      current_child_->SetAttributeEmpty(ATTR_ERROR_MSG);
    } else {
      string error_msg = client_request_state_->query_status().msg().msg();

      for (const auto& detail : client_request_state_->query_status().msg().details()) {
        error_msg += "\n" + detail;
      }

      current_child_->SetAttribute(ATTR_ERROR_MSG, error_msg);
    }

    current_child_->End(client_request_state_);

    debug_log_span(current_child_.get(), to_string(child_span_type_), query_id_, false);

    current_child_.reset();
    child_span_type_ = ChildSpanType::NONE;
  } else {
    LOG(WARNING) << "Attempted to end a non-active child span trace_id=\""
        << root_->TraceId() << "\" span_id=\"" << root_->SpanId() << "\"\n"
        << GetStackTrace();
    DCHECK(current_child_) << "Cannot end child span when one is not active.";
  }
} // function EndChildSpan

} // namespace impala