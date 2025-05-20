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

#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <opentelemetry/nostd/shared_ptr.h>

#include "common/compiler-util.h"
#include "observe/timed-span.h"
#include "runtime/query-driver.h"
#include "service/client-request-state.h"
#include "util/debug-util.h"

using namespace opentelemetry;
using namespace std;

DECLARE_string(cluster_id);

namespace impala {

SpanManager::SpanManager(nostd::shared_ptr<trace::Tracer> tracer,
    const QueryHandle* query_handle) : tracer_(std::move(tracer)),
    query_handle_(query_handle), query_id_(PrintId((*query_handle)->query_id())) {}

// Implementation of SpanManager methods
void SpanManager::StartRootSpan() {
  lock_guard<mutex> l(root_span_mu_);

  DCHECK(!root_) << "Cannot start a new root span while one is already active.";
  DCHECK(query_handle_) << "Cannot start root span without a valid query handle.";

  root_ = make_shared<TimedSpan>(tracer_, "QueryStartTime", "Runtime",
      query_id_,
      OtelAttributesMap{
        {"ClusterId", FLAGS_cluster_id},
        {"QueryId", query_id_},
        {"RequestPool", (*query_handle_)->request_pool()},
        {"SessionId", query_id_},
        {"UserName", (*query_handle_)->effective_user()}
      },
      trace::SpanKind::kServer);
} // function StartRootSpan

void SpanManager::EndRootSpan() {
  lock_guard<mutex> l(root_span_mu_);

  DCHECK(root_) << "Cannot end root span when one is not active.";
  DCHECK(query_handle_) << "Cannot end root span without a valid query handle.";

  if (LIKELY(root_)) {
    root_->SetAttribute("ErrorMsg", "TODO");
    root_->SetAttribute("State",
      ClientRequestState::ExecStateToString((*query_handle_)->exec_state()));
    root_->End(query_handle_);
    root_.reset();
  }
} // function EndRootSpan

void SpanManager::EndChildSpan() {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(current_child_) << "Cannot end child span when one is not active.";
  DCHECK(query_handle_) << "Cannot end child span without a valid query handle.";

  if (LIKELY(current_child_)) {
    current_child_->SetAttribute("StatusMessage",
        (*query_handle_)->query_status().msg().msg());
    current_child_->End(query_handle_);
    current_child_.reset();
  }
} // function EndChildSpan

void SpanManager::AddChildSpanEvent(const opentelemetry::nostd::string_view name) {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(current_child_) << "Cannot add event when child span is not active.";

  if (LIKELY(current_child_)) {
    current_child_->AddEvent(name);
  } else {
    LOG(WARNING) << "Attempted to add event '" << name << "' to a non-active child span.";
  }
} // function AddChildSpanEvent

void SpanManager::StartChildSpanInit() {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(!current_child_) << "Cannot start a new child span while one is already active.";
  DCHECK(query_handle_) << "Cannot start child span without a valid query handle.";

  const string span_name = query_id_ + " - Init";

  current_child_ = make_shared<TimedSpan>(tracer_, span_name, "BeginTime", "ElapsedTime",
    OtelAttributesMap{
        {"Name", span_name},
        {"DefaultDb", (*query_handle_)->default_db()},
        {"Running", false},
        {"QueryId", query_id_},
        {"QueryString", (*query_handle_)->redacted_sql()},
        {"UserName", (*query_handle_)->effective_user()}
      });
} // function StartChildSpanInit

void SpanManager::StartChildSpanQuerySubmitted() {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(!current_child_) << "Cannot start a new child span while one is already active.";
  DCHECK(query_handle_) << "Cannot start child span without a valid query handle.";

  const string span_name = query_id_ + " - Submitted";

  current_child_ = make_shared<TimedSpan>(tracer_, span_name, "BeginTime", "ElapsedTime",
    OtelAttributesMap{
        {"Name", span_name},
        {"Running", false}
    });
} // function StartChildSpanQuerySubmitted

void SpanManager::StartChildSpanPlanning() {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(!current_child_) << "Cannot start a new child span while one is already active.";
  DCHECK(query_handle_) << "Cannot start child span without a valid query handle.";

  const string span_name = query_id_ + " - Planning";

  current_child_ = make_shared<TimedSpan>(tracer_, span_name, "BeginTime", "ElapsedTime",
    OtelAttributesMap{
        {"Name", span_name},
        {"Running", false}
    });
} // function StartChildSpanPlanning

void SpanManager::StartChildSpanAdmissionControl() {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(!current_child_) << "Cannot start a new child span while one is already active.";
  DCHECK(query_handle_) << "Cannot start child span without a valid query handle.";

  const string span_name = query_id_ + " - AdmissionControl";

  current_child_ = make_shared<TimedSpan>(tracer_, span_name, "BeginTime", "ElapsedTime",
    OtelAttributesMap{
        {"Name", span_name},
        {"Running", false}
    });
} // function StartChildSpanAdmissionControl

void SpanManager::StartChildSpanQueryExecution() {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(!current_child_) << "Cannot start a new child span while one is already active.";
  DCHECK(query_handle_) << "Cannot start child span without a valid query handle.";

  const string span_name = query_id_ + " - QueryExecution";

  current_child_ = make_shared<TimedSpan>(tracer_, span_name, "BeginTime", "ElapsedTime",
    OtelAttributesMap{
        {"Name", span_name},
        {"Running", true}
    });
} // function StartChildSpanQueryExecution

void SpanManager::StartChildSpanClose() {
  lock_guard<mutex> l(child_span_mu_);

  DCHECK(!current_child_) << "Cannot start a new child span while one is already active.";
  DCHECK(query_handle_) << "Cannot start child span without a valid query handle.";

  const string span_name = query_id_ + " - Close";

  current_child_ = make_shared<TimedSpan>(tracer_, span_name, "BeginTime", "ElapsedTime",
    OtelAttributesMap{
        {"Name", span_name},
        {"Running", false}
    });
} // function StartChildSpanClose

} // namespace impala