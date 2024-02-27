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

// Contains implementations of the QueryStateRecord and QueryStateExpanded struct
// functions. These structs represent the state of a query for capturing the query in the
// query log and completed queries table.

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>

#include <gutil/strings/numbers.h>
#include <gutil/strings/strcat.h>
#include "runtime/coordinator.h"
#include "scheduling/admission-controller.h"
#include "scheduling/scheduler.h"
#include "service/client-request-state.h"
#include "service/query-state-record.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/string-util.h"

using namespace std;

namespace impala {
 
QueryStateRecord::QueryStateRecord(
    const ClientRequestState& query_handle, vector<uint8_t>&& compressed_profile)
  : compressed_profile(compressed_profile) {
  Init(query_handle);
}

QueryStateRecord::QueryStateRecord(const ClientRequestState& query_handle)
  : compressed_profile() {
  Init(query_handle);
}

void QueryStateRecord::Init(const ClientRequestState& query_handle) {
  id = query_handle.query_id();

  const string* plan_str = query_handle.summary_profile()->GetInfoString("Plan");
  if (plan_str != nullptr) {
    plan = *plan_str;
    boost::algorithm::trim_if(plan, boost::algorithm::is_any_of("\n"));
  }

  stmt = query_handle.sql_stmt();
  effective_user = query_handle.effective_user();
  default_db = query_handle.default_db();
  start_time_us = query_handle.start_time_us();
  end_time_us = query_handle.end_time_us();
  wait_time_ms = query_handle.wait_time_ms();
  query_handle.summary_profile()->GetTimeline(&timeline);

  Coordinator* coord = query_handle.GetCoordinator();
  if (coord != nullptr) {
    num_completed_scan_ranges = coord->scan_progress().num_complete();
    total_scan_ranges = coord->scan_progress().total();
    num_completed_fragment_instances = coord->query_progress().num_complete();
    total_fragment_instances = coord->query_progress().total();
    auto utilization = coord->ComputeQueryResourceUtilization();
    total_peak_mem_usage = utilization.total_peak_mem_usage;
    cluster_mem_est = query_handle.schedule()->cluster_mem_est();
    bytes_read = utilization.bytes_read;
    bytes_sent = utilization.exchange_bytes_sent + utilization.scan_bytes_sent;
    has_coord = true;
  } else {
    num_completed_scan_ranges = 0;
    total_scan_ranges = 0;
    num_completed_fragment_instances = 0;
    total_fragment_instances = 0;
    total_peak_mem_usage = 0;
    cluster_mem_est = 0;
    bytes_read = 0;
    bytes_sent = 0;
    has_coord = false;
  }
  beeswax_query_state = query_handle.BeeswaxQueryState();
  ClientRequestState::RetryState retry_state = query_handle.retry_state();
  if (retry_state == ClientRequestState::RetryState::NOT_RETRIED) {
    query_state = beeswax::_QueryState_VALUES_TO_NAMES.find(beeswax_query_state)->second;
  } else {
    query_state = query_handle.RetryStateToString(retry_state);
  }
  num_rows_fetched = query_handle.num_rows_fetched();
  query_status = query_handle.query_status();

  query_handle.query_events()->ToThrift(&event_sequence);

  const TExecRequest& request = query_handle.exec_request();
  stmt_type = request.stmt_type;
  // Save the query fragments so that the plan can be visualised.
  for (const TPlanExecInfo& plan_exec_info: request.query_exec_request.plan_exec_info) {
    fragments.insert(fragments.end(),
        plan_exec_info.fragments.begin(), plan_exec_info.fragments.end());
  }
  all_rows_returned = query_handle.eos();
  last_active_time_ms = query_handle.last_active_ms();
  // For statement types other than QUERY/DML, show an empty string for resource pool
  // to indicate that they are not subjected to admission control.
  if (stmt_type == TStmtType::QUERY || stmt_type == TStmtType::DML) {
    resource_pool = query_handle.request_pool();
  }
  user_has_profile_access = query_handle.user_has_profile_access();

  // In some cases like canceling and closing the original query or closing the session
  // we may not create the new query, we also check whether the retrided query id is set.
  was_retried = query_handle.WasRetried() && query_handle.IsSetRetriedId();
  if (was_retried) {
    retried_query_id = make_unique<TUniqueId>(query_handle.retried_id());
  }
}

bool QueryStateRecord::StartTimeComparator::operator() (
    const QueryStateRecord& lhs, const QueryStateRecord& rhs) const {
  if (lhs.start_time_us == rhs.start_time_us) return lhs.id < rhs.id;
  return lhs.start_time_us < rhs.start_time_us;
}

int64_t EstimateSize(const QueryStateRecord* record) {
  int64_t size = sizeof(QueryStateRecord); // 800
  size += sizeof(uint8_t) * record->compressed_profile.capacity();
  size += record->effective_user.capacity();
  size += record->default_db.capacity();
  size += record->stmt.capacity();
  size += record->plan.capacity();
  size += record->query_state.capacity();
  size += record->timeline.capacity();
  size += record->resource_pool.capacity();

  // The following dynamic memory of field members are estimated rather than
  // exactly sized. Some of thrift members might be nested, but the estimation
  // does not traverse deeper than the first level.

  // TExecSummary exec_summary
  if (record->exec_summary.__isset.nodes) {
    size += sizeof(TPlanNodeExecSummary) * record->exec_summary.nodes.capacity();
  }
  if (record->exec_summary.__isset.exch_to_sender_map) {
    size += sizeof(int32_t) * 2 * record->exec_summary.exch_to_sender_map.size();
  }
  if (record->exec_summary.__isset.error_logs) {
    for (const auto& log : record->exec_summary.error_logs) size += log.capacity();
  }
  if (record->exec_summary.__isset.queued_reason) {
    size += record->exec_summary.queued_reason.capacity();
  }

  // Status query_status
  if (!record->query_status.ok()) {
    size += record->query_status.msg().msg().capacity();
    for (const auto& detail : record->query_status.msg().details()) {
      size += detail.capacity();
    }
  }

  // TEventSequence event_sequence
  size += record->event_sequence.name.capacity();
  size += sizeof(int64_t) * record->event_sequence.timestamps.capacity();
  for (const auto& label : record->event_sequence.labels) size += label.capacity();

  // vector<TPlanFragment> fragments
  size += sizeof(TPlanFragment) * record->fragments.capacity();

  return size;
}

} // namespace impala