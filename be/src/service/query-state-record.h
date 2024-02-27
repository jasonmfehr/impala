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

#include <cstdint>
#include <map>
#include <vector>

#include "gen-cpp/ExecStats_types.h"
#include "gen-cpp/Types_types.h"
#include "util/network-util.h"

namespace impala {

class ClientRequestState;

/// Snapshot of a query's state, archived in the query log. Not mutated after
/// construction.
struct QueryStateRecord {
  /// Compressed representation of profile returned by RuntimeProfile::Compress().
  /// Must be initialised to a valid value if this is a completed query.
  /// Empty if this was initialised from a running query.
  const std::vector<uint8_t> compressed_profile;

  /// Query id
  TUniqueId id;

  /// Queries are run and authorized on behalf of the effective_user.
  /// If there is no delegated user, this will be the connected user. Otherwise, it
  /// will be set to the delegated user.
  std::string effective_user;

  /// If true, effective_user has access to the runtime profile and execution
  /// summary.
  bool user_has_profile_access;

  /// default db for this query
  std::string default_db;

  /// SQL statement text
  std::string stmt;

  /// Text representation of plan
  std::string plan;

  /// DDL, DML etc.
  TStmtType::type stmt_type;

  /// True if the query required a coordinator fragment
  bool has_coord;

  /// The number of scan ranges that have completed.
  int64_t num_completed_scan_ranges;

  /// The total number of scan ranges.
  int64_t total_scan_ranges;

  /// The number of fragment instances that have completed.
  int64_t num_completed_fragment_instances;

  /// The total number of fragment instances.
  int64_t total_fragment_instances;

  /// The number of rows fetched by the client
  int64_t num_rows_fetched;

  /// The state of the query as of this snapshot. The possible values for the
  /// query_state = union(beeswax::QueryState, ClientRequestState::RetryState). This is
  /// necessary so that the query_state can accurately reflect if a query has been
  /// retried or not. This string is not displayed in the runtime profiles, it is only
  /// displayed on the /queries endpoint of the Web UI when listing out the state of
  /// each query. This is necessary so that users can clearly see if a query has been
  /// retried or not.
  std::string query_state;

  /// The beeswax::QueryState of the query as of this snapshot.
  beeswax::QueryState::type beeswax_query_state;

  /// Start and end time of the query, in Unix microseconds.
  /// A query whose end_time_us is 0 indicates that it is an in-flight query.
  /// These two variables are initialized with the corresponding values from
  /// ClientRequestState.
  int64_t start_time_us, end_time_us;

  /// The request waited time in ms for queued.
  int64_t wait_time_ms;

  /// Total peak memory usage by this query at all backends.
  int64_t total_peak_mem_usage;

  /// The cluster wide estimated memory usage of this query.
  int64_t cluster_mem_est;

  /// Total bytes read by this query at all backends.
  int64_t bytes_read;

  /// The total number of bytes sent (across the network) by this query in exchange
  /// nodes. Does not include remote reads, data written to disk, or data sent to the
  /// client.
  int64_t bytes_sent;

  // Query timeline from summary profile.
  std::string timeline;

  /// Summary of execution for this query.
  TExecSummary exec_summary;

  Status query_status;

  /// Timeline of important query events
  TEventSequence event_sequence;

  /// Save the query plan fragments so that the plan tree can be rendered on the debug
  /// webpages.
  vector<TPlanFragment> fragments;

  // If true, this query has no more rows to return
  bool all_rows_returned;

  // The most recent time this query was actively being processed, in Unix milliseconds.
  int64_t last_active_time_ms;

  /// Resource pool to which the request was submitted for admission, or an empty
  /// string if this request doesn't go through admission control.
  std::string resource_pool;

  /// True if this query was retried, false otherwise.
  bool was_retried = false;

  /// If this query was retried, the query id of the retried query.
  std::unique_ptr<const TUniqueId> retried_query_id;

  /// Initialise from 'exec_state' of a completed query. 'compressed_profile' must be
  /// a runtime profile decompressed with RuntimeProfile::Compress().
  QueryStateRecord(
      const ClientRequestState& exec_state, std::vector<uint8_t>&& compressed_profile);

  /// Initialize from 'exec_state' of a running query
  QueryStateRecord(const ClientRequestState& exec_state);

  /// Default constructor used only when participating in collections
  QueryStateRecord() { }

  struct StartTimeComparator {
    /// Comparator that sorts by start time.
    bool operator() (const QueryStateRecord& lhs, const QueryStateRecord& rhs) const;
  };

  private:
  // Common initialization for constructors.
  void Init(const ClientRequestState& exec_state);
}; // struct QueryStateRecord

/// Return the estimated size of given record in bytes.
/// It does not meant to return exact byte size of given QueryStateRecord in memory,
/// but should account for compressed_profile vector of record.
int64_t EstimateSize(const QueryStateRecord* record);

} // namespace impala
