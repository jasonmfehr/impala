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

/// Contains declarations that only pertain to the worker thread that persists completed
/// queries into the database table.

#pragma once

#include "workload_mgmt/workload-management.h"

#include <array>
#include <string>
#include <utility>

#include <boost/algorithm/string/join.hpp>
#include <gflags/gflags_declare.h>
#include <gutil/strings/substitute.h>

#include "gen-cpp/SystemTables_types.h"
#include "runtime/exec-env.h"
#include "service/query-options.h"
#include "service/query-state-record.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/sql-util.h"
#include "util/string-util.h"

DECLARE_int32(query_log_max_sql_length);
DECLARE_int32(query_log_max_plan_length);

namespace impala {
namespace workloadmgmt {

/// Struct defining the context for generating the sql DML that inserts records into the
/// completed queries table.
struct FieldParserContext {
  const QueryStateExpanded* record;
  const std::string cluster_id;
  StringStreamPop& sql;

  FieldParserContext(const QueryStateExpanded* rec, const std::string& cluster_id,
      StringStreamPop& s) : record(rec), cluster_id(cluster_id), sql(s) {}
}; // struct FieldParserContext


/// Type of a function that retrieves one piece of information from the context and
/// adds it to the SQL statement that inserts rows into the completed queries table.
using FieldParser = void (*)(FieldParserContext&);

/// Constant declaring how to convert from micro and nano seconds to milliseconds.
static constexpr double MICROS_TO_MILLIS = 1000;
static constexpr double NANOS_TO_MILLIS = 1000000;

/// SQL column type for duration columns that store a millisecond value.
static const string MILLIS_DECIMAL_TYPE = strings::Substitute("DECIMAL($0,$1)",
    DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE);

/// Helper function to add a decimal value to the stream that is building the completed
/// queries insert DML.
///
/// Parameters:
///   `ctx`     The field parse context object.
///   `val`     A value to write in the completed queries sql stream.
///   `factor`  The provided val input will be divided by this value.
static void _write_decimal(FieldParserContext& ctx, int64_t val, double factor) {
  ctx.sql << "CAST(" << val / factor << " AS " << MILLIS_DECIMAL_TYPE << ")";
}

/// Helper function to add the timestamp for a single event from the events timeline into
/// the stream that is building the completed queries insert DML.
///
/// Parameters:
///   `ctx`  The field parse context object.
///   `target_event` The element from the QueryEvent enum that represents the event being
///                  inserted into the sql statement. The corresponding event value will
///                  be retrieved from the map of query events.
static void _write_event(FieldParserContext& ctx, QueryEvent target_event) {
  const auto& event = ctx.record->events.find(target_event);
  DCHECK(event != ctx.record->events.end());
  _write_decimal(ctx, event->second, NANOS_TO_MILLIS);
}

/// Number of query table columns. Used to initialize a std::array.
constexpr size_t NumQueryTableColumns = TQueryTableColumn::ORDERBY_COLUMNS + 1;

/// Array containing parser functions for each query column. These parsers are used to
/// generate the value for each query column from a completed query represented by a
/// `QueryStateExpanded` object.
const std::array<FieldParser, NumQueryTableColumns> FIELD_PARSERS = {{
  // cluster id
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << ctx.cluster_id << "'";
  }},

  // query id
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << PrintId(ctx.record->base_state->id) << "'";
  }},

  // session_id
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << PrintId(ctx.record->session_id) << "'";
  }},

  // session type
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << ctx.record->session_type << "'";
  }},

  // hiveserver2 protocol version
  {[](FieldParserContext& ctx) {
    ctx.sql << "'";
    if (ctx.record->session_type == TSessionType::HIVESERVER2) {
      ctx.sql << ctx.record->hiveserver2_protocol_version;
    }
    ctx.sql << "'";
  }},

  // db user
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << ctx.record->base_state->effective_user << "'";
  }},

  // db user connection
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << ctx.record->db_user_connection << "'";
  }},

  // db name
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << ctx.record->base_state->default_db << "'";
  }},

  // impala coordinator
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" <<TNetworkAddressToString(
        ExecEnv::GetInstance()->configured_backend_address()) << "'";
  }},

  // query status
  {[](FieldParserContext& ctx) {
    ctx.sql << "'";
    if (ctx.record->base_state->query_status.ok()) {
      ctx.sql << "OK";
    } else {
      ctx.sql << EscapeSql(ctx.record->base_state->query_status.msg().msg());
    }
    ctx.sql << "'";
  }},

  // query state
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << ctx.record->base_state->query_state << "'";
  }},

  // impala query end state
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << ctx.record->impala_query_end_state << "'";
  }},

  // query type
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << ctx.record->base_state->stmt_type << "'";
  }},

  // network address
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << TNetworkAddressToString(ctx.record->client_address) << "'";
  }},

  // start time utc
  {[](FieldParserContext& ctx) {
    ctx.sql << "UNIX_MICROS_TO_UTC_TIMESTAMP(" <<
        ctx.record->base_state->start_time_us << ")";
  }},

  // total time ms
  {[](FieldParserContext& ctx) {
    _write_decimal(ctx, (ctx.record->base_state->end_time_us -
        ctx.record->base_state->start_time_us), MICROS_TO_MILLIS);
  }},

  // query opts config
  {[](FieldParserContext& ctx) {
    const string opts_str = DebugQueryOptions(ctx.record->query_options);
    ctx.sql << "'" << EscapeSql(opts_str) << "'";
  }},

  // resource pool
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << EscapeSql(ctx.record->base_state->resource_pool) << "'";
  }},

  // per host mem estimate
  {[](FieldParserContext& ctx) {
    ctx.sql << ctx.record->per_host_mem_estimate;
  }},

  // dedicated coord mem estimate
  {[](FieldParserContext& ctx) {
    ctx.sql << ctx.record->dedicated_coord_mem_estimate;
  }},

  // per host fragment instances
  {[](FieldParserContext& ctx) {
    ctx.sql << "'";

    if (!ctx.record->per_host_state.empty()) {
      for (const auto& iter : ctx.record->per_host_state) {
        ctx.sql << TNetworkAddressToString(iter.first) << "=" <<
            iter.second.fragment_instance_count << ',';
      }
      ctx.sql.move_back();
    }

    ctx.sql << "'";
  }},

  // backends count
  {[](FieldParserContext& ctx) {
    if (ctx.record->per_host_state.empty()) {
      ctx.sql << 0;
    } else {
      ctx.sql << ctx.record->per_host_state.size();
    }
  }},

  // admission result
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << ctx.record->admission_result << "'";
  }},

  // cluster memory admitted
  {[](FieldParserContext& ctx) {
    ctx.sql << ctx.record->base_state->cluster_mem_est;
  }},

  // executor group
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << ctx.record->executor_group << "'";
  }},

  // executor groups
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << EscapeSql(ctx.record->executor_groups) << "'";
  }},

  // exec summary
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << EscapeSql(ctx.record->exec_summary) << "'";
  }},

  // num rows fetched
  {[](FieldParserContext& ctx) {
    ctx.sql << ctx.record->base_state->num_rows_fetched;
  }},

  // row materialization rows per sec
  {[](FieldParserContext& ctx) {
    ctx.sql << ctx.record->row_materialization_rate;
  }},

  // row materialization time ms
  {[](FieldParserContext& ctx) {
    _write_decimal(ctx, ctx.record->row_materialization_time, NANOS_TO_MILLIS);
  }},

  // compressed bytes spilled
  {[](FieldParserContext& ctx) {
    ctx.sql << ctx.record->compressed_bytes_spilled;
  }},

  // event planning finished
  {[](FieldParserContext& ctx) {
    _write_event(ctx, PLANNING_FINISHED);
  }},

  // event submit for admission
  {[](FieldParserContext& ctx) {
    _write_event(ctx, SUBMIT_FOR_ADMISSION);
  }},

  // event completed admission
  {[](FieldParserContext& ctx) {
    _write_event(ctx, COMPLETED_ADMISSION);
  }},

  // event all backends started
  {[](FieldParserContext& ctx) {
    _write_event(ctx, ALL_BACKENDS_STARTED);
  }},

  // event rows available
  {[](FieldParserContext& ctx) {
    _write_event(ctx, ROWS_AVAILABLE);
  }},

  // event first row fetched
  {[](FieldParserContext& ctx) {
    _write_event(ctx, FIRST_ROW_FETCHED);
  }},

  // event last row fetched
  {[](FieldParserContext& ctx) {
    _write_event(ctx, LAST_ROW_FETCHED);
  }},

  // event unregister query
  {[](FieldParserContext& ctx) {
    _write_event(ctx, UNREGISTER_QUERY);
  }},

  // read io wait total ms
  {[](FieldParserContext& ctx) {
    _write_decimal(ctx, ctx.record->read_io_wait_time_total, NANOS_TO_MILLIS);
  }},

  // read io wait mean ms
  {[](FieldParserContext& ctx) {
    _write_decimal(ctx, ctx.record->read_io_wait_time_mean, NANOS_TO_MILLIS);
  }},

  // bytes read cache total
  {[](FieldParserContext& ctx) {
    ctx.sql << ctx.record->bytes_read_cache_total;
  }},

  // bytes read total
  {[](FieldParserContext& ctx) {
    ctx.sql << ctx.record->bytes_read_total;
  }},

  // pernode peak mem min
  {[](FieldParserContext& ctx) {
    auto min_elem = min_element(ctx.record->per_host_state.cbegin(),
        ctx.record->per_host_state.cend(), PerHostPeakMemoryComparator);

    if (LIKELY(min_elem != ctx.record->per_host_state.cend())) {
      ctx.sql << min_elem->second.peak_memory_usage;
    } else {
      ctx.sql << 0;
    }
  }},

  // pernode peak mem max
  {[](FieldParserContext& ctx) {
    auto max_elem = max_element(ctx.record->per_host_state.cbegin(),
        ctx.record->per_host_state.cend(), PerHostPeakMemoryComparator);

    if (UNLIKELY(max_elem == ctx.record->per_host_state.cend())) {
      ctx.sql << 0;
    } else {
      ctx.sql << max_elem->second.peak_memory_usage;
    }
  }},

  // pernode peak mem mean
  {[](FieldParserContext& ctx) {
    int64_t calc_mean = 0;

    if (LIKELY(!ctx.record->per_host_state.empty())) {
      for (const auto& host : ctx.record->per_host_state) {
        calc_mean += host.second.peak_memory_usage;
      }

      calc_mean = calc_mean / ctx.record->per_host_state.size();
    }

    ctx.sql << calc_mean;
  }},

  // sql
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" <<
        EscapeSql(ctx.record->redacted_sql, FLAGS_query_log_max_sql_length) << "'";
  }},

  // plan
  {[](FieldParserContext& ctx) {
    ctx.sql << "'"
        << EscapeSql(ctx.record->base_state->plan, FLAGS_query_log_max_plan_length)
        << "'";
  }},

  // tables queried
  {[](FieldParserContext& ctx) {
    ctx.sql << "'" << PrintTableList(ctx.record->tables) << "'";
  }},

  // select columns
  {[](FieldParserContext& ctx) {
    ctx.sql << "'"
        << boost::algorithm::join(ctx.record->select_columns, ",") << "'";
  }},

  // where columns
  {[](FieldParserContext& ctx) {
    ctx.sql << "'"
        << boost::algorithm::join(ctx.record->where_columns, ",") << "'";
  }},

  // join columns
  {[](FieldParserContext& ctx) {
    ctx.sql << "'"
        << boost::algorithm::join(ctx.record->join_columns, ",") << "'";
  }},

  // aggregate columns
  {[](FieldParserContext& ctx) {
    ctx.sql << "'"
        << boost::algorithm::join(ctx.record->aggregate_columns, ",") << "'";
  }},

  // orderby columns
  {[](FieldParserContext& ctx) {
    ctx.sql << "'"
        << boost::algorithm::join(ctx.record->orderby_columns, ",") << "'";
  }}
}}; // FIELD_PARSERS constant array

/// Track the state of the thread that processes the completed queries queue.
enum class WorkloadManagementState {
  // Workload management has not started.
  NOT_STARTED,

  // Running initial startup checks.
  STARTING,

  // Intial startup checks completed.
  STARTED,

  // Initial setup of the workload management db tables is done, completed queries queue
  // is now being processed.
  RUNNING,

  // Coordinator graceful shutdown initiated, and all running queries have finished or
  // been cancelled. The completed queries queue can now be drained.
  SHUTTING_DOWN,

  // In-memory completed queries queue drained, coordinator shutdown can finish.
  SHUTDOWN
};

/// Represents one query that has completed.
struct CompletedQuery {
  // Contains information about the completed query.
  const std::shared_ptr<QueryStateExpanded> query;

  // Count of the number of times the completed query has attempted to be inserted into
  // the completed queries table. The count is tracked so that the number of attempts can
  // be limited and failing inserts do not retry indefinitely.
  uint8_t insert_attempts_count;

  CompletedQuery(const std::shared_ptr<QueryStateExpanded> query) :
      query(std::move(query)) {
    insert_attempts_count = 0;
  }
};

} // namespace workloadmgmt
} // namespace impala
