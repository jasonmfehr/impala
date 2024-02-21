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

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <list>
#include <tuple>
#include <utility>

#include <gflags/gflags.h>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/strcat.h"
#include "runtime/exec-env.h"
#include "runtime/query-driver.h"
#include "service/client-request-state.h"
#include "service/impala-server.h"
#include "service/query-options.h"
#include "service/query-state-record.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/string-util.h"
#include "util/thread.h"
#include "util/ticker.h"

#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/JniCatalog_types.h"
#include "gen-cpp/Query_types.h"
#include "gen-cpp/TCLIService_types.h"

using namespace impala;
using namespace std;

DEFINE_bool(enable_workload_mgmt, false,
    "Specifies if Impala will automatically write completed queries in the query log "
    "table. If this value is set to true and then later removed, the query log table "
    "will remain intact and accessible.");

DEFINE_string_hidden(query_log_table_name, "impala_query_log", "Specifies the name of "
    "the query log table where completed queries will be stored. This table will be in "
    "the 'sys' database.");

DEFINE_validator(query_log_table_name, [](const char* name, const string& val) {
  if (!val.empty()) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must not be empty.";
  return false;
});

DEFINE_string_hidden(query_log_table_location, "", "Specifies the location of the query "
    "log table where completed queries will be stored.");

DEFINE_validator(query_log_table_location, [](const char* name, const string& val) {
  if (val.find_first_of('\'') != string::npos) {
    LOG(ERROR) << "Invalid value for --" << name << ": must not contain single quotes.";
    return false;
  }

  if (val.find_first_of('"') != string::npos) {
    LOG(ERROR) << "Invalid value for --" << name << ": must not contain double quotes.";
    return false;
  }

  if (val.find_first_of('\n') != string::npos) {
    LOG(ERROR) << "Invalid value for --" << name << ": must not contain newlines.";
    return false;
  }

  return true;
});

DEFINE_int32(query_log_write_interval_s, 300, "Number of seconds to wait between "
    "batches of inserts to the query log table. The countdown to the next write starts "
    "immediately when a write begins, but a new write will not start until the prior "
    "write has completed. Min value is 1. Max value is 14400.");

DEFINE_validator(query_log_write_interval_s, [](const char* name, int32_t val) {
  if (val > 0 && val <= 14400) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be greater than 0 and less "
      "than or equal to 14,400.";
  return false;
});

DEFINE_int32_hidden(query_log_write_timeout_s, 0, "Specifies the query timeout in "
    "seconds for inserts to the query log table. A value less than 1 indicates to use "
    "the same value as the query_log_write_interval_s flag.");

DEFINE_int32(query_log_max_queued, 100000, "Maximum number of records that can be queued "
    "before they are written to the impala query log table. This flag operates "
    "independently of the 'query_log_write_interval_s' flag. If the number of queued "
    "records reaches this value, the records will be written to the query log table no "
    "matter how much time has passed since the last write. The countdown to the next "
    "write (based on the time period defined in the 'query_log_write_interval_s' flag) "
    "is not restarted.");

DEFINE_validator(query_log_max_queued, [](const char* name, int32_t val) {
  if (val >= 0) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be greater than or equal to 0";
  return false;
});

DEFINE_string_hidden(workload_mgmt_user, "impala", "Specifies the user that will be used "
    "to create, update, and insert records into the query log table.");

DEFINE_validator(workload_mgmt_user, [](const char* name, const string& val) {
  if (FLAGS_enable_workload_mgmt && val == "") {
    LOG(ERROR) << "Invalid value for --" << name << ": must be a valid user when "
        "workload management is enabled.";
    return false;
  }

  return true;
});

DEFINE_int32_hidden(query_log_shutdown_deadline_s, 30, "Number of seconds to wait for "
    "the queue of completed queries to be drained to the query log table before timing "
    "out and continuing the shutdown process. The completed queries drain process runs "
    "after the shutdown process completes, thus the max shutdown time is extended by the "
    "value specified in this flag.");

DEFINE_validator(query_log_shutdown_deadline_s, [](const char* name, int32_t val) {
  if (val >= 0) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be a positive value";
  return false;
});

DEFINE_string(cluster_id, "", "Specifies an identifier string that uniquely represents "
    "this cluster. This identifier is included in the query log table if enabled.");

DEFINE_int32_hidden(query_log_max_insert_attempts, 3, "Maximum number of times to "
    "attempt to insert a record into the completed queries table before abandining it.");

DEFINE_validator(query_log_max_insert_attempts, [](const char* name, int32_t val) {
  if (val > 0) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be greater than 1";
  return false;
});

DEFINE_string_hidden(wm_table_props, "", "Comma separated list of additional Iceberg "
    "table properties in the format 'key'='value' to apply when creating the query log "
    "table. Only applies when the table is being created. After table creation, this "
    "property does nothing");

namespace impala {

/// Track the completed queries thread state to enable both force and timed wake up of
/// CompletedQueriesThread.
enum ThreadState {
  NOT_STARTED,
  INITIALIZING,
  RUNNING,
  SHUTDOWN_REQUESTED,
  SHUTDOWN
};

/// Database where the completed queries table is located.
static const string WM_DB = "sys";

/// Fully qualified table name where completed queries are stored.
static const string FQ_TABLE = StrCat(WM_DB, ".", FLAGS_query_log_table_name);

/// Non-values portion of the sql DML to insert records into the completed queries table.
static string INSERT_DML = "";

/// Timeout in seconds for the insert dml.
static const int32_t INSERT_TIMEOUT_S = (FLAGS_query_log_write_timeout_s < 1 ?
    FLAGS_query_log_write_interval_s : FLAGS_query_log_write_timeout_s);

/// Coordinate suspesion and shutdown of the completed queries queue processing thread.
static condition_variable completed_queries_cv_;
static condition_variable completed_queries_shutdown_cv_;

/// Track the state of the thread that processes the completed queries queue. Access to
/// the ThreadState variable must only happen after taking a lock on the associated mutex.
static ThreadState completed_queries_thread_state_ = NOT_STARTED;
static mutex completed_queries_threadstate_mu_;

/// Struct defining the context for generating the sql DML that inserts records into the
/// completed queries table.
struct WMFieldParserContext {
  const QueryStateExpanded* record;
  StringStreamPop& sql;

  WMFieldParserContext(const QueryStateExpanded* rec,
      StringStreamPop& s) : record(rec), sql(s) {}
}; // struct WMFieldParserContext

using WMFieldParser = void (*)(WMFieldParserContext&);

/// Functions used to generate the SQL DML to insert records into the completed queries
/// table.
static list<tuple<string, string, WMFieldParser>> fields_;

// TODO: can this be written with a list_initializer to make it more generic?
static const string EscapeSql(const string& sql) {
  string ret;

  for (auto iter = sql.cbegin(); iter != sql.cend(); iter++) {
    switch(*iter) {
      case '\\':
        ret.push_back('\\');
        ret.push_back('\\');
        break;
      case '\'':
        ret.push_back('\\');
        ret.push_back('\'');
        break;
      case '\n':
        ret.push_back('\\');
        ret.push_back('n');
        break;
      case ';':
        ret.push_back('\\');
        ret.push_back(';');
        break;
      default:
        ret.push_back(*iter);
        break;
    }
  }

  return ret;
} // function EscapeSql

/// Functions to generate individual fields in the completed queries insert sql.
/// Cluster Id (from startup flag)
static void _clusterId(WMFieldParserContext& ctx) {
  ctx.sql << "'" << FLAGS_cluster_id << "'";
}

/// Query Id
static void _queryId(WMFieldParserContext& ctx) {
  ctx.sql << "'" << PrintId(ctx.record->base_state->id) << "'";
}

/// Session Id
static void _sessionId(WMFieldParserContext& ctx) {
  ctx.sql << "'" << PrintId(ctx.record->session_id) << "'";
}

/// Session Type
static void _sessionType(WMFieldParserContext& ctx) {
  ctx.sql << "'" << ctx.record->session_type << "'";
}

/// Hiveserver2 Protocol Version
static void _hs2ProtocolVersion(WMFieldParserContext& ctx) {
  ctx.sql << "'";
  if (ctx.record->session_type == TSessionType::HIVESERVER2) {
    ctx.sql << ctx.record->hiveserver2_protocol_version;
  }
  ctx.sql << "'";
}

/// Effective User
static void _effectiveUser(WMFieldParserContext& ctx) {
  ctx.sql << "'" << ctx.record->base_state->effective_user << "'";
}

/// DB User
static void _dbUser(WMFieldParserContext& ctx) {
  ctx.sql << "'" << ctx.record->db_user_connection << "'";
}

/// Default Db
static void _defaultDb(WMFieldParserContext& ctx) {
  ctx.sql << "'" << ctx.record->base_state->default_db << "'";
}

/// Impala Coordinator
static void _impalaCoordinator(WMFieldParserContext& ctx) {
  ctx.sql << "'" <<TNetworkAddressToString(
      ExecEnv::GetInstance()->configured_backend_address()) << "'";
}

/// Query Status
static void _queryStatus(WMFieldParserContext& ctx) {
  ctx.sql << "'";
  if (ctx.record->base_state->query_status.ok()) {
    ctx.sql << "OK";
  } else {
    ctx.sql << EscapeSql(ctx.record->base_state->query_status.msg().msg());
  }
  ctx.sql << "'";
}

/// Query State
static void _queryState(WMFieldParserContext& ctx) {
  ctx.sql << "'" << ctx.record->base_state->query_state << "'";
}

/// Impala Query End State
static void _impalaQueryEndState(WMFieldParserContext& ctx) {
  ctx.sql << "'" << ctx.record->impala_query_end_state << "'";
}

/// Query Type
static void _queryType(WMFieldParserContext& ctx) {
  ctx.sql << "'";
  if (ctx.record->base_state->stmt_type == TStmtType::SET) {
    ctx.sql << "SET";
  } else if (ctx.record->base_state->stmt_type == TStmtType::QUERY) {
    ctx.sql << "QUERY";
  } else if (ctx.record->base_state->stmt_type == TStmtType::DML) {
    ctx.sql << "DML";
  } else if (ctx.record->base_state->stmt_type == TStmtType::DDL) {
    ctx.sql << "DDL";
  } else if (ctx.record->base_state->stmt_type == TStmtType::EXPLAIN) {
    ctx.sql << "EXPLAIN";
  } else if (ctx.record->base_state->stmt_type == TStmtType::ADMIN_FN) {
    ctx.sql << "ADMIN";
  } else if (ctx.record->base_state->stmt_type == TStmtType::CONVERT) {
    ctx.sql << "CONVERT";
  } else if (ctx.record->base_state->stmt_type == TStmtType::LOAD) {
    ctx.sql << "LOAD";
  } else if (ctx.record->base_state->stmt_type == TStmtType::TESTCASE) {
    ctx.sql << "TESTCASE";
  } else {
    ctx.sql << "N/A";
  }
  ctx.sql << "'";
}

/// Client Network Address
static void _clientNetworkAddress(WMFieldParserContext& ctx) {
  ctx.sql << "'" << TNetworkAddressToString(ctx.record->client_address)
      << "'";
}

/// Query Start Time in UTC
static void _queryStartTime(WMFieldParserContext& ctx) {
  ctx.sql << "UNIX_MICROS_TO_UTC_TIMESTAMP(" << ctx.record->base_state->start_time_us
      << ")";
}

/// Query Duration
static void _queryDuration(WMFieldParserContext& ctx) {
  ctx.sql <<  (ctx.record->base_state->end_time_us -
    ctx.record->base_state->start_time_us) * 1000;
}

/// SQL Statement
static void _stmt(WMFieldParserContext& ctx) {
  ctx.sql << "'" << EscapeSql(ctx.record->redacted_sql) << "'";
}

/// Query Options set by Configuration
static void _queryOptsConfig(WMFieldParserContext& ctx) {
  ctx.sql << "'" << EscapeSql(
      DebugQueryOptions(ctx.record->base_state->query_options)) << "'";
}

/// Resource Pool
static void _pool(WMFieldParserContext& ctx) {
  ctx.sql << "'" << EscapeSql(ctx.record->base_state->resource_pool) << "'";
}

/// Per-host Memory Estimate
static void _per_host_mem_estimate(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->per_host_mem_estimate;
}

/// Dedicated Coordinator Memory Estimate
static void _dedicated_coord_mem_estimate(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->dedicated_coord_mem_estimate;
}

/// Per-Host Fragment Instances
static void _per_host_fragment_instances(WMFieldParserContext& ctx) {
  ctx.sql << "'";

  if (!ctx.record->per_host_state.empty()) {
    for (auto iter = ctx.record->per_host_state.cbegin();
        iter != ctx.record->per_host_state.cend(); iter++) {
      ctx.sql << TNetworkAddressToString(iter->first) << "=" <<
          iter->second.fragment_instances << ',';
    }
    ctx.sql.move_back();
  }

  ctx.sql << "'";
}

/// Backends Count
static void _backends_count(WMFieldParserContext& ctx) {
  if (ctx.record->per_host_state.empty()) {
    ctx.sql << 0;
  } else {
    ctx.sql << ctx.record->per_host_state.size();
  }
}

/// Admission Result
static void _admission_result(WMFieldParserContext& ctx) {
  ctx.sql << "'" << ctx.record->admission_result << "'";
}

/// Cluster Memory Admitted
static void _cluster_memory_admitted(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->base_state->cluster_mem_est;
}

/// Executor Group
static void _executor_group(WMFieldParserContext& ctx) {
  ctx.sql << "'" << ctx.record->executor_group << "'";
}

/// Executor Groups
static void _executor_groups(WMFieldParserContext& ctx) {
  ctx.sql << "'" << EscapeSql(ctx.record->executor_groups) << "'";
}

// Exec Summary (also known as the operator summary)
static void _exec_summary(WMFieldParserContext& ctx) {
  ctx.sql << "'" << EscapeSql(ctx.record->exec_summary) << "'";
}

// Query Plan
static void _plan(WMFieldParserContext& ctx) {
  ctx.sql << "'" << EscapeSql(ctx.record->base_state->plan) << "'";
}

// Number of rows fetched
static void _num_rows_fetched(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->base_state->num_rows_fetched;
}

// Row Materialization Rate
static void _row_materialization_rate(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->row_materialization_rate;
}

// Row Materialization Timer
static void _row_materialization_time(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->row_materialization_time;
}

// Compressed Bytes Spilled to Disk
static void _compressed_bytes_spilled(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->compressed_bytes_spilled;
}

// Helper function to write a single event from the events timeline
static void _write_event(WMFieldParserContext& ctx, const string event_name) {
  int64_t val = 0;

  if (!ctx.record->events_timeline_empty()) {
    for (auto event : ctx.record->EventsTimeline()) {
      if (event.first == event_name) {
        val = event.second;
        break;
      }
    }
  }

  ctx.sql << val;
}

/// Events Timeline Planning Finished
static void _et_planning_finished(WMFieldParserContext& ctx) {
  _write_event(ctx, "planning_finished");
}

/// Events Timeline Submit for Admission
static void _et_submit_for_admission(WMFieldParserContext& ctx) {
  _write_event(ctx, "submit_for_admission");
}

/// Events Timeline Completed Admission
static void _et_completed_admission(WMFieldParserContext& ctx) {
  _write_event(ctx, "completed_admission");
}

/// Events Timeline All Execution Backends Started
static void _et_all_backends_started(WMFieldParserContext& ctx) {
  _write_event(ctx, "all_execution_backends_fragment_instances_started");
}

/// Events Timeline Rows Available
static void _et_rows_available(WMFieldParserContext& ctx) {
  _write_event(ctx, "rows_available");
}

/// Events Timeline First Row Fetched
static void _et_first_row_fetched(WMFieldParserContext& ctx) {
  _write_event(ctx, "first_row_fetched");
}

/// Events Timeline Last Row Fetched
static void _et_last_row_fetched(WMFieldParserContext& ctx) {
  _write_event(ctx, "last_row_fetched");
}

/// Events Timeline Unregister Query
static void _et_unregister_query(WMFieldParserContext& ctx) {
  _write_event(ctx, "unregister_query");
}

/// Read IO Wait Time Total
static void _read_io_wait_total(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->read_io_wait_time_total;
}

/// Read IO Wait Time Total
static void _read_io_wait_mean(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->read_io_wait_time_mean;
}

/// Bytes Read from the Data Cache Total
static void _bytes_read_cache_total(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->bytes_read_cache_total;
}

/// Bytes Read Total
static void _bytes_read_total(WMFieldParserContext& ctx) {
  ctx.sql << ctx.record->bytes_read_total;
}

/// Per-Node Peak Memory Usage Min
static void _pernode_peak_mem_min(WMFieldParserContext& ctx) {
  auto min_elem = min_element(ctx.record->per_host_state.cbegin(),
      ctx.record->per_host_state.cend(), PerHostPeakMemoryComparator);

  if (LIKELY(min_elem != ctx.record->per_host_state.cend())) {
    ctx.sql << min_elem->second.peak_memory_usage;
  } else {
    ctx.sql << 0;
  }
}

/// Per-Node Peak Memory Usage Max
static void _pernode_peak_mem_max(WMFieldParserContext& ctx) {
  auto min_elem = max_element(ctx.record->per_host_state.cbegin(),
      ctx.record->per_host_state.cend(), PerHostPeakMemoryComparator);

  if (UNLIKELY(min_elem == ctx.record->per_host_state.cend())) {
    ctx.sql << 0;
  } else {
    ctx.sql << min_elem->second.peak_memory_usage;
  }
}

/// Per-Node Peak Memory Usage Mean
static void _pernode_peak_mem_mean(WMFieldParserContext& ctx) {
  int64_t calc_mean = 0;

  if(LIKELY(!ctx.record->per_host_state.empty())) {
    for (auto host : ctx.record->per_host_state) {
      calc_mean += host.second.peak_memory_usage;
    }

    calc_mean = calc_mean / ctx.record->per_host_state.size();
  }

  ctx.sql << calc_mean;
}
/// End of functions to generate individual fields in the completed queries insert sql.

/// Helper function to build a std::tuple to be stored in the fields_ list.
static tuple<string, string, WMFieldParser> MakeTuple(string field_name,
    WMFieldParser parser_func, string column_type = "STRING") {
  return make_tuple<string, string, WMFieldParser>(move(field_name),
      move(column_type), move(parser_func));
}
/// Builds the list of field parsers. Each field has its own parser that defines the field
/// name, field SQL type, and function to produce the value from a QueryStateExpanded
/// object.
///
/// Note: While Field order is not meaningful, do not rearrange the order of these fields
///       to ensure that every Impala has the same structure for the completed queries
///       table. If new columns are needed, add them on the end. If a column is no longer
///       useful, blank it out instead of deleting the column.
static void BuildFieldParsers() {
  fields_.push_back(MakeTuple("cluster_id", _clusterId));
  fields_.push_back(MakeTuple("query_id", _queryId));
  fields_.push_back(MakeTuple("session_id", _sessionId));
  fields_.push_back(MakeTuple("session_type", _sessionType));
  fields_.push_back(MakeTuple("hiveserver2_protocol_version", _hs2ProtocolVersion));
  fields_.push_back(MakeTuple("db_user", _effectiveUser));
  fields_.push_back(MakeTuple("db_user_connection", _dbUser));
  fields_.push_back(MakeTuple("db_name", _defaultDb));
  fields_.push_back(MakeTuple("impala_coordinator", _impalaCoordinator));
  fields_.push_back(MakeTuple("query_status", _queryStatus));
  fields_.push_back(MakeTuple("query_state", _queryState));
  fields_.push_back(MakeTuple("impala_query_end_state", _impalaQueryEndState));
  fields_.push_back(MakeTuple("query_type", _queryType));
  fields_.push_back(MakeTuple("network_address", _clientNetworkAddress));
  fields_.push_back(MakeTuple("start_time_utc", _queryStartTime, "TIMESTAMP"));
  fields_.push_back(MakeTuple("total_time_ns", _queryDuration, "BIGINT"));
  fields_.push_back(MakeTuple("sql", _stmt));
  fields_.push_back(MakeTuple("query_opts_config", _queryOptsConfig));
  fields_.push_back(MakeTuple("resource_pool", _pool));
  fields_.push_back(MakeTuple("per_host_mem_estimate", _per_host_mem_estimate, "BIGINT"));
  fields_.push_back(MakeTuple(
      "dedicated_coord_mem_estimate", _dedicated_coord_mem_estimate, "BIGINT"));
  fields_.push_back(MakeTuple(
      "per_host_fragment_instances", _per_host_fragment_instances));
  fields_.push_back(MakeTuple("backends_count", _backends_count, "INTEGER"));
  fields_.push_back(MakeTuple("admission_result", _admission_result));
  fields_.push_back(MakeTuple(
      "cluster_memory_admitted", _cluster_memory_admitted, "BIGINT"));
  fields_.push_back(MakeTuple("executor_group", _executor_group));
  fields_.push_back(MakeTuple("executor_groups", _executor_groups));
  fields_.push_back(MakeTuple("exec_summary", _exec_summary));
  fields_.push_back(MakeTuple("plan", _plan));
  fields_.push_back(MakeTuple("num_rows_fetched", _num_rows_fetched, "BIGINT"));
  fields_.push_back(MakeTuple(
      "row_materialization_bytes_per_sec", _row_materialization_rate, "BIGINT"));
  fields_.push_back(MakeTuple(
      "row_materialization_time_ns", _row_materialization_time, "BIGINT"));
  fields_.push_back(MakeTuple(
      "compressed_bytes_spilled", _compressed_bytes_spilled, "BIGINT"));
  fields_.push_back(MakeTuple(
      "event_planning_finished", _et_planning_finished, "BIGINT"));
  fields_.push_back(MakeTuple(
      "event_submit_for_admission", _et_submit_for_admission, "BIGINT"));
fields_.push_back(MakeTuple(
      "event_completed_admission", _et_completed_admission, "BIGINT"));
fields_.push_back(MakeTuple(
      "event_all_backends_started", _et_all_backends_started, "BIGINT"));
fields_.push_back(MakeTuple(
      "event_rows_available", _et_rows_available, "BIGINT"));
fields_.push_back(MakeTuple(
      "event_first_row_fetched", _et_first_row_fetched, "BIGINT"));
fields_.push_back(MakeTuple(
      "event_last_row_fetched", _et_last_row_fetched, "BIGINT"));
fields_.push_back(MakeTuple(
      "event_unregister_query", _et_unregister_query, "BIGINT"));
fields_.push_back(MakeTuple("read_io_wait_total_ns", _read_io_wait_total, "BIGINT"));
fields_.push_back(MakeTuple("read_io_wait_mean_ns", _read_io_wait_mean, "BIGINT"));
fields_.push_back(MakeTuple("bytes_read_cache_total", _bytes_read_cache_total, "BIGINT"));
fields_.push_back(MakeTuple("bytes_read_total", _bytes_read_total, "BIGINT"));
fields_.push_back(MakeTuple("pernode_peak_mem_min", _pernode_peak_mem_min, "BIGINT"));
fields_.push_back(MakeTuple("pernode_peak_mem_max", _pernode_peak_mem_max, "BIGINT"));
fields_.push_back(MakeTuple("pernode_peak_mem_mean", _pernode_peak_mem_mean, "BIGINT"));

} // function BuildFieldParsers

/// Generates the first portion of the DML that inserts records into the completed queries
/// table.  This portion of the statement is constant and thus is only generated once.
static void BuildInsertDML() {
  StringStreamPop fields;

  fields << "INSERT INTO " << FQ_TABLE << "(";
  for (auto iter = fields_.cbegin(); iter != fields_.cend(); iter++) {
    fields << get<0>(*iter) << ",";
  }
  fields.move_back();

  fields << ") VALUES ";

  INSERT_DML = fields.str();
} // function BuildInsertDML

/// Helper function to determine if the maximum number of queued completed queries has
/// been exceeded.
///
/// Return:
///   `true`  There is a max limit on the number of queued completed queries and that
///           limit has been exceeded.
///   `false` Either there is no max number of queued completed queries or there is a
///           limit that has not been exceeded.
static inline bool MaxRecordsExceeded(size_t record_count) noexcept {
  return FLAGS_query_log_max_queued > 0 && record_count > FLAGS_query_log_max_queued;
} // MaxRecordsExceeded

/// Helper function to set up the completed queries table by generating and executing the
/// DML statements that create the database and table.
static const Status InitDB(InternalServer* server) {
  {
    lock_guard<mutex> l(completed_queries_threadstate_mu_);
    completed_queries_thread_state_ = INITIALIZING;
  }

  TQueryOptions query_opts;
  query_opts.__set_timezone("UTC");
  RETURN_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      StrCat("create database if not exists ", WM_DB, " comment "
      "'System database for Impala introspection'"), query_opts, false));

  StringStreamPop create_table_sql;
  create_table_sql << "CREATE TABLE IF NOT EXISTS " << FQ_TABLE << "(";

  for (auto iter = fields_.cbegin(); iter != fields_.cend(); iter++) {
    create_table_sql << get<0>(*iter) << " " << get<1>(*iter) << ",";
  }
  create_table_sql.move_back();

  create_table_sql << ") PARTITIONED BY SPEC(identity(cluster_id), HOUR(start_time_utc)) "
      << "STORED AS iceberg ";

  if (!FLAGS_query_log_table_location.empty()) {
    create_table_sql << "LOCATION '" << FLAGS_query_log_table_location << "' ";
  }

  create_table_sql << "TBLPROPERTIES ('schema_version'='1.0.0','format-version'='2'";

  if (!FLAGS_wm_table_props.empty()) {
    create_table_sql << "," << FLAGS_wm_table_props;
  }

  create_table_sql << ")";

  query_opts.__set_sync_ddl(true);
  RETURN_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      create_table_sql.str(), query_opts, false));

  LOG(INFO) << "Completed query log initialization. storage_type=\""
      << FLAGS_enable_workload_mgmt  << "\" write_interval=\"" <<
      FLAGS_query_log_write_interval_s << "s\"";

  return Status::OK();
} // function InitDB

Status ImpalaServer::InitWorkloadManagement() {
  if (FLAGS_enable_workload_mgmt) {
    return Thread::Create("impala-server", "completed-queries",
      bind<void>(&ImpalaServer::CompletedQueriesThread, this),
      &completed_queries_thread_);
  }

  return Status::OK();
} // ImpalaServer::InitWorkloadManagement

void ImpalaServer::ShutdownWorkloadManagement() {
  unique_lock<mutex> l(completed_queries_threadstate_mu_);
  if (completed_queries_thread_state_ == RUNNING) {
    completed_queries_thread_state_ = SHUTDOWN_REQUESTED;
    completed_queries_cv_.notify_all();
    completed_queries_shutdown_cv_.wait_for(l,
        chrono::seconds(FLAGS_query_log_shutdown_deadline_s),
        []{ return completed_queries_thread_state_ == SHUTDOWN; });
  }
} // ImpalaServer::ShutdownWorkloadManagement

void ImpalaServer::EnqueueCompletedQuery(const QueryHandle& query_handle,
    const shared_ptr<QueryStateRecord> qs_rec) {

  // Do not enqueue queries that are not written to the table or if workload management is
  // not enabled.
  if (query_handle->stmt_type() == TStmtType::SET
      || !query_handle.query_driver()->IncludedInQueryLog()
      || !FLAGS_enable_workload_mgmt){
    return;
  }

  // Do not enqueue use and show ddl queries. This check is separate because combining it
  // with the previous check resulted in very confusing code that had duplication.
  if (query_handle->stmt_type() == TStmtType::DDL) {
    switch (query_handle->catalog_op_type()) {
      case TCatalogOpType::SHOW_TABLES:
      case TCatalogOpType::SHOW_DBS:
      case TCatalogOpType::SHOW_STATS:
      case TCatalogOpType::USE:
      case TCatalogOpType::SHOW_FUNCTIONS:
      case TCatalogOpType::SHOW_CREATE_TABLE:
      case TCatalogOpType::SHOW_DATA_SRCS:
      case TCatalogOpType::SHOW_ROLES:
      case TCatalogOpType::SHOW_GRANT_PRINCIPAL:
      case TCatalogOpType::SHOW_FILES:
      case TCatalogOpType::SHOW_CREATE_FUNCTION:
      case TCatalogOpType::SHOW_VIEWS:
      case TCatalogOpType::DESCRIBE_TABLE:
      case TCatalogOpType::DESCRIBE_DB:
      case TCatalogOpType::DESCRIBE_HISTORY:
        return;
      case TCatalogOpType::RESET_METADATA:
      case TCatalogOpType::DDL:
        break;
      default:
        LOG(FATAL) << "unknown ddl type: " << to_string(query_handle->catalog_op_type());
    }
  }

  shared_ptr<QueryStateExpanded> exp_rec = make_shared<QueryStateExpanded>(*query_handle,
      move(qs_rec));

  {
    lock_guard<mutex> l(completed_queries_lock_);
    completed_queries_.emplace_back(make_pair(move(exp_rec), 0));
    ImpaladMetrics::COMPLETED_QUERIES_QUEUED->Increment(1L);

    if (MaxRecordsExceeded(completed_queries_.size())) {
      completed_queries_cv_.notify_all();
    }
  }
} // ImpalaServer::EnqueueCompletedQuery

void ImpalaServer::CompletedQueriesThread() {
  BuildFieldParsers();

  // The initialization code only works when run in a separate thread for reasons unknown.
  ABORT_IF_ERROR(InitDB(internal_server_.get()));

  BuildInsertDML();

  completed_queries_ready_ = make_shared<bool>();

  {
    lock_guard<mutex> l(completed_queries_threadstate_mu_);
    completed_queries_thread_state_ = RUNNING;

    completed_queries_ticker_ = make_unique<TickerSB>(
        chrono::seconds(FLAGS_query_log_write_interval_s), completed_queries_cv_,
        completed_queries_lock_, completed_queries_ready_);
    ABORT_IF_ERROR(completed_queries_ticker_->Start("impala-server",
        "completed-queries-ticker"));
  }

  while (true) {
    // Exit this thread if a shutdown was initiated.
    {
      lock_guard<mutex> l(completed_queries_threadstate_mu_);
      if (completed_queries_thread_state_ == SHUTDOWN_REQUESTED) {
        completed_queries_thread_state_ = SHUTDOWN;
        completed_queries_shutdown_cv_.notify_all();
        return;
      }
    }

    // Sleep this thread until it is time to process queued completed queries. During the
    // wait, the completed_queries_lock_ is only locked while calling the lambda function
    // predicate. After waking up, the completed_queries_lock_ will be locked.
    unique_lock<mutex> l(completed_queries_lock_);
    completed_queries_cv_.wait(l,
        [this]{
          lock_guard<mutex> l2(completed_queries_threadstate_mu_);
          // To guard against spurious wakeups, this predicate ensures there are completed
          // queries queued up before waking up the thread.
          return (*completed_queries_ready_ && !completed_queries_.empty()) ||
              MaxRecordsExceeded(completed_queries_.size()) ||
              completed_queries_thread_state_ == SHUTDOWN_REQUESTED;
        });
    *completed_queries_ready_ = false;

    // transfer all currently queued completed queries to another list for processing
    // so that the completed queries queue is not blocked while creating and executing the
    // DML to insert into the query log table
    if (!completed_queries_.empty()) {
      if (MaxRecordsExceeded(completed_queries_.size())) {
        ImpaladMetrics::COMPLETED_QUERIES_MAX_RECORDS_WRITES->Increment(1L);
      } else {
        ImpaladMetrics::COMPLETED_QUERIES_SCHEDULED_WRITES->Increment(1L);
      }

      // Copy all completed queries to a temporary list so that inserts to the
      // completed_queries list are not blocked while generating and running an insert
      // SQL statement for the completed queries.
      list<completed_query_entry> queries_to_insert;
      queries_to_insert.splice(queries_to_insert.cend(), completed_queries_);
      completed_queries_lock_.unlock();

      string sql;

      for (auto iter = queries_to_insert.begin(); iter != queries_to_insert.end();
          iter++) {
        if (iter->second >= FLAGS_query_log_max_insert_attempts) {
          LOG(ERROR) << "could not write completed query table=\"" << FQ_TABLE <<
              "\" query_id=\"" << PrintId(iter->first->base_state->id) << "\"";
          iter = queries_to_insert.erase(iter);
          ImpaladMetrics::COMPLETED_QUERIES_QUEUED->Increment(-1);
          continue;
        }

        iter->second += 1;
        StrAppend(&sql, QueryStateToSql(iter->first.get()), ",");
      }

      // In the case where queries_to_insert only contains records that have exceeded
      // the max insert attempts, sql will be empty.
      if (!sql.empty()) {
        // Remove the last comma.
        sql.pop_back();

        TQueryOptions query_opts;
        TUniqueId tmp_query_id;

        query_opts.__set_timezone("UTC");
        query_opts.__set_query_timeout_s(INSERT_TIMEOUT_S);
        const Status ret_status = internal_server_->ExecuteIgnoreResults(
            FLAGS_workload_mgmt_user, StrCat(INSERT_DML, sql), query_opts, false,
            &tmp_query_id);

        if (ret_status.ok()) {
          LOG(INFO) << "wrote completed queries table=\"" << FQ_TABLE << "\" "
              "record_count=\"" << queries_to_insert.size() << "\"";
          ImpaladMetrics::COMPLETED_QUERIES_QUEUED->Increment(
              queries_to_insert.size() * -1);
          ImpaladMetrics::COMPLETED_QUERIES_WRITTEN->Increment(
              queries_to_insert.size());
        } else {
          LOG(WARNING) << "failed to write completed queries table=\"" << FQ_TABLE <<
              "\" record_count=\"" << queries_to_insert.size() << "\"";
            LOG(WARNING) << ret_status.GetDetail();
          ImpaladMetrics::COMPLETED_QUERIES_FAIL->Increment(queries_to_insert.size());
          completed_queries_lock_.lock();
          completed_queries_.splice(
              completed_queries_.cend(), queries_to_insert);
          completed_queries_lock_.unlock();
        }
      }
    }
  }
} // ImpalaServer::CompletedQueriesThread

string ImpalaServer::QueryStateToSql(const QueryStateExpanded* rec) const {
  DCHECK(rec != nullptr);
  StringStreamPop sql;
  WMFieldParserContext ctx(rec, sql);

  sql << "(";

  for (auto iter = fields_.cbegin(); iter != fields_.cend(); iter++) {
    get<2>(*iter)(ctx);
    sql << ",";
  }

  sql.move_back();
  sql << ")";

  return sql.str();
} // ImpalaServer::QueryStateToSql

} // namespace impala
