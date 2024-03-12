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

/// This file contains the code for the initialization process for workload management.
/// The init process handles:
///   1. determining which coordinator will perform the init process
///   2. starting the workload management thread which creates/updates the necessary
///      database tables and runs the completed queries processing loop
///
/// The initialization process runs in a coordinated manner between the
/// WorkloadManagementTopicUpdate and InitWorkloadManagement functions to ensure that
/// coordinator startup is not blocked. This process is managed through the init_ctx
/// variable.
///
/// The init process starts by each coordinator generating a random id for itself and
/// placing that id onto the workload management statestore topic. Then, the topic is read
/// and the coordinator who's id appears first in the topic chooses itself as the leader.
/// Other coordinators go into a waiting mode. The leader runs the sql statements to
/// create and update the workload management database tables. After this processing
/// completes, the leader places a completed message onto the workload management
/// statestore topic and starts its completed query processing loop. The other non-leader
/// coordinators pick up the completed message from the statestore topic, run
/// initialization of in-memory variables, and start their completed query processing
/// loop. The initialization process is handled in the same thread that runs the completed
/// query processing loop to ensure that coordinator startup is not blocked.
///
/// All coordinators release the resources managed by the init_ctx unique_ptr before
/// starting their processing loops.


#include "service/workload-management-init.h"

#include <mutex>
#include <optional>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gutil/strings/strcat.h>

#include "common/atomic.h"
#include "common/logging.h"
#include "common/status.h"
#include "gen-cpp/CatalogObjects_constants.h"
#include "gen-cpp/TCLIService_types.h"
#include "gen-cpp/Types_types.h"
#include "kudu/util/version_util.h"
#include "service/impala-server.h"
#include "service/workload-management.h"
#include "statestore/statestore-subscriber.h"
#include "util/debug-util.h"

using namespace std;
using namespace impala;
using namespace impala::workload_management;
using boost::algorithm::starts_with;
using boost::algorithm::trim_copy;
using boost::algorithm::to_lower;
using kudu::Version;
using kudu::ParseVersion;

DECLARE_int32(query_log_write_interval_s);
DECLARE_int32(query_log_write_timeout_s);
DECLARE_string(debug_actions);
DECLARE_string(query_log_request_pool);
DECLARE_string(query_log_table_location);
DECLARE_string(query_log_table_name);
DECLARE_string(query_log_table_props);
DECLARE_string(workload_mgmt_user);
DECLARE_string(workload_mgmt_schema_version);

namespace impala {

/// Name of the database where all workload management tables will be stored.
static const string DB = "sys";

/// Tracks the initialization process from start to finish. This pointer is reset to null
/// after the init process completes.
static unique_ptr<InitContext> init_ctx = make_unique<InitContext>();

/// Sets up the sys database generating and executing the necessary DML statements.
static void _setupDb(InternalServer* server,
    InternalServer::QueryOptionMap& insert_query_opts) {
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";
  ABORT_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      StrCat("CREATE DATABASE IF NOT EXISTS ", DB, " COMMENT "
      "'System database for Impala introspection'"), insert_query_opts, false));
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";
} // function _setupDb

/// Appends all relevant fields to a create or alter table sql statement.
static void _appendFields(StringStreamPop& stream,
    std::function<bool(const FieldDefinition& item)> p) {
  bool match = false;

  for (const auto& field : FIELD_DEFINITIONS) {
   if (p(field)) {
    match = true;
    stream << field.FormattedColName() << " " << field.db_column_type;

      if (field.db_column_type == TPrimitiveType::DECIMAL) {
        stream << "(" << field.precision << "," << field.scale << ")";
      }

      stream << ",";
   }
  }

  DCHECK_EQ(match, true);
  stream.move_back();
} // function _appendFields

/// Sets up the query table by generating and executing the necessary DML statements.
static void _setupTable(InternalServer* server, const string& table_name,
    InternalServer::QueryOptionMap& insert_query_opts, const Version& target_version,
    bool is_system_table = false) {
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";

  StringStreamPop create_table_sql;
  create_table_sql << "CREATE ";
  // System tables do not have anything to purge, and must not be managed tables.
  if (is_system_table) create_table_sql << "EXTERNAL ";
  create_table_sql << "TABLE IF NOT EXISTS " << table_name << "(";

  _appendFields(create_table_sql, [target_version](const FieldDefinition& f){
      return f.schema_version <= target_version;});

  create_table_sql << ") ";

  if (!is_system_table) {
    create_table_sql << "PARTITIONED BY SPEC(identity(cluster_id), HOUR(start_time_utc)) "
        << "STORED AS iceberg ";

    if (!FLAGS_query_log_table_location.empty()) {
      create_table_sql << "LOCATION '" << FLAGS_query_log_table_location << "' ";
    }
  }

  create_table_sql << "TBLPROPERTIES ('schema_version'='" << target_version.ToString()
      << "','format-version'='2'";

  if (is_system_table) {
    create_table_sql << ",'"
                     << g_CatalogObjects_constants.TBL_PROP_SYSTEM_TABLE <<"'='true'";
  } else if (!FLAGS_query_log_table_props.empty()) {
    create_table_sql << "," << FLAGS_query_log_table_props;
  }

  create_table_sql << ")";

  VLOG(2) << "Creating workload management table '" << table_name
      << "' on schema version '" << target_version.ToString() << "'";
  ABORT_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      create_table_sql.str(), insert_query_opts, false));

  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";

  LOG(INFO) << "Completed " << table_name << " initialization. write_interval=\"" <<
      FLAGS_query_log_write_interval_s << "s\"";
} // function _setupTable

/// Upgrades a table by running alter table statements.
static void _upgradeTable(InternalServer* server, const string& table_name,
    InternalServer::QueryOptionMap& insert_query_opts, const Version& current_version,
    const Version& target_version) {

  DCHECK_NE(current_version, target_version);

  StringStreamPop cols_to_add;

  cols_to_add << "ALTER TABLE " << table_name << " ADD IF NOT EXISTS COLUMNS(";
  _appendFields(cols_to_add, [current_version, target_version](const FieldDefinition& f){
      return f.schema_version > current_version && f.schema_version <= target_version;});
  cols_to_add << ")";

  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";

  VLOG(2) << "Upgrading workload management table '" << table_name << "' from schema "
      << "version '" << current_version.ToString() << "' to '"
      << target_version.ToString() << "'";

  for (const string& sql : array<string, 2>{
    std::move(cols_to_add.str()),
    StrCat("ALTER TABLE ", table_name, " SET TBLPROPERTIES ('schema_version'='",
        target_version.ToString(), "')")
}) {
    ABORT_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
        sql, insert_query_opts, false));
  }

  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";
} // function _upgradeTable

static string _retrieveSchemaVersion(InternalServer* server, const string table_name,
     const InternalServer::QueryOptionMap& insert_query_opts) {

  vector<apache::hive::service::cli::thrift::TRow> query_results;

  const Status describe_table = server->ExecuteAndFetchAllHS2(FLAGS_workload_mgmt_user,
      StrCat("DESCRIBE EXTENDED ", table_name), query_results, insert_query_opts, false);

  // If an error, ignore the error and run as if workload management has never
  // executed. Since all the DDLs use the "if not exists" clause, extra runs of the
  // DDLs will not cause any harm.
  if (describe_table.ok()) {
    for(auto& res : query_results) {
      if (starts_with(res.colVals[1].stringVal.value, "schema_version") ){
        return trim_copy(res.colVals[2].stringVal.value);
      }
    }
  }

  return "";
} // _retrieveSchemaVersion

void ImpalaServer::InitWorkloadManagement() {
  // Fully qualified table name based on startup flags.
  const string log_table_name = StrCat(DB, ".", FLAGS_query_log_table_name);

  // Verify FIELD_DEFINITIONS includes all QueryTableColumns.
  DCHECK_EQ(_TQueryTableColumn_VALUES_TO_NAMES.size(), FIELD_DEFINITIONS.size());
  for (const auto& field : FIELD_DEFINITIONS) {
    // Verify all fields match their column position.
    DCHECK_EQ(FIELD_DEFINITIONS[field.db_column].db_column, field.db_column);
  }

  // Ensure a valid schema version was specified on the command line flag.
  Version target_schema_version;
  if (!ParseVersion(FLAGS_workload_mgmt_schema_version,
      &target_schema_version).ok()) {
    ABORT_WITH_ERROR(StrCat("Invalid workload management schema version '",
        FLAGS_workload_mgmt_schema_version, "'"));
  }
  VLOG(2) << "Target workload management schema version is '"
      << target_schema_version.ToString() << "'";

  if (target_schema_version != VERSION_1_0_0 && target_schema_version != VERSION_1_1_0) {
    ABORT_WITH_ERROR(StrCat("Workload management schema version '",
        FLAGS_workload_mgmt_schema_version, "' does not match any valid version"));
  }

  // Setup default query options that will be provided on all queries that insert rows
  // into the completed queries table.
  InternalServer::QueryOptionMap insert_query_opts;

  insert_query_opts[TImpalaQueryOptions::TIMEZONE] = "UTC";
  insert_query_opts[TImpalaQueryOptions::QUERY_TIMEOUT_S] = std::to_string(
      FLAGS_query_log_write_timeout_s < 1 ?
      FLAGS_query_log_write_interval_s : FLAGS_query_log_write_timeout_s);
  if (!FLAGS_query_log_request_pool.empty()) {
    insert_query_opts[TImpalaQueryOptions::REQUEST_POOL] = FLAGS_query_log_request_pool;
  }

  {
    unique_lock<mutex> init_lock(init_ctx->guard);

    // Wait until all necessary preconditions are met.
    wm_init_cv_.wait(init_lock, [this, &init_lock] {
      // Wait until the internal_server is fully initialization since it will be used to
      // run DML statements.
      if (!internal_server_.get()) return false;

      return init_ctx->state == LEADER_INIT_RUNNING
          || init_ctx->state == NONLEADER_INIT_RUNNING;
    });

    {
      lock_guard<mutex> l(completed_queries_threadstate_mu_);
      completed_queries_thread_state_ = INITIALIZING;
    }

    if (init_ctx->state == LEADER_INIT_RUNNING) {
      init_lock.unlock();

      string wm_schema_version;
      Version parsed_actual_schema_version;

      // Create and/or update the completed queries table if needed.
      wm_schema_version = _retrieveSchemaVersion(internal_server_.get(), log_table_name,
          insert_query_opts);
      VLOG(2) << "Actual current workload management schema version of the '"
          << log_table_name << "' table is '" << wm_schema_version << "'";

      if(wm_schema_version != target_schema_version.ToString()) {
        if (wm_schema_version.empty()) {
          // First time setting up workload management.
          // Setup the sys database.
          _setupDb(internal_server_.get(), insert_query_opts);

          // Create the query log table and upgrade it to the target schema version.
          _setupTable(internal_server_.get(), log_table_name, insert_query_opts,
              VERSION_1_0_0);

          parsed_actual_schema_version = VERSION_1_0_0;
        } else {
          if(!ParseVersion(wm_schema_version, &parsed_actual_schema_version).ok()) {
            ABORT_WITH_ERROR(StrCat("Invalid actual workload management schema version '",
                wm_schema_version, "'"));
          }
        }

        if (target_schema_version < parsed_actual_schema_version) {
          ABORT_WITH_ERROR(StrCat("Target schema version '",
              target_schema_version.ToString(), " of the '", log_table_name, "' table is "
              "lower than the actual schema version '", wm_schema_version,
              "'. Downgrades are not supported. the target schema version must be "
              "greater than or equal to the actual schema version"));
        }

        // Handle the schema upgrade scenario.
        if (target_schema_version > parsed_actual_schema_version) {
          // Upgrade the query log table.
          _upgradeTable(internal_server_.get(), log_table_name, insert_query_opts,
              parsed_actual_schema_version, target_schema_version);
        }
      }

      // Create and/or update the live queries table if needed.
      // Determine the live queries table name.
      string live_table_name = StrCat(DB, ".",
          to_string(TSystemTableName::IMPALA_QUERY_LIVE));
      to_lower(live_table_name);

      wm_schema_version = _retrieveSchemaVersion(internal_server_.get(), live_table_name,
          insert_query_opts);
      VLOG(2) << "Actual current workload management schema version of the '"
          << live_table_name << "' table is '" << wm_schema_version << "'";

      if (wm_schema_version != target_schema_version.ToString()) {
        if (wm_schema_version.empty()) {
          // First time setting up workload management.
          // Create the query live table on the target schema version.
          _setupTable(internal_server_.get(), live_table_name, insert_query_opts,
              target_schema_version, true);
        } else {
          if(!ParseVersion(wm_schema_version, &parsed_actual_schema_version).ok()) {
            ABORT_WITH_ERROR(StrCat("Invalid actual workload management schema version '",
                wm_schema_version, "'"));
          }

          if (target_schema_version < parsed_actual_schema_version) {
            ABORT_WITH_ERROR(StrCat("Target schema version '",
                target_schema_version.ToString(), " of the '", live_table_name,
                "' table is lower than the actual schema version '", wm_schema_version,
                "'. Downgrades are not supported. the target schema version must be "
                "greater than or equal to the actual schema version"));
          }

          // Handle the schema upgrade scenario.
          if (target_schema_version > parsed_actual_schema_version) {
            // Drop the live query table to re-create it.
            VLOG(2) << "Dropping '" << live_table_name << "' so it can be upgraded to "
                << "the latest schema version '" << target_schema_version.ToString()
                << "'";
            ABORT_IF_ERROR(internal_server_->ExecuteIgnoreResults(
                FLAGS_workload_mgmt_user, StrCat("DROP TABLE IF EXISTS ",
                live_table_name), insert_query_opts, false));

            // Create the live query table on the target schema version.
            _setupTable(internal_server_.get(), live_table_name, insert_query_opts,
                target_schema_version, true);
          }
        }
      }

      init_lock.lock();
      init_ctx->state = LEADER_INIT_DONE;
    } else {
      init_ctx->state = DONE;
    }
  }

  LOG(INFO) << "Completed workload management initialization";
  WorkloadManagementWorker(insert_query_opts, log_table_name);
} // ImpalaServer::InitWorkloadManagement

static const string INIT_START_MSG = "wm_init_start";
static const string INIT_DONE_MSG = "wm_init_done";

static TTopicDelta build_topic_delta(string key, string value, string topic_name) {
  TTopicDelta my_delta;
  my_delta.topic_name = topic_name;

  TTopicItem item;
  item.key = key;
  item.value = value;

  my_delta.topic_entries.push_back(item);

  return my_delta;
} // build_topic_delta

void ImpalaServer::WorkloadManagementTopicUpdate(
    const StatestoreSubscriber::TopicDeltaMap& state,
    std::vector<TTopicDelta>* topic_updates) {

  // Once initialization is done and the init_ctx has been released, there is no need to
  // process any more messages.
  if (!init_ctx.get()) {
    return;
  }

  DebugActionNoFail(FLAGS_debug_actions, "WORKLOAD_MGMT_INIT");

  unique_lock<mutex> l(init_ctx->guard);

  // When initialization has completed, release resources no longer needed.
  if (init_ctx->state == DONE) {
    l.unlock();
    l.release();
    init_ctx.reset(nullptr);

    return;
  }

  // Assign a random id to each daemon when first initialized and start the workload
  // management thread.
  if (init_ctx->state == NOT_RUNNING) {
    init_ctx->id = PrintId(RandomUniqueID());
    init_ctx->state = DETERMINE_LEADER;
    topic_updates->push_back(build_topic_delta(init_ctx->id, INIT_START_MSG,
        wm_topic_name_));

    ABORT_IF_ERROR(Thread::Create("impala-server", "completed-queries",
        bind<void>(&ImpalaServer::InitWorkloadManagement, this),
        &completed_queries_thread_));
  }

  // Lead coordinator notifies the rest of the cluster once it has finished workload
  // management initialization.
  if(init_ctx->state == LEADER_INIT_DONE) {
    topic_updates->push_back(build_topic_delta(init_ctx->id, INIT_DONE_MSG,
        wm_topic_name_));
    init_ctx->state = DONE;

    return;
  }

  // First look to see if the topic we're interested in has an update.
  auto topic = state.find(wm_topic_name_);

  // Ignore spurious messages.
  if (topic == state.end()) return;

  const TTopicDelta& update = topic->second;

  for (auto& e : update.topic_entries) {
    if (e.value == INIT_DONE_MSG) {
      // The lead coordinator has sent the message indicating it has completed its work
      // and other non-lead coordinators can continue. If the current daemon is not a lead
      // coordinator, perform it's limited init and start the completed queries
      // processing loop.
      if (init_ctx->state == DETERMINE_LEADER
          || init_ctx->state == NONLEADER_INIT_WAITING) {
        // In the case where this coordinator is joining a cluster that has already had
        // workload management initialized, the state will be DETERMINE_LEADER.
        init_ctx->state = NONLEADER_INIT_RUNNING;

        // Start the workload management thread on non-lead coordinators.
        VLOG(2) << "Starting workload management thread without setting up "
            << "the workload management database tables";
        l.unlock();
        wm_init_cv_.notify_all();
      }
    } else if (e.value == INIT_START_MSG) {
      if (init_ctx->state == DETERMINE_LEADER) {
        if (init_ctx->id == e.key) {
          VLOG(2) << "Starting workload management initialization that will set up the "
            << "workload management database tables";
          init_ctx->state = LEADER_INIT_RUNNING;
          l.unlock();
          wm_init_cv_.notify_all();
        } else {
          init_ctx->state = NONLEADER_INIT_WAITING;
          VLOG(2) << "Waiting for another coordinator to finish the workload management "
            << "database table setup";
        }
      }
    } else {
      DCHECK(false) << "Unknown statestore message with key='" << e.key << "' and value='"
          << e.value << "'";
    }
  }
} // ImpalaServer::WorkloadManagementTopicUpdate

} // namespace impala
