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

#include "service/workload-management.h"

#include <mutex>
#include <optional>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gutil/strings/strcat.h>

#include "common/atomic.h"
#include "common/logging.h"
#include "common/status.h"
#include "gen-cpp/CatalogObjects_constants.h"
#include "gen-cpp/Types_types.h"
#include "service/impala-server.h"
#include "statestore/statestore-subscriber.h"
#include "util/debug-util.h"

using namespace std;
using namespace impala::workload_management;

DECLARE_int32(query_log_write_interval_s);
DECLARE_int32(query_log_write_timeout_s);
DECLARE_string(query_log_request_pool);
DECLARE_string(query_log_table_location);
DECLARE_string(query_log_table_name);
DECLARE_string(query_log_table_props);
DECLARE_string(workload_mgmt_user);

namespace impala {

/// Name of the database where all workload management tables will be stored.
static const string DB = "sys";

/// Default query options that will be provided on all queries that insert rows into the
/// completed queries table. See the initialization code in the
/// ImpalaServer::WorkloadManagementWorker function for details on which options are set.
static InternalServer::QueryOptionMap insert_query_opts;

static optional<AtomicBool> is_lead_coord;
static AtomicBool init_done;
static mutex init_mutex;

/// Sets up the sys database generating and executing the necessary DML statements.
static void SetupDb(InternalServer* server) {
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";
  ABORT_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      StrCat("CREATE DATABASE IF NOT EXISTS ", DB, " COMMENT "
      "'System database for Impala introspection'"), insert_query_opts, false));
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";
} // function SetupDb

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
static void SetupTable(InternalServer* server, const string& table_name,
    bool is_system_table = false) {
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";

  StringStreamPop create_table_sql;
  create_table_sql << "CREATE ";
  // System tables do not have anything to purge, and must not be managed tables.
  if (is_system_table) create_table_sql << "EXTERNAL ";
  create_table_sql << "TABLE IF NOT EXISTS " << table_name << "(";

  _appendFields(create_table_sql, [](const FieldDefinition& f){return !f.append_field;});

  create_table_sql << ") ";

  if (!is_system_table) {
    create_table_sql << "PARTITIONED BY SPEC(identity(cluster_id), HOUR(start_time_utc)) "
        << "STORED AS iceberg ";

    if (!FLAGS_query_log_table_location.empty()) {
      create_table_sql << "LOCATION '" << FLAGS_query_log_table_location << "' ";
    }
  }

  create_table_sql << "TBLPROPERTIES ('schema_version'='1.0.0','format-version'='2'";

  if (is_system_table) {
    create_table_sql << ",'"
                     << g_CatalogObjects_constants.TBL_PROP_SYSTEM_TABLE <<"'='true'";
  } else if (!FLAGS_query_log_table_props.empty()) {
    create_table_sql << "," << FLAGS_query_log_table_props;
  }

  create_table_sql << ")";

  ABORT_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      create_table_sql.str(), insert_query_opts, false));

  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";

  LOG(INFO) << "Completed " << table_name << " initialization. write_interval=\"" <<
      FLAGS_query_log_write_interval_s << "s\"";
} // function SetupTable

/// Upgrades a table by running alter table statements.
static void _upgradeTo_2_0_0(InternalServer* server, const string& table_name) {
  StringStreamPop cols_to_add;

  cols_to_add << "ALTER TABLE " << table_name << " ADD IF NOT EXISTS COLUMNS(";
  _appendFields(cols_to_add, [](const FieldDefinition& f){ return f.append_field;});
  cols_to_add << ")";

  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";

  for (const string& sql : array<string, 2>{
    std::move(cols_to_add.str()),
    "ALTER TABLE " + table_name + " SET TBLPROPERTIES ('schema_version'='2.0.0')"
  }) {
    ABORT_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
        sql, insert_query_opts, false));
  }

  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";
} // function _upgradeTo_2_0_0

void ImpalaServer::InitWorkloadManagement() {
  // Verify FIELD_DEFINITIONS includes all QueryTableColumns.
  DCHECK_EQ(_TQueryTableColumn_VALUES_TO_NAMES.size(), FIELD_DEFINITIONS.size());
  for (const auto& field : FIELD_DEFINITIONS) {
    // Verify all fields match their column position.
    DCHECK_EQ(FIELD_DEFINITIONS[field.db_column].db_column, field.db_column);
  }

  // Wait until the internal server is ready.
  unique_lock<mutex> l(init_mutex);
  wm_init_cv_.wait(l, [this] {
    LOG(WARNING) << "WAIT FOR INTERNAL SERVER PREDICATE" << std::endl;
    if (!internal_server_.get()) return false;

    if (is_lead_coord.has_value() && is_lead_coord.value().Load()) return true;

    return init_done.Load();
  });

  {
    lock_guard<mutex> l(completed_queries_threadstate_mu_);
    completed_queries_thread_state_ = INITIALIZING;
  }

  // Setup default query options.
  insert_query_opts[TImpalaQueryOptions::TIMEZONE] = "UTC";
  insert_query_opts[TImpalaQueryOptions::QUERY_TIMEOUT_S] = std::to_string(
      FLAGS_query_log_write_timeout_s < 1 ?
      FLAGS_query_log_write_interval_s : FLAGS_query_log_write_timeout_s);
  if (!FLAGS_query_log_request_pool.empty()) {
    insert_query_opts[TImpalaQueryOptions::REQUEST_POOL] = FLAGS_query_log_request_pool;
  }

  // Fully qualified table name based on startup flags.
  const string log_table_name = StrCat(DB, ".", FLAGS_query_log_table_name);

  if (is_lead_coord.value().Load()) {
    // The initialization code only works when run in a separate thread for reasons unknown.
    SetupDb(internal_server_.get());
    SetupTable(internal_server_.get(), log_table_name);
    _upgradeTo_2_0_0(internal_server_.get(), log_table_name);

    std::string live_table_name = to_string(TSystemTableName::IMPALA_QUERY_LIVE);
    boost::algorithm::to_lower(live_table_name);
    SetupTable(internal_server_.get(), StrCat(DB, ".", live_table_name), true);
    _upgradeTo_2_0_0(internal_server_.get(), StrCat(DB, ".", live_table_name));

    LOG(WARNING) << "WORKLOAD MANAGEMENT INIT COMPLETE" << std::endl;
    init_done.Store(true);
  } else {
    LOG(WARNING) << "SKIPPING WORKLOAD MANAGEMENT INIT" << std::endl;
  }
  
  WorkloadManagementWorker(insert_query_opts, log_table_name);
} // ImpalaServer::InitWorkloadManagement

static std::mutex update_topic_lock_;
static optional<string> my_id;
static optional<string> first_id;

static TTopicDelta build_topic_delta(string key, string value) {
  TTopicDelta my_delta;
  my_delta.topic_name = Statestore::IMPALA_WORKLOAD_MANAGEMENT_TOPIC;

  TTopicItem item;
  item.key = key;
  item.value = value;

  my_delta.topic_entries.push_back(item);

  return my_delta;
}
void ImpalaServer::WorkloadManagementTopicUpdate(
    const StatestoreSubscriber::TopicDeltaMap& state,
    std::vector<TTopicDelta>* topic_updates) {

  lock_guard<mutex> l(update_topic_lock_);
  // LOG(WARNING) << "STATESTORE HEARTBEAT" << std::endl;

  if (!my_id.has_value()) {
    my_id = PrintId(RandomUniqueID());
    LOG(WARNING) << "MY ID: " << my_id.value() << std::endl;
    topic_updates->push_back(build_topic_delta(my_id.value(), "rand_id"));
  }

  if(is_lead_coord.has_value() && is_lead_coord.value().Load() && init_done.Load()) {
    // Notify the rest of the cluster that workload management initialization is done.
    topic_updates->push_back(build_topic_delta("wm_init_done", "true"));
  }

  // First look to see if the topic we're interested in has an update.
  auto topic = state.find(Statestore::IMPALA_WORKLOAD_MANAGEMENT_TOPIC);

  // Ignore spurious messages.
  if (topic == state.end()) return;

  const TTopicDelta& update = topic->second;

  // If the update transmitted by the statestore is empty, we don't need to process it.
  if (update.topic_entries.empty()) return;

  if(!first_id.has_value()) {
    first_id = update.topic_entries[0].key;

    if (first_id.value() == my_id.value()) {
      LOG(WARNING) << "YAY I'M THE FIRST" << std::endl;
      is_lead_coord.emplace(true);
    } else {
      LOG(WARNING) << "BOO I'M NOT THE FIRST" << std::endl;
      is_lead_coord.emplace(false);
    }

    ABORT_IF_ERROR(Thread::Create("impala-server", "completed-queries",
      bind<void>(&ImpalaServer::InitWorkloadManagement, this),
      &completed_queries_thread_));
  } else {
    for (auto& e : update.topic_entries) {
      if (e.key == "wm_init_done" && is_lead_coord.has_value()
          && !is_lead_coord.value().Load() && !init_done.Load()) {
        init_done.Store(true);
        wm_init_cv_.notify_all();
      }
    }
  }
} // ImpalaServer::WorkloadManagementTopicUpdate

} // namespace impala
