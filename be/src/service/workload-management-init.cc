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
///   1. Checking the state of the workload management db and tables.
///   2. Creating or upgrading the db/tables if necessary.
///   3. Starting the workload management thread which runs the completed queries
///      processing loop.

#include "service/workload-management.h"

#include <mutex>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gutil/strings/strcat.h>

#include "common/status.h"
#include "gen-cpp/CatalogObjects_constants.h"
#include "gen-cpp/SystemTables_types.h"
#include "gen-cpp/TCLIService_types.h"
#include "gen-cpp/Types_types.h"
#include "kudu/util/random.h"
#include "kudu/util/version_util.h"
#include "service/impala-server.h"
#include "util/string-join.h"
#include "util/string-util.h"
#include "util/version-util.h"

using namespace std;
using namespace impala;
using boost::algorithm::starts_with;
using boost::algorithm::to_lower;
using boost::algorithm::trim_copy;
using kudu::Version;
using kudu::ParseVersion;

DECLARE_int32(krpc_port);
DECLARE_int32(query_log_write_interval_s);
DECLARE_int32(query_log_write_timeout_s);
DECLARE_string(query_log_request_pool);
DECLARE_string(query_log_table_location);
DECLARE_string(query_log_table_name);
DECLARE_string(query_log_table_props);
DECLARE_string(workload_mgmt_user);
DECLARE_string(workload_mgmt_schema_version);

namespace impala {

/// Name of the database where all workload management tables will be stored.
static const string DB = "sys";

/// Sets up the sys database generating and executing the necessary DML statements.
static void _setupDb(InternalServer* internal_server,
    InternalServer::QueryOptionMap& insert_query_opts) {
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";
  ABORT_IF_ERROR(internal_server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      StrCat("CREATE DATABASE IF NOT EXISTS ", DB, " COMMENT "
      "'System database for Impala introspection'"), insert_query_opts, false));
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";
} // function _setupDb

/// Appends all relevant fields to a create or alter table sql statement.
/// Errors if no columns were added to the sql stream.
static void _appendCols(StringStreamPop& sql,
    std::function<bool(const FieldDefinition& item)> shouldIncludeCol) {
  bool match = false;

  for (const auto& field : FIELD_DEFINITIONS) {
   if (shouldIncludeCol(field)) {
    match = true;
    sql << field.db_column << " " << field.db_column_type;

      if (field.db_column_type == TPrimitiveType::DECIMAL) {
        sql << "(" << field.precision << "," << field.scale << ")";
      }

      sql << ",";
   }
  }

  DCHECK(match);
  sql.move_back();
} // function _appendCols

/// Sets up the query table by generating and executing the necessary DML statements.
static void _setupTable(InternalServer* internal_server,
    InternalServer::QueryOptionMap& insert_query_opts, const string& table_name,
    const Version& target_version, bool is_system_table = false) {
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";

  StringStreamPop create_table_sql;
  create_table_sql << "CREATE ";
  // System tables do not have anything to purge, and must not be managed tables.
  if (is_system_table) create_table_sql << "EXTERNAL ";
  create_table_sql << "TABLE IF NOT EXISTS " << table_name << "(";

  _appendCols(create_table_sql, [target_version](const FieldDefinition& f){
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
  ABORT_IF_ERROR(internal_server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      create_table_sql.str(), insert_query_opts, false));

  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";

  LOG(INFO) << "Completed " << table_name << " initialization. write_interval=\"" <<
      FLAGS_query_log_write_interval_s << "s\"";
} // function _setupTable

/// Upgrades a table by running alter table statements.
static void _upgradeTable(InternalServer* internal_server,
    InternalServer::QueryOptionMap& insert_query_opts, const string& table_name,
    const Version& current_version, const Version& target_version) {

  DCHECK_NE(current_version, target_version);

  StringStreamPop cols_to_add;

  cols_to_add << "ALTER TABLE " << table_name << " ADD IF NOT EXISTS COLUMNS(";
  _appendCols(cols_to_add, [current_version, target_version](const FieldDefinition& f){
      return f.schema_version > current_version && f.schema_version <= target_version; });
  cols_to_add << ")";

  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";

  VLOG(2) << "Upgrading workload management table '" << table_name << "' from schema "
      << "version '" << current_version.ToString() << "' to '"
      << target_version.ToString() << "'";

  // Run the alter table statement.
  ABORT_IF_ERROR(internal_server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      cols_to_add.str(), insert_query_opts, false));

  // Set the table schema_version property to the upgraded schema version.
  ABORT_IF_ERROR(internal_server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      StrCat("ALTER TABLE ", table_name, " SET TBLPROPERTIES ('schema_version'='",
      target_version.ToString(), "')"), insert_query_opts, false));

  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";
} // function _upgradeTable

/// Retrieves the workload management schema version from a table by searching the results
/// from a DESCRIBE EXTENDED query for the schema_version table property.
static Version _retrieveSchemaVersion(InternalServer* internal_server,
    const string table_name, const InternalServer::QueryOptionMap& insert_query_opts) {

  vector<apache::hive::service::cli::thrift::TRow> query_results;

  const Status describe_table = internal_server->ExecuteAndFetchAllHS2(
      FLAGS_workload_mgmt_user, StrCat("DESCRIBE EXTENDED ", table_name), query_results,
      insert_query_opts, false);

  // If an error, ignore the error and run as if workload management has never
  // executed. Since all the DDLs use the "if not exists" clause, extra runs of the
  // DDLs will not cause any harm.
  if (describe_table.ok()) {
    const string SCHEMA_VER_PROP_NAME = "schema_version";

    // Table exists, search for its schema_version table property.
    for(auto& res : query_results) {
      if (starts_with(res.colVals[1].stringVal.value, SCHEMA_VER_PROP_NAME) ){
        const string schema_ver = trim_copy(res.colVals[2].stringVal.value);
        Version parsed_schema_ver;

        VLOG(2) << "Actual current workload management schema version of the '"
            << table_name << "' table is '" << schema_ver << "'";

        if(!ParseVersion(schema_ver, &parsed_schema_ver).ok()) {
          ABORT_WITH_ERROR(StrCat("Invalid actual workload management schema version '",
              schema_ver, "' for table '", table_name, "'"));
        }

        return parsed_schema_ver;
      }
    }

    // If the for loop does not find the schema_version table property, then it has been
    // removed outside of the workload management code.
    ABORT_WITH_ERROR(StrCat("Table '", table_name, "' is missing required property '",
        SCHEMA_VER_PROP_NAME, "'"));
  }

  return NO_TABLE_EXISTS;
} // function _retrieveSchemaVersion

/// Manages the schema for the query log table. If the table does not exist, it is
/// created. If it does exist, but is not on the schema version specified via the command
/// line flag, it is altered to bring it up to the new version.
static string _logTableSchemaManagement(InternalServer* internal_server,
    InternalServer::QueryOptionMap& insert_query_opts,
    const Version target_schema_version) {

  Version parsed_actual_schema_version;

  // Create and/or update the completed queries table if needed.
  // Fully qualified table name based on startup flags.
  string log_table_name = StrCat(DB, ".", FLAGS_query_log_table_name);
  to_lower(log_table_name);

  parsed_actual_schema_version = _retrieveSchemaVersion(internal_server, log_table_name,
      insert_query_opts);

  if (parsed_actual_schema_version == target_schema_version) {
    LOG(INFO) << "Target schema version '" << target_schema_version.ToString() <<
        "' matches actual schema version '" << parsed_actual_schema_version.ToString() <<
        "' for the '" << log_table_name << "' table";
  } else if (parsed_actual_schema_version == NO_TABLE_EXISTS) {
    // Log table does not exist.
    _setupDb(internal_server, insert_query_opts);

    // Create the query log table at the target schema version.
    _setupTable(internal_server, insert_query_opts, log_table_name,
        target_schema_version);
  } else if(parsed_actual_schema_version > target_schema_version) {
    // Actual schema version is greater than the target schema version.
    LOG(WARNING) << "Target schema version '" << target_schema_version.ToString() <<
        "' of the '" << log_table_name << "' table is lower than the actual schema " <<
        "version '" << parsed_actual_schema_version.ToString() << "'";
  } else {
    // Target schema version is greater than the actual schema version. Upgrade the query
    // log table.
    _upgradeTable(internal_server, insert_query_opts, log_table_name,
        parsed_actual_schema_version, target_schema_version);
  }

  return log_table_name;
} // function _logTableSchemaManagement

/// Manages the schema for the live queries table. If the table does not exist, it is
/// created. If it does exist, but is not on the schema version specified via the command
/// line flag, it is dropped and recreated on the new version. Since the live table is an
/// in-memory system table, it cannot be altered. It must be dropped and re-created.
/// Note: assumes the _setupDb function has already run.
static void _liveTableSchemaManagement(InternalServer* internal_server,
    InternalServer::QueryOptionMap& insert_query_opts,
    const Version target_schema_version) {

  Version parsed_actual_schema_version;

  // Fully qualified table name based on startup flags.
  string live_table_name = StrCat(DB, ".",
      to_string(TSystemTableName::IMPALA_QUERY_LIVE));
  to_lower(live_table_name);

  parsed_actual_schema_version = _retrieveSchemaVersion(internal_server, live_table_name,
      insert_query_opts);

  if (parsed_actual_schema_version == target_schema_version) {
    LOG(INFO) << "Target schema version '" << target_schema_version.ToString() <<
        "' matches actual schema version '" << parsed_actual_schema_version.ToString() <<
        "' for the '" << live_table_name << "' table";
  } else if (parsed_actual_schema_version == NO_TABLE_EXISTS) {
    // Live table does not exist.
    // Create the live query table on the target schema version.
    _setupTable(internal_server, insert_query_opts, live_table_name,
        target_schema_version, true);
  } else if(parsed_actual_schema_version > target_schema_version) {
    // Actual schema version is greater than the target schema version.
    LOG(WARNING) << "Target schema version '" << target_schema_version.ToString() <<
        "' of the '" << live_table_name << "' table is lower than the actual schema " <<
        "version '" << parsed_actual_schema_version.ToString() << "'";
  } else {
    // All checks have passed. Drop the live query table to re-create it since the live
    // query table cannot be altered.
    VLOG(2) << "Dropping '" << live_table_name << "' so it can be upgraded to "
        << "the target schema version '" << target_schema_version.ToString()
        << "'";
    ABORT_IF_ERROR(internal_server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
        StrCat("DROP TABLE IF EXISTS ", live_table_name), insert_query_opts, false));

    // Create the live query table on the target schema version.
    _setupTable(internal_server, insert_query_opts, live_table_name,
        target_schema_version, true);
  }
} // function _liveTableSchemaManagement

void ImpalaServer::InitWorkloadManagement() {
  LOG(INFO) << "Starting workload management initialization process" << endl;

  // Add jitter to startup process to ensure workload management init does not run
  // concurrently on all coordinators. Running the same create and alter table statements
  // at the same time across multiple coordinators frequently results in table not found
  // errors when running the alter table statements.
  //
  // Jitter is calculated by adding the port number to a random int between 1 and 60,000
  // then reducing the sum by an order of magnitude before sleeping for that many
  // milliseconds. The longest sleep possible is the max port number plus 60,000 which is
  // (65535 + 60000) / 10 = 12,553.5 or 12.5 seconds.
  kudu::Random sleep_rand(FLAGS_krpc_port);
  int ms_to_sleep = (FLAGS_krpc_port + sleep_rand.Uniform32(60000) + 1) / 10;
  VLOG(2) << "sleeping for '" << ms_to_sleep << "' ms";
  SleepForMs(ms_to_sleep);

  {
    lock_guard<mutex> l(workload_mgmt_state_mu_);
    workload_mgmt_state_ = WorkloadMgmtState::INITIALIZING;
  }

  // Ensure there is at least one known version.
  DCHECK_GT(KNOWN_VERSIONS.size(), 0);

  // Ensure a valid schema version was specified on the command line flag.
  const Version latest_schema_version = *KNOWN_VERSIONS.rbegin();
  Version target_schema_version = latest_schema_version;
  if (!FLAGS_workload_mgmt_schema_version.empty()
      && !ParseVersion(FLAGS_workload_mgmt_schema_version, &target_schema_version).ok()) {
    ABORT_WITH_ERROR(StrCat("Invalid workload management schema version '",
        FLAGS_workload_mgmt_schema_version, "'"));
  }

  VLOG(2) << "Target workload management schema version is '"
      << target_schema_version.ToString() << "'";

  // Verify the specified workload management schema version is a known version.
  if (auto v = KNOWN_VERSIONS.find(target_schema_version); v == KNOWN_VERSIONS.end()) {
    ABORT_WITH_ERROR(StrCat("Workload management schema version '",
        FLAGS_workload_mgmt_schema_version, "' is not one of the known versions: '",
        JoinToString(KNOWN_VERSIONS, "', '"), "'"));
  }

  // Warn if not targeting the latest version.
  if (target_schema_version != latest_schema_version) {
    LOG(WARNING) << "Target schema version '" << target_schema_version.ToString() <<
        "' is not the latest schema version '" << latest_schema_version.ToString() << "'";
  }

  // Verify FIELD_DEFINITIONS includes all QueryTableColumns.
  DCHECK_EQ(_TQueryTableColumn_VALUES_TO_NAMES.size(), FIELD_DEFINITIONS.size());
  for (const auto& field : FIELD_DEFINITIONS) {
    // Verify all fields match their column position.
    DCHECK_EQ(FIELD_DEFINITIONS[field.db_column].db_column, field.db_column);
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

  // Create and/or update the query log table if needed.
  const string log_table_name = _logTableSchemaManagement(this, insert_query_opts,
      target_schema_version);

  // Create and/or update the live queries table if needed.
  // Must be run after _logTableSchemaManagement
  _liveTableSchemaManagement(this, insert_query_opts, target_schema_version);

  {
    lock_guard<mutex> l(workload_mgmt_state_mu_);
    workload_mgmt_state_ = WorkloadMgmtState::INITIALIZED;
  }

  LOG(INFO) << "Completed workload management initialization";

  WorkloadManagementWorker(insert_query_opts, log_table_name, target_schema_version);
} // function ImpalaServer::InitWorkloadManagement

} // namespace impala
