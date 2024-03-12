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

#include "workload_mgmt/workload-management.h"

#include <sstream>
#include <string>

#include <gtest/gtest.h>
#include <gutil/strings/strcat.h>

#include "gen-cpp/SystemTables_types.h"
#include "gen-cpp/Types_types.h"
#include "kudu/util/version_util.h"

using namespace std;
using namespace impala;
using namespace impala::workloadmgmt;

using kudu::ParseVersion;
using kudu::Version;

static constexpr int8_t EXPECTED_DECIMAL_PRECISION = 18;
static constexpr int8_t EXPECTED_DECIMAL_SCALE = 3;

/// Builds and returns a string that can be used to identify the column name during a test
/// failure situation.
static string _eMsg(const string& expected_name) {
  return StrCat("for field: ", expected_name);
}

/// Asserts the common fields on a FieldDefinition instance.
static void _assertCol(const string& expected_name,
    const TPrimitiveType::type& expected_type, const string& expected_version,
    const TQueryTableColumn::type db_col,  const FieldDefinition& actual){
  stringstream actual_col_name;
  actual_col_name << db_col;

  stringstream actual_col_type;
  actual_col_type << actual.db_column_type;

  EXPECT_EQ(expected_name, actual_col_name.str()) << _eMsg(expected_name);
  EXPECT_EQ(to_string(expected_type), actual_col_type.str()) << _eMsg(expected_name);
  EXPECT_EQ(expected_version, actual.schema_version.ToString()) << _eMsg(expected_name);
}

/// Asserts a column of type string.
static void _assertColStr(const string& expected_name, const string& expected_version,
    const TQueryTableColumn::type db_col, const FieldDefinition& actual){
  _assertCol(expected_name, TPrimitiveType::STRING, expected_version, db_col, actual);
}

/// Asserts a column of type big int.
static void _assertColBigInt(const string& expected_name, const string& expected_version,
    const TQueryTableColumn::type db_col, const FieldDefinition& actual){
  _assertCol(expected_name, TPrimitiveType::BIGINT, expected_version, db_col, actual);
}

/// Asserts the common and decimal related fields on a FieldDefinition instance.
static void _assertColDecimal(const string& expected_name, const string& expected_version,
    const TQueryTableColumn::type db_col,  const FieldDefinition& actual){
  _assertCol(expected_name, TPrimitiveType::DECIMAL, expected_version, db_col, actual);

  EXPECT_EQ(EXPECTED_DECIMAL_PRECISION, actual.precision) << _eMsg(expected_name);
  EXPECT_EQ(EXPECTED_DECIMAL_SCALE, actual.scale) << _eMsg(expected_name);
}

TEST(WorkloadManagementFieldsTest, CheckFieldDefinitions) {
  // Asserts FIELD_DEFINITIONS includes all QueryTableColumns and has correctly defined
  // each column.
  EXPECT_EQ(_TQueryTableColumn_VALUES_TO_NAMES.size(), FIELD_DEFINITIONS.size());

  auto it = FIELD_DEFINITIONS.cbegin();

  _assertColStr("CLUSTER_ID", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("QUERY_ID", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("SESSION_ID", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("SESSION_TYPE", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("HIVESERVER2_PROTOCOL_VERSION", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("DB_USER", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("DB_USER_CONNECTION", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("DB_NAME", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("IMPALA_COORDINATOR", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("QUERY_STATUS", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("QUERY_STATE", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("IMPALA_QUERY_END_STATE", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("QUERY_TYPE", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("NETWORK_ADDRESS", "1.0.0", it->first, it->second);
  it++;
  _assertCol("START_TIME_UTC", TPrimitiveType::TIMESTAMP, "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("TOTAL_TIME_MS", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("QUERY_OPTS_CONFIG", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("RESOURCE_POOL", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("PER_HOST_MEM_ESTIMATE", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("DEDICATED_COORD_MEM_ESTIMATE", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("PER_HOST_FRAGMENT_INSTANCES", "1.0.0", it->first, it->second);
  it++;
  _assertCol("BACKENDS_COUNT", TPrimitiveType::INT, "1.0.0", it->first, it->second);
  it++;
  _assertColStr("ADMISSION_RESULT", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("CLUSTER_MEMORY_ADMITTED", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("EXECUTOR_GROUP", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("EXECUTOR_GROUPS", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("EXEC_SUMMARY", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("NUM_ROWS_FETCHED", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("ROW_MATERIALIZATION_ROWS_PER_SEC", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("ROW_MATERIALIZATION_TIME_MS", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("COMPRESSED_BYTES_SPILLED", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("EVENT_PLANNING_FINISHED", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("EVENT_SUBMIT_FOR_ADMISSION", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("EVENT_COMPLETED_ADMISSION", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("EVENT_ALL_BACKENDS_STARTED", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("EVENT_ROWS_AVAILABLE", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("EVENT_FIRST_ROW_FETCHED", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("EVENT_LAST_ROW_FETCHED", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("EVENT_UNREGISTER_QUERY", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("READ_IO_WAIT_TOTAL_MS", "1.0.0", it->first, it->second);
  it++;
  _assertColDecimal("READ_IO_WAIT_MEAN_MS", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("BYTES_READ_CACHE_TOTAL", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("BYTES_READ_TOTAL", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("PERNODE_PEAK_MEM_MIN", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("PERNODE_PEAK_MEM_MAX", "1.0.0", it->first, it->second);
  it++;
  _assertColBigInt("PERNODE_PEAK_MEM_MEAN", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("SQL", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("PLAN", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("TABLES_QUERIED", "1.0.0", it->first, it->second);
  it++;
  _assertColStr("SELECT_COLUMNS", "1.1.0", it->first, it->second);
  it++;
  _assertColStr("WHERE_COLUMNS", "1.1.0", it->first, it->second);
  it++;
  _assertColStr("JOIN_COLUMNS", "1.1.0", it->first, it->second);
  it++;
  _assertColStr("AGGREGATE_COLUMNS", "1.1.0", it->first, it->second);
  it++;
  _assertColStr("ORDERBY_COLUMNS", "1.1.0", it->first, it->second);
  it++;

  EXPECT_EQ(FIELD_DEFINITIONS.cend(), it);
}
