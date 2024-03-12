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

#include <sstream>
#include <string>

#include <gutil/strings/strcat.h>

#include "gen-cpp/SystemTables_types.h"
#include "testutil/gtest-util.h"

using std::stringstream;
using std::string;

namespace impala {

static constexpr int8_t EXPECTED_DECIMAL_PRECISION = 18;
static constexpr int8_t EXPECTED_DECIMAL_SCALE = 3;

// Builds and returns a string that can be used to identify the column name during a test
// failure situation.
static string _eMsg(const string& expected_name) {
  return StrCat("for field: ", expected_name);
}

// Asserts the common fields on a FieldDefinition instance.
static void _assertCol(const string& expected_name,
    const TPrimitiveType::type& expected_type, const string& expected_version,
    const FieldDefinition& actual){
  stringstream actual_col_name;
  actual_col_name << actual.db_column;

  stringstream actual_col_type;
  actual_col_type << actual.db_column_type;

  EXPECT_EQ(expected_name, actual_col_name.str()) << _eMsg(expected_name);
  EXPECT_EQ(to_string(expected_type), actual_col_type.str()) << _eMsg(expected_name);
  EXPECT_EQ(expected_version, actual.schema_version.ToString()) << _eMsg(expected_name);
}

static void _assertColStr(const string& expected_name, const string& expected_version,
    const FieldDefinition& actual){
  _assertCol(expected_name, TPrimitiveType::STRING, expected_version, actual);
}

static void _assertColBigInt(const string& expected_name, const string& expected_version,
    const FieldDefinition& actual){
  _assertCol(expected_name, TPrimitiveType::BIGINT, expected_version, actual);
}

// Asserts the common fields and the decimal related fields on a FieldDefinition instance.
static void _assertColDecimal(const string& expected_name,
    const string& expected_version, const FieldDefinition& actual){
  _assertCol(expected_name, TPrimitiveType::DECIMAL, expected_version, actual);

  EXPECT_EQ(EXPECTED_DECIMAL_PRECISION, actual.precision) << _eMsg(expected_name);
  EXPECT_EQ(EXPECTED_DECIMAL_SCALE, actual.scale) << _eMsg(expected_name);
}

TEST(WorkloadManagementTest, CheckColumnNames) {
  EXPECT_EQ(54, FIELD_DEFINITIONS.size());

  _assertColStr("CLUSTER_ID", "1.0.0", FIELD_DEFINITIONS[0]);
  _assertColStr("QUERY_ID", "1.0.0", FIELD_DEFINITIONS[1]);
  _assertColStr("SESSION_ID", "1.0.0", FIELD_DEFINITIONS[2]);
  _assertColStr("SESSION_TYPE", "1.0.0", FIELD_DEFINITIONS[3]);
  _assertColStr("HIVESERVER2_PROTOCOL_VERSION", "1.0.0", FIELD_DEFINITIONS[4]);
  _assertColStr("DB_USER", "1.0.0", FIELD_DEFINITIONS[5]);
  _assertColStr("DB_USER_CONNECTION", "1.0.0", FIELD_DEFINITIONS[6]);
  _assertColStr("DB_NAME", "1.0.0", FIELD_DEFINITIONS[7]);
  _assertColStr("IMPALA_COORDINATOR", "1.0.0", FIELD_DEFINITIONS[8]);
  _assertColStr("QUERY_STATUS", "1.0.0", FIELD_DEFINITIONS[9]);
  _assertColStr("QUERY_STATE", "1.0.0", FIELD_DEFINITIONS[10]);
  _assertColStr("IMPALA_QUERY_END_STATE", "1.0.0", FIELD_DEFINITIONS[11]);
  _assertColStr("QUERY_TYPE", "1.0.0", FIELD_DEFINITIONS[12]);
  _assertColStr("NETWORK_ADDRESS", "1.0.0", FIELD_DEFINITIONS[13]);
  _assertCol("START_TIME_UTC", TPrimitiveType::TIMESTAMP, "1.0.0", FIELD_DEFINITIONS[14]);
  _assertColDecimal("TOTAL_TIME_MS", "1.0.0", FIELD_DEFINITIONS[15]);
  _assertColStr("QUERY_OPTS_CONFIG", "1.0.0", FIELD_DEFINITIONS[16]);
  _assertColStr("RESOURCE_POOL", "1.0.0", FIELD_DEFINITIONS[17]);
  _assertColBigInt("PER_HOST_MEM_ESTIMATE", "1.0.0", FIELD_DEFINITIONS[18]);
  _assertColBigInt("DEDICATED_COORD_MEM_ESTIMATE", "1.0.0", FIELD_DEFINITIONS[19]);
  _assertColStr("PER_HOST_FRAGMENT_INSTANCES", "1.0.0", FIELD_DEFINITIONS[20]);
  _assertCol("BACKENDS_COUNT", TPrimitiveType::INT, "1.0.0", FIELD_DEFINITIONS[21]);
  _assertColStr("ADMISSION_RESULT", "1.0.0", FIELD_DEFINITIONS[22]);
  _assertColBigInt("CLUSTER_MEMORY_ADMITTED", "1.0.0", FIELD_DEFINITIONS[23]);
  _assertColStr("EXECUTOR_GROUP", "1.0.0", FIELD_DEFINITIONS[24]);
  _assertColStr("EXECUTOR_GROUPS", "1.0.0", FIELD_DEFINITIONS[25]);
  _assertColStr("EXEC_SUMMARY", "1.0.0", FIELD_DEFINITIONS[26]);
  _assertColBigInt("NUM_ROWS_FETCHED", "1.0.0", FIELD_DEFINITIONS[27]);
  _assertColBigInt("ROW_MATERIALIZATION_ROWS_PER_SEC", "1.0.0", FIELD_DEFINITIONS[28]);
  _assertColDecimal("ROW_MATERIALIZATION_TIME_MS", "1.0.0", FIELD_DEFINITIONS[29]);
  _assertColBigInt("COMPRESSED_BYTES_SPILLED", "1.0.0", FIELD_DEFINITIONS[30]);
  _assertColDecimal("EVENT_PLANNING_FINISHED", "1.0.0", FIELD_DEFINITIONS[31]);
  _assertColDecimal("EVENT_SUBMIT_FOR_ADMISSION", "1.0.0", FIELD_DEFINITIONS[32]);
  _assertColDecimal("EVENT_COMPLETED_ADMISSION", "1.0.0", FIELD_DEFINITIONS[33]);
  _assertColDecimal("EVENT_ALL_BACKENDS_STARTED", "1.0.0", FIELD_DEFINITIONS[34]);
  _assertColDecimal("EVENT_ROWS_AVAILABLE", "1.0.0", FIELD_DEFINITIONS[35]);
  _assertColDecimal("EVENT_FIRST_ROW_FETCHED", "1.0.0", FIELD_DEFINITIONS[36]);
  _assertColDecimal("EVENT_LAST_ROW_FETCHED", "1.0.0", FIELD_DEFINITIONS[37]);
  _assertColDecimal("EVENT_UNREGISTER_QUERY", "1.0.0", FIELD_DEFINITIONS[38]);
  _assertColDecimal("READ_IO_WAIT_TOTAL_MS", "1.0.0", FIELD_DEFINITIONS[39]);
  _assertColDecimal("READ_IO_WAIT_MEAN_MS", "1.0.0", FIELD_DEFINITIONS[40]);
  _assertColBigInt("BYTES_READ_CACHE_TOTAL", "1.0.0", FIELD_DEFINITIONS[41]);
  _assertColBigInt("BYTES_READ_TOTAL", "1.0.0", FIELD_DEFINITIONS[42]);
  _assertColBigInt("PERNODE_PEAK_MEM_MIN", "1.0.0", FIELD_DEFINITIONS[43]);
  _assertColBigInt("PERNODE_PEAK_MEM_MAX", "1.0.0", FIELD_DEFINITIONS[44]);
  _assertColBigInt("PERNODE_PEAK_MEM_MEAN", "1.0.0", FIELD_DEFINITIONS[45]);
  _assertColStr("SQL", "1.0.0", FIELD_DEFINITIONS[46]);
  _assertColStr("PLAN", "1.0.0", FIELD_DEFINITIONS[47]);
  _assertColStr("TABLES_QUERIED", "1.0.0", FIELD_DEFINITIONS[48]);
  _assertColStr("SELECT_COLUMNS", "1.1.0", FIELD_DEFINITIONS[49]);
  _assertColStr("WHERE_COLUMNS", "1.1.0", FIELD_DEFINITIONS[50]);
  _assertColStr("JOIN_COLUMNS", "1.1.0", FIELD_DEFINITIONS[51]);
  _assertColStr("AGGREGATE_COLUMNS", "1.1.0", FIELD_DEFINITIONS[52]);
  _assertColStr("ORDERBY_COLUMNS", "1.1.0", FIELD_DEFINITIONS[53]);
}

} // namespace impala
