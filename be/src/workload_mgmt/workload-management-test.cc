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

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/SystemTables_types.h"
#include "kudu/util/version_util.h"
#include "testutil/gtest-util.h"

/// Allows for resetting the cache variable containing the schema version parsed from the
/// workload_mgmt_schema_version startup flag.
#include "workload_mgmt/workload-management.cc"

DECLARE_string(query_log_table_name);
DECLARE_string(workload_mgmt_schema_version);

using namespace std;
using namespace impala;
using namespace impala::workloadmgmt;

using kudu::ParseVersion;
using kudu::Version;

TEST(WorkloadManagementTest, StartupChecksHappyPath) {
  FLAGS_workload_mgmt_schema_version = "1.1.0";
  Version actual;

  ASSERT_OK(StartupChecks(&actual));
  ASSERT_EQ("1.1.0", actual.ToString());
  ASSERT_EQ("1.1.0", GetTargetSchemaVersion().ToString());
}

TEST(WorkloadManagementTest, StartupChecksHappyPathOlderVersion) {
  parsed_target_schema_version.reset();
  FLAGS_workload_mgmt_schema_version = "1.0.0";
  Version actual;

  ASSERT_OK(StartupChecks(&actual));
  ASSERT_EQ("1.0.0", actual.ToString());
  ASSERT_EQ("1.0.0", GetTargetSchemaVersion().ToString());
}

TEST(WorkloadManagementTest, StartupChecksInvalidSchemaVersion) {
  parsed_target_schema_version.reset();
  FLAGS_workload_mgmt_schema_version = "1.1.";
  Version actual;

  Status ret = StartupChecks(&actual);

  ASSERT_EQ(TErrorCode::type::GENERAL, ret.code());
  ASSERT_EQ("Invalid workload management schema version '1.1.'", ret.msg().msg());
}

TEST(WorkloadManagementTest, StartupChecksUnknownVersion) {
  FLAGS_workload_mgmt_schema_version = "99999.99999.99999";
  Version actual;

  Status ret = StartupChecks(&actual);

  ASSERT_EQ(TErrorCode::type::GENERAL, ret.code());
  ASSERT_EQ("99999.99999.99999", actual.ToString());
  ASSERT_EQ("Workload management schema version '99999.99999.99999' is not one of the "
      "known versions: '1.0.0', '1.1.0'", ret.msg().msg());
}

// Asserts the target schema version is cached the first time StartupChecks are called.
// Also asserts that callers cannot modify the cached schema version via the StatupChecks
// function output parameter pointer.
TEST(WorkloadManagementTest, StartupChecksTargetSchemaVersion) {
  FLAGS_workload_mgmt_schema_version = "1.0.0";
  Version actual;

  // Initial function call, caches the schema version.
  ASSERT_OK(StartupChecks(&actual));
  ASSERT_EQ("1.0.0", actual.ToString());
  ASSERT_EQ("1.0.0", GetTargetSchemaVersion().ToString());

  // Second function call, assert the cached schema version is used.
  FLAGS_workload_mgmt_schema_version = "";
  ASSERT_OK(StartupChecks(&actual));
  ASSERT_EQ("1.0.0", actual.ToString());
  ASSERT_EQ("1.0.0", GetTargetSchemaVersion().ToString());

  // Attempt to overwrite the cached schema version.
  ASSERT_TRUE(ParseVersion("8.8.8", &actual).ok());
  ASSERT_EQ("8.8.8", actual.ToString());

  // Assert the cached schema version was not overwritten.
  ASSERT_OK(StartupChecks(&actual));
  ASSERT_EQ("1.0.0", actual.ToString());
  ASSERT_EQ("1.0.0", GetTargetSchemaVersion().ToString());
}

TEST(WorkloadManagementTest, QueryLogTableNameWithoutDb) {
  FLAGS_query_log_table_name = "fOObaR";
  ASSERT_EQ("foobar", QueryLogTableName(false));
}

TEST(WorkloadManagementTest, QueryLogTableNameWithDb) {
  FLAGS_query_log_table_name = "FOobAr";
  ASSERT_EQ("sys.foobar", QueryLogTableName(true));
}

TEST(WorkloadManagementTest, QueryLiveTableNameWithoutDb) {
  ASSERT_EQ("impala_query_live", QueryLiveTableName(false));
}

TEST(WorkloadManagementTest, QueryLiveTableNameWithDb) {
  ASSERT_EQ("sys.impala_query_live", QueryLiveTableName(true));
}

TEST(WorkloadManagementTest, KnownVersions) {
  // Asserts the known versions are correct and are in the correct order in the
  // KNOWN_VERSIONS set.
  ASSERT_EQ(2, KNOWN_VERSIONS.size());

  auto iter = KNOWN_VERSIONS.cbegin();

  EXPECT_EQ("1.0.0", iter->ToString());
  iter++;

  EXPECT_EQ("1.1.0", iter->ToString());
  iter++;

  EXPECT_EQ(KNOWN_VERSIONS.cend(), iter);
}

TEST(WorkloadManagementTest, GetTargetSchemaVersionHappyPath) {
  // Asserts the GetTargetSchemaVersion function can be called on its own.
  parsed_target_schema_version.reset();
  FLAGS_workload_mgmt_schema_version = "1.0.0";
  ASSERT_EQ("1.0.0", GetTargetSchemaVersion().ToString());

  FLAGS_workload_mgmt_schema_version = "4.5.6";
  ASSERT_EQ("1.0.0", GetTargetSchemaVersion().ToString());
}

TEST(WorkloadManagementTestDeathTest, GetTargetSchemaVersionInvalid) {
  FLAGS_workload_mgmt_schema_version = "notaversion";
  ASSERT_DEATH(GetTargetSchemaVersion(), "Check failure stack trace");
}
