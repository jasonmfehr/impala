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
#include "testutil/death-test-util.h"

/// Allows for resetting the cache variable `parsed_target_schema_version` which contains
/// the schema version parsed from the workload_mgmt_schema_version startup flag.
#include "workload_mgmt/workload-management.cc"

DECLARE_string(query_log_table_name);
DECLARE_string(workload_mgmt_schema_version);

using namespace std;
using namespace impala;
using namespace impala::workloadmgmt;

using kudu::ParseVersion;
using kudu::Version;

TEST(WorkloadManagementTest, StartupChecksHappyPath) {
  parsed_target_schema_version.reset();
  FLAGS_workload_mgmt_schema_version = "1.1.0";
  Version actual;

  ASSERT_OK(StartupChecks(&actual));
  EXPECT_EQ("1.1.0", actual.ToString());
  EXPECT_TRUE(parsed_target_schema_version.has_value());
  EXPECT_EQ("1.1.0", parsed_target_schema_version->ToString());
}

TEST(WorkloadManagementTest, StartupChecksDefaultValue) {
  parsed_target_schema_version.reset();
  FLAGS_workload_mgmt_schema_version = "";
  Version actual;

  ASSERT_OK(StartupChecks(&actual));
  EXPECT_EQ("1.1.0", actual.ToString());
  EXPECT_TRUE(parsed_target_schema_version.has_value());
  EXPECT_EQ("1.1.0", parsed_target_schema_version->ToString());
}

TEST(WorkloadManagementTest, StartupChecksHappyPathOlderVersion) {
  // Asserts the StartupChecks function works on older schema versions.
  parsed_target_schema_version.reset();
  FLAGS_workload_mgmt_schema_version = "1.0.0";
  Version actual;

  ASSERT_OK(StartupChecks(&actual));
  EXPECT_EQ("1.0.0", actual.ToString());
  EXPECT_TRUE(parsed_target_schema_version.has_value());
  EXPECT_EQ("1.0.0", parsed_target_schema_version->ToString());
}

TEST(WorkloadManagementTest, GetTargetSchemaVersionInvalid) {
  // Asserts the correct error is returned when the workload management schema version is
  // set to an invalid version string.
  parsed_target_schema_version.reset();
  FLAGS_workload_mgmt_schema_version = "notaversion";

  Version v;
  ASSERT_TRUE(ParseVersion("0.0.0", &v).ok());

  Status actual = StartupChecks(&v);
  EXPECT_EQ(TErrorCode::GENERAL, actual.code());
  EXPECT_EQ(actual.msg().msg(),
      "Invalid workload management schema version 'notaversion'");
  EXPECT_FALSE(parsed_target_schema_version.has_value());

  // An error condition results in the returned version being set to the default value.
  EXPECT_EQ("0.0.0", v.ToString());
}

TEST(WorkloadManagementTest, StartupChecksUnknownVersion) {
  parsed_target_schema_version.reset();
  FLAGS_workload_mgmt_schema_version = "99999.99999.99999";

  Version v;
  ASSERT_TRUE(ParseVersion("0.0.0", &v).ok());

  Status ret = StartupChecks(&v);

  EXPECT_EQ(TErrorCode::type::GENERAL, ret.code());
  EXPECT_EQ("0.0.0", v.ToString());
  EXPECT_EQ("Workload management schema version '99999.99999.99999' is not one of the "
      "known versions: '1.0.0', '1.1.0'", ret.msg().msg());
  EXPECT_FALSE(parsed_target_schema_version.has_value());
}

TEST(WorkloadManagementTest, StartupChecksTargetSchemaVersion) {
  // Asserts the target schema version is cached the first time StartupChecks are called.
  // Also asserts that callers cannot modify the cached schema version via the
  // StatupChecks function output parameter pointer.
  parsed_target_schema_version.reset();
  FLAGS_workload_mgmt_schema_version = "1.0.0";
  Version actual;

  // Initial function call, caches the schema version.
  ASSERT_OK(StartupChecks(&actual));
  EXPECT_EQ("1.0.0", actual.ToString());
  EXPECT_TRUE(parsed_target_schema_version.has_value());
  EXPECT_EQ("1.0.0", parsed_target_schema_version->ToString());

  // Second function call, assert the cached schema version is used.
  FLAGS_workload_mgmt_schema_version = "";
  ASSERT_OK(StartupChecks(&actual));
  EXPECT_EQ("1.0.0", actual.ToString());
  EXPECT_TRUE(parsed_target_schema_version.has_value());
  EXPECT_EQ("1.0.0", parsed_target_schema_version->ToString());

  // Attempt to overwrite the cached schema version.
  ASSERT_TRUE(ParseVersion("8.8.8", &actual).ok());
  EXPECT_EQ("8.8.8", actual.ToString());
  EXPECT_OK(StartupChecks(&actual));

  // Assert the cached schema version was not overwritten.
  EXPECT_EQ("1.0.0", actual.ToString());
  EXPECT_TRUE(parsed_target_schema_version.has_value());
  EXPECT_EQ("1.0.0", parsed_target_schema_version->ToString());
}

TEST(WorkloadManagementTest, IncludeFieldFalse) {
  // Asserts the IncludeField() function returns `false` when the workload management
  // target schema version is lower than the first schema version `1.0.0`.
  Version v;
  ASSERT_TRUE(kudu::ParseVersion("0.0.0", &v).ok());
  parsed_target_schema_version = v;

  ASSERT_FALSE(IncludeField(TQueryTableColumn::type::CLUSTER_ID));
}

TEST(WorkloadManagementTest, IncludeFieldTrue) {
  // Asserts the IncludeField() function returns `true` when the workload management
  // target schema version is equal to the second schema version `1.1.0`.
  Version v;
  ASSERT_TRUE(kudu::ParseVersion("1.1.0", &v).ok());
  parsed_target_schema_version = v;

  ASSERT_TRUE(IncludeField(TQueryTableColumn::type::CLUSTER_ID));
  ASSERT_TRUE(IncludeField(TQueryTableColumn::type::AGGREGATE_COLUMNS));
}

TEST(WorkloadManagementTest, IncludeFieldMixed) {
  // Asserts the IncludeField() function returns the correct response when the workload
  // management target schema version is equal to the first schema version `1.0.0`.
  Version v;
  ASSERT_TRUE(kudu::ParseVersion("1.0.0", &v).ok());
  parsed_target_schema_version = v;

  // Check a schema version 1.0.0 field.
  ASSERT_TRUE(IncludeField(TQueryTableColumn::type::CLUSTER_ID));

  // Check a schema version 1.1.0 field.
  ASSERT_FALSE(IncludeField(TQueryTableColumn::type::AGGREGATE_COLUMNS));
}

TEST(WorkloadManagementDeathTest, IncludeFieldMissing) {
  // Asserts the case where the FIELD_DEFINITIONS map is missing a column, and that
  // column is checked for inclusion.
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  IMPALA_ASSERT_DEBUG_DEATH(IncludeField(static_cast<TQueryTableColumn::type>(9999)),
      "impala::workloadmgmt::IncludeField()");
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
