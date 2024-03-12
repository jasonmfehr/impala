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

// This file contains the definition of the FIELD_DEFINITIONS list from the associated
// header file. Each field definition consists of the database column name for the field,
// the sql type of the database column, and a function that extracts the actual value from
// a `QueryStateExpanded` instance and writes it to the stream that is collecting all the
// values for the insert dml.

#include "workload_mgmt/workload-management.h"

#include <algorithm>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/join.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gutil/strings/strcat.h>

#include "common/compiler-util.h"
#include "common/status.h"
#include "gen-cpp/SystemTables_types.h"
#include "kudu/util/version_util.h"
#include "util/version-util.h"

DECLARE_string(query_log_table_name);
DECLARE_string(workload_mgmt_schema_version);

using namespace std;
using namespace impala;

using kudu::Version;
using kudu::ParseVersion;

namespace impala {
namespace workloadmgmt {

optional<Version> parsed_target_schema_version;
mutex determine_target_schema_version;

/// Determines if the provided Version matches one of the known schema versions.
static Status _isVersionKnown(Version v) {
  if (auto iter = KNOWN_VERSIONS.find(v); UNLIKELY(iter == KNOWN_VERSIONS.end())) {
    vector<string> transformed(KNOWN_VERSIONS.size());

    transform(KNOWN_VERSIONS.cbegin(), KNOWN_VERSIONS.cend(), transformed.begin(),
        [](const Version& v) -> string {
          return v.ToString();
        });

    return Status(StrCat("Workload management schema version '",
        v.ToString(), "' is not one of the known versions: '",
        boost::algorithm::join(transformed, "', '"), "'"));
  }

  return Status::OK();
} // function _isVersionKnown

/// Parses the workload management schema version startup flag into a kudu::Version
/// object. This object is cached in the parsed_target_schema_version variable and
/// subsequent calls to this function return the cached value.
static Status _determineTargetSchemaVersion(Version* target_schema_version) {
  lock_guard<mutex> l(determine_target_schema_version);

  DCHECK_NE(nullptr, target_schema_version);

  if (parsed_target_schema_version) {
    // This function has already been called once.
    Version tmp_ver = Version(*parsed_target_schema_version);
    *target_schema_version = move(tmp_ver);

    return Status::OK();
  }

  *target_schema_version = *KNOWN_VERSIONS.rbegin();

  // Ensure a valid schema version was specified on the command line flag.
  if (!FLAGS_workload_mgmt_schema_version.empty()
      && !ParseVersion(FLAGS_workload_mgmt_schema_version, target_schema_version).ok()) {
    return Status(StrCat("Invalid workload management schema version '",
        FLAGS_workload_mgmt_schema_version, "'"));
  }

  RETURN_IF_ERROR(_isVersionKnown(*target_schema_version));

  parsed_target_schema_version = *target_schema_version;

  return Status::OK();
} // function _determineTargetSchemaVersion

Status StartupChecks(Version* target_schema_version) {
  DCHECK_NE(nullptr, target_schema_version);

  // Determine target workload management schema version.
  RETURN_IF_ERROR(_determineTargetSchemaVersion(target_schema_version));

  LOG(INFO) << "Target workload management schema version is '"
      << target_schema_version->ToString() << "'";

  // Warn if not targeting the latest version.
  const Version latest_schema_version = *KNOWN_VERSIONS.rbegin();
  if (*target_schema_version != latest_schema_version) {
    LOG(WARNING) << "Target schema version '" << target_schema_version->ToString() <<
        "' is not the latest schema version '" << latest_schema_version.ToString() << "'";
  }

  return Status::OK();
} // function StartupChecks

Version GetTargetSchemaVersion() {
  unique_lock<mutex> l(determine_target_schema_version);

  if (parsed_target_schema_version) {
    return *parsed_target_schema_version;
  }

  l.unlock();
  Version v;
  ABORT_IF_ERROR(_determineTargetSchemaVersion(&v));

  return v;
} // function GetTargetSchemaVersion

string QueryLogTableName(bool with_db) {
  string log_table_name = FLAGS_query_log_table_name;

  if (with_db) {
    log_table_name = StrCat(WM_DB, ".", log_table_name);
  }

  return boost::algorithm::to_lower_copy(log_table_name);
} // function QueryLogTableName

string QueryLiveTableName(bool with_db) {
  string live_table_name = to_string(TSystemTableName::IMPALA_QUERY_LIVE);

  if (with_db) {
    live_table_name = StrCat(WM_DB, ".", live_table_name);
  }

  return boost::algorithm::to_lower_copy(live_table_name);
} // function QueryLiveTableName

} // namespace workloadmgmt
} // namespace impala
