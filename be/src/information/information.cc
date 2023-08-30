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

#include <gflags/gflags.h>
#include <string>

#include "common/logging.h"
#include "information/information.h"

using namespace impala;

DEFINE_string(store_query_history, "",
    "Specifies if Impala will automatically store query history in a table. An empty or "
    "not-specified value will result in query history not being stored. A value of "
    "'impala' will store the query history table on the Impala instance. If this value "
    "is specified and then later removed, the query history table will remain intact and "
    "accessible.");

DEFINE_validator(store_query_history, [](const char* name, const std::string& val) {
  if (val == "" || val == "impala") return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be one of '' or 'impala'";
  return false;
});

DEFINE_string(query_history_table_name, "default.impala_query_history", "Specifies the "
    "name of the table where query history will be stored. If this value contains a dot, "
    "the left-side part will be used as the database name and the right-side part the "
    "table name. If there is not a dot, then the 'default' database will be used.");

DEFINE_int32(query_history_write_duration_s, 300, "Number of seconds to wait before "
    "inserting completed queries into the query history table.  Setting this to 0 "
    "indicates that queries should be inserted immediately after completion.");

namespace impala {

void CompletedQueryQueue::AddCompletedQuery(const std::shared_ptr<CompletedQuery>& q) {
  queries.push(q);
}

}