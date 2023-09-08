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
#include <memory>
#include <string>

#include "common/logging.h"
#include "information/information.h"
#include "util/time.h"

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
  namespace information {

    CompletedQueries::CompletedQueries() {
      this->queries_ = std::forward_list<std::shared_ptr<CompletedQuery>>();
    }

    std::shared_ptr<CompletedQuery> CompletedQueries::Pop() {
      std::shared_ptr<CompletedQuery> elem = nullptr;

      this->mu_.lock();

      if (!this->queries_.empty()) {
        elem = this->queries_.front();
        this->queries_.pop_front();
      }

      this->mu_.unlock();

      return elem;
    }

    void CompletedQueries::Push(std::shared_ptr<CompletedQuery> query) {
      this->mu_.lock();
      this->queries_.push_front(query);
      this->mu_.unlock();
    }

    void CompletedQueries::Push(CompletedQuery& query) {
      this->Push(std::make_shared<CompletedQuery>(query));
    }

    void CompletedQueries::TransferFrom(std::shared_ptr<CompletedQueries> other) {
      std::shared_ptr<CompletedQuery> elem;

      while ((elem = other->Pop()) != nullptr) {
        this->Push(elem);
      }
    }

    bool CompletedQueries::Empty() const {
      bool ret;

      this->mu_.lock();
      ret = this->queries_.empty();
      this->mu_.unlock();

      return ret;
    }

    std::string CompletedQueries::BuildInsertSQL() {
      std::string sql = "INSERT INTO " + this->query_history_table_name() + 
          "(query_id, session_id, session_type, server_port) VALUES ";

      this->mu_.lock();

      auto iter = this->queries_.cbegin();
      while (iter != this->queries_.cend()) {
        sql += "(";
        sql += "'" + iter->get()->query_id + "', ";
        sql += "'" + iter->get()->session_id + "', ";
        sql += "'" + iter->get()->session_type + "', ";
        sql += std::to_string(iter->get()->server_port);
        sql += ")";
        iter++;
      }

      this->mu_.unlock();

      return sql;
    }

    std::string CompletedQueries::query_history_table_name() {
      return FLAGS_query_history_table_name;
    }

  }
}