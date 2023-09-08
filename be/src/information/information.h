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

#pragma once

#include <memory>
#include <mutex>
#include <forward_list>
#include <string>

namespace impala {
  namespace information {

    struct CompletedQuery {
      std::string   query_id;     // Impala assigned query identifier
      std::string   session_id;   // Impala assigned session id for the client session
      std::string   session_type; // client session type
      std::uint16_t server_port;  // server's tcp port where the client connected
    };

    class CompletedQueries {
      public:
        CompletedQueries();
        void TransferFrom(std::shared_ptr<CompletedQueries> other);
        std::shared_ptr<CompletedQuery> Pop();
        void Push(std::shared_ptr<CompletedQuery> query);
        void Push(CompletedQuery& query);
        bool Empty() const;
        std::string BuildInsertSQL();

      private:
        mutable std::recursive_mutex mu_;
        std::forward_list<std::shared_ptr<CompletedQuery>> queries_;
        std::string query_history_table_name();
    };

  }
}