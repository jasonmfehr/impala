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

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include "rpc/thrift-server.h"
#include "service/impala-server.h"
#include "service/query-result-set.h"

namespace impala {

  class InternalServer {
    public:
      InternalServer(std::shared_ptr<ImpalaServer> impala_server);
      ~InternalServer();

      // Runs the entire lifecycle of session creation, query submission, query execution,
      // query cleanup, and session close. Blocks until the entire lifecycle has completed
      // or an error has occurred.  Results cannot be retrieved.
      //
      // Returns the overall status of executing the query.
      Status ExecuteAndWait(const std::string& sql);
      Status ExecuteAndWait(const std::string& sql, TUniqueId& session_id,
          TUniqueId& query_id);

      TUniqueId OpenSession(const std::string& user_name = "impala");
      void CloseSession(const TUniqueId& session_id);

      Status SubmitQuery(const std::string sql, const TUniqueId session_id,
          TUniqueId& query_id);
      Status WaitForQuery(const TUniqueId& query_id);
      Status FetchRows(const TUniqueId& query_id, const int32_t max_rows,
          QueryResultSet* fetched_rows, const int64_t block_on_wait_time_us);
      Status CloseQuery(const TUniqueId query_id);

    private:

      struct SessionData {
        SessionData(std::shared_ptr<ThriftServer::ConnectionContext> _connection_context,
            std::shared_ptr<ImpalaServer::SessionState> _session_state) :
            connection_context(_connection_context), session_state(_session_state) {
          // no-op
        }

        std::shared_ptr<ThriftServer::ConnectionContext>  connection_context;
        std::shared_ptr<ImpalaServer::SessionState>       session_state;
        std::set<TUniqueId>                               running_queries; // TODO - this can be removed if impala_server automatically closes queries when a session is closed
        std::mutex                                        lock;
      }; // struct SessionData

      struct QueryData {
        QueryData(const TUniqueId& _session_id, std::shared_ptr<QueryHandle> _query_handle) :
            session_id(_session_id), query_handle(_query_handle) {
          // no-op
        }

        const TUniqueId session_id; // TODO - this can be removed if impala_server automatically closes queries when a session is closed
        std::shared_ptr<QueryHandle> query_handle;
        std::mutex lock;
        // TODO - track query state here so we don't try to FetchRows on a closed query
      }; // struct QueryData

      std::shared_ptr<ImpalaServer> impala_server_;

      // UUID generator for session IDs and secrets. Uses system random device to get
      // cryptographically secure random numbers.
      boost::uuids::basic_random_generator<boost::random_device> crypto_uuid_generator_;
      std::mutex uuid_lock_;

      // map of open sessions, key is the session id
      std::map<TUniqueId, std::shared_ptr<SessionData>> sessions_;
      std::mutex sessions_lock_;

      // map of query id to query handle to enable quicker query handle lookup
      // TODO - need a way of locking individual handles
      std::map<TUniqueId, std::shared_ptr<QueryData>> queries_index_;
      std::mutex queries_index_lock_;

      TUniqueId RandomUUID();
      const std::shared_ptr<SessionData> GetSessionDataSafe(TUniqueId session_id);
      const std::shared_ptr<QueryData> GetQuerySafe(const TUniqueId& query_id);
      

  }; // InternalServer class

}
