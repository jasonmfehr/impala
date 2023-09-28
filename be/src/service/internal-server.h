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
  /// Thin wrapper around a QueryHandle that enables coordinated access to a query that
  /// was started by an InternalServer.
  ///
  /// When directly accessing the `handle` member, first lock the mutex:
  /// `std::lock_guard<std::mutex> l(internal_query.lock);`
  struct InternalQuery {
    QueryHandle handle;
    mutex lock;
    TUniqueId session_id;

    /// Convenience wrapper around the `QueryHandle.FetchRows` method.  Returns all the
    /// query results in a single `std::vector`.
    ///
    /// This method locks the internal mutex.  Thus, callers must ensure they do not
    /// hold a lock on the internal mutex before calling this method.
    shared_ptr<vector<string>> FetchAllRowsText() {
      shared_ptr<vector<string>> full_row_set = make_shared<vector<string>>();
    
      lock_guard<mutex> l(this->lock);
      auto results_metadata = this->handle->result_metadata();
      vector<string> row_set = vector<string>();
      QueryResultSet* result_set = QueryResultSet::CreateAsciiQueryResultSet(
          *results_metadata, &row_set, true);
      int64_t block_wait_time = 30000000;

      while (!this->handle->eos()) {
        ABORT_IF_ERROR(handle->FetchRows(10, result_set, block_wait_time));
        full_row_set->insert(full_row_set->cend(), row_set.cbegin(), row_set.cend());
      }

      return full_row_set;
    }
  };

  /// Enables Impala coordinators to submit queries to themselves.
  ///
  /// Internally, this class directly calls the methods on ImpalaServer that are called by
  /// the Beeswax and HS2 servers.  Thus, it does not strictly adhere to either protocol.
  /// Since Impala requires sessions to have a defined type, sessions created by
  /// InternalServer show up as Beeswax sessions. Since sessions are considered Beeswax
  /// sessions, they also only support a single query per session. Even though there is a
  /// one-to-one relationship between sessions and queries, each has its own distinct id.
  class InternalServer {
    public:
      InternalServer(shared_ptr<ImpalaServer> impala_server);

      /// Creates a new session under the specified user and submits a query under that
      /// session. Blocks until result rows are available.
      ///
      /// Returns a Status indicating the result of submitting the query.
      Status ExecuteAndWait(const string &user_name, const string& sql,
          InternalQuery& query);

      ///
      shared_ptr<vector<string>> ExecuteAndFetchAllText(const string &user_name,
          const string& sql);

      Status SubmitQuery(const string &user_name, const string sql, InternalQuery& query);

      void CloseQuery(InternalQuery& query);

    private:

      struct SessionData {
        SessionData(shared_ptr<ThriftServer::ConnectionContext> _connection_context,
            shared_ptr<ImpalaServer::SessionState> _session_state) :
            connection_context(_connection_context), session_state(_session_state) {
          // no-op
        }

        shared_ptr<ThriftServer::ConnectionContext>  connection_context;
        shared_ptr<ImpalaServer::SessionState>       session_state;
        mutex                                        lock;
      }; // struct SessionData

      shared_ptr<ImpalaServer> impala_server_;

      // UUID generator for session IDs and secrets. Uses system random device to get
      // cryptographically secure random numbers.
      boost::uuids::basic_random_generator<boost::random_device> crypto_uuid_generator_;
      mutex uuid_lock_;

      // map of open sessions, key is the session id
      map<TUniqueId, shared_ptr<SessionData>> sessions_;
      mutex sessions_lock_;

      TUniqueId RandomUUID();
      const shared_ptr<SessionData> GetSessionDataSafe(TUniqueId session_id,
          bool erase = false);
      shared_ptr<SessionData> OpenSession(const string& user_name);

  }; // InternalServer class

}
