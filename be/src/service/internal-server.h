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

#include <boost/random/random_device.hpp>
#include <boost/uuid/random_generator.hpp>

#include "common/status.h"
#include "gen-cpp/Results_types.h"
#include "gen-cpp/Types_types.h"
#include "rpc/thrift-server.h"
#include "runtime/query-driver.h"
#include "service/client-request-state.h"
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
    std::mutex lock;
    TUniqueId session_id;

    /// Convenience wrapper around the `QueryHandle.FetchRows` method.  Returns all the
    /// query results in a single `std::vector`.
    ///
    /// This method locks the internal mutex.  Thus, callers must ensure they do not
    /// hold a lock on the internal mutex before calling this method.
    std::shared_ptr<std::vector<std::string>> FetchAllRowsText() {
      std::shared_ptr<std::vector<std::string>> full_row_set =
          make_shared<std::vector<std::string>>();
    
      std::lock_guard<std::mutex> l(this->lock);
      const TResultSetMetadata* results_metadata = this->handle->result_metadata();
      std::vector<std::string> row_set = std::vector<std::string>();
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
  ///
  /// Since this class directly calls ImpalaServer methods, it bypasses all authentication
  /// methods.
  ///
  /// Usage:
  ///   The easiest way is to use this class is to call the `ExecuteAndFetchAllText`
  ///   function which runs the provided sql, returns all the results, and closes the
  ///   query. This function is useful for running create/insert/update queries that do
  ///   not return many results.
  ///
  ///   The next way is to call the `ExecuteAndWait` function. This function runs the
  ///   provided sql and blocks until results become available. Retrieving the results can
  ///   be done by leveraging the `QueryHandle` created by this method. The `CloseQuery`
  ///   function must be called by clients once all results are read.
  ///
  ///   The lowest level function is `SubmitQuery`. This function starts a query running
  ///   and returns immediately. Waiting for results, retrieving results,  and closing the
  ///   query is left up to the client to do.
  class InternalServer {
    public:
      InternalServer(std::shared_ptr<ImpalaServer> impala_server);

      /// Creates a new session under the specified user and submits a query under that
      /// session. No authentication is performed. Blocks until result rows are available.
      /// Then, builds a `std::vector` containing all result rows.  Finally, cleans up the
      /// query and session.
      ///
      /// Intended for use as a convenienve method when query results are extremely small.
      ///
      /// Parameters:
      ///   `user_name` specifies the username that will be reported as running this query
      ///   `sql`       text of the sql query to run
      /// 
      /// Returns:
      ///   `std::vector<std::string>` containing all result rows from the query.
      std::shared_ptr<std::vector<std::string>> ExecuteAndFetchAllText(
          const std::string &user_name, const std::string& sql);

      /// Creates a new session under the specified user and submits a query under that
      /// session. No authentication is performed. Blocks until result rows are available.
      ///
      /// After retrieving the results, clients must call `CloseQuery` to properly close
      /// and clean up the query and session.
      ///
      /// Parameters:
      ///   `user_name` specifies the username that will be reported as running this query
      ///   `sql`       text of the sql query to run
      ///   `query`     in-out parameter that will be populated with information about
      ///               the executing query, the query lock must be available or else this
      ///               method will deadlock
      ///
      /// Returns:
      ///   `impala::Status` indicating the result of submitting the query.
      Status ExecuteAndWait(const std::string &user_name, const std::string& sql,
          InternalQuery& query);

      /// Creates a new session under the specified user and submits a query under that
      /// session. No authentication is performed. Returns immediately after the query is
      /// submitted to the coordinator.
      ///
      /// Parameters:
      ///   `user_name` specifies the username that will be reported as running this query
      ///   `sql`       text of the sql query to run
      ///   `query`     in-out parameter that will be populated with information about
      ///               the executing query, the query lock must be available or else this
      ///               method will deadlock
      ///
      /// Returns:
      ///   `impala::Status` indicating the result of submitting the query.
      Status SubmitQuery(const std::string &user_name, const std::string sql,
          InternalQuery& query);

      /// Closes and cleans up the query and its associated session.
      ///
      /// Parameters:
      ///   `query` object from the call to `SubmitQuery`, the query lock must be
      ///           available or else this method will deadlock
      void CloseQuery(InternalQuery& query);

    private:
      /// Convenience struct to store data related to individual Impala sessions
      struct SessionData {
        SessionData(std::shared_ptr<ThriftServer::ConnectionContext> _connection_context,
            std::shared_ptr<ImpalaServer::SessionState> _session_state) :
            connection_context(_connection_context), session_state(_session_state) {
          // no-op
        }

        std::shared_ptr<ThriftServer::ConnectionContext>  connection_context;
        std::shared_ptr<ImpalaServer::SessionState>       session_state;
        std::mutex                                   lock;
      }; // struct SessionData

      /// ImpalaServer that is delegated to for session and query management
      std::shared_ptr<ImpalaServer> impala_server_;

      /// UUID generator for session IDs and secrets. Uses system random device to get
      /// cryptographically secure random numbers.
      boost::uuids::basic_random_generator<boost::random_device> crypto_uuid_generator_;
      std::mutex uuid_lock_;

      /// Map of open sessions, key is the session id.
      /// Use the associated `sessions_lock_` mutex before accessing.
      map<TUniqueId, std::shared_ptr<SessionData>> sessions_;
      std::mutex sessions_lock_;

      /// Random `impala::TUniqueID` generator.
      TUniqueId RandomUUID();

      /// Convenience method that looks up a session in the `sessions_` map.
      ///
      /// Parameters:
      ///   `session_id` specifies the id of the session to retrieve from the sessions map
      ///   `erase`      if set to `true`, the session will be removed from the sessions
      ///                map after it is found
      ///
      /// Returns:
      ///   `impala::SessionData` representing the provided session id, if the session
      ///                         could not be found, will be `NULL`
      const std::shared_ptr<SessionData> GetSessionDataSafe(TUniqueId session_id,
          bool erase = false);

      /// Convenience method that initializes a session
      std::shared_ptr<SessionData> OpenSession(const std::string& user_name);

  }; // InternalServer class

}
