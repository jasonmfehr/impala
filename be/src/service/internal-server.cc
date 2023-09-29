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

#include <memory>
#include <mutex>
#include <string>

#include <boost/uuid/uuid.hpp>

#include "common/status.h"
#include "gen-cpp/Query_types.h"
#include "gen-cpp/Types_types.h"
#include "rpc/thrift-server.h"
#include "service/client-request-state.h"
#include "service/impala-server.h"
#include "service/internal-server.h"
#include "util/uid-util.h"

using namespace std;
using boost::uuids::uuid;

namespace impala {

  InternalServer::InternalServer(shared_ptr<ImpalaServer> impala_server) {
    this->impala_server_ = impala_server;
  }

  const Status InternalServer::ExecuteAndFetchAllText(const string &user_name,
      const string& sql, std::shared_ptr<std::vector<std::string>>& results) {
    InternalQuery query;
    RETURN_IF_ERROR(this->ExecuteAndWait(user_name, sql, query));

    shared_ptr<vector<string>> full_row_set = query.FetchAllRowsText();
    this->CloseQuery(query);

    results->insert(results->cend(), full_row_set->cbegin(), full_row_set->cend());

    return Status::OK();
  }

  const Status InternalServer::ExecuteAndWait(const string &user_name, const string& sql,
      InternalQuery& query) {
    RETURN_IF_ERROR(this->SubmitQuery(user_name, sql, query));

    {
      lock_guard<mutex> l(query.lock);
      query.handle->Wait();
    }

    return Status::OK();
  }

  const Status InternalServer::SubmitQuery(const string &user_name, const string sql,
      InternalQuery& query) {
    
    shared_ptr<SessionData> session_data = this->OpenSession(user_name);
    query.session_id = session_data->session_state->session_id;
    
    // build a query context
    TQueryCtx query_context;
    query_context.client_request.stmt = sql;

    {
      lock_guard<mutex> l(session_data->lock);
      lock_guard<mutex> l2(query.lock);

      session_data->session_state->ToThrift(session_data->session_state->session_id,
          &query_context.session);

      RETURN_IF_ERROR(this->impala_server_->Execute(&query_context,
          session_data->session_state, &query.handle, nullptr));

      RETURN_IF_ERROR(this->impala_server_->SetQueryInflight(
          session_data->session_state, query.handle));
    }

    return Status::OK();
  }

  shared_ptr<InternalServer::SessionData> InternalServer::OpenSession(
      const string& user_name) {
    shared_ptr<ThriftServer::ConnectionContext> conn_ctx =
        make_shared<ThriftServer::ConnectionContext>();
    conn_ctx->connection_id = this->RandomUUID();
    conn_ctx->server_name = this->impala_server_->BEESWAX_SERVER_NAME;
    conn_ctx->username = user_name;
    conn_ctx->network_address.hostname = "in-memory.localhost";

    this->impala_server_->ConnectionStart(*conn_ctx.get());

    TUniqueId session_id;
    {
      lock_guard<mutex> l(this->impala_server_->connection_to_sessions_map_lock_);
      session_id = *this->impala_server_->
          connection_to_sessions_map_[conn_ctx->connection_id].cbegin();
    }

    shared_ptr<ImpalaServer::SessionState> session_state;
    {
      lock_guard<mutex> l(this->impala_server_->session_state_map_lock_);
      session_state = this->impala_server_->session_state_map_[session_id];
    }

    this->impala_server_->MarkSessionActive(session_state);

    shared_ptr<SessionData> session_data = make_shared<SessionData>(conn_ctx,
        session_state);

    {
      lock_guard<mutex> l(this->sessions_lock_);
      this->sessions_.insert(make_pair(session_id, session_data));
    }

    return session_data;
  }

  const bool InternalServer::CloseQuery(const InternalQuery& query) {
    shared_ptr<SessionData> session_data;

    if (this->GetSessionDataSafe(query.session_id, session_data, true)) {
      lock_guard<mutex> l(session_data->lock);
      lock_guard<mutex> l2(query.lock);
      this->impala_server_->MarkSessionInactive(session_data->session_state);
      this->impala_server_->ConnectionEnd(*session_data->connection_context.get());

      return true;
    }

    return false;
  }

  TUniqueId InternalServer::RandomUUID() {
     uuid conn_uuid;
    {
      lock_guard<mutex> l(this->uuid_lock_);
      conn_uuid = this->crypto_uuid_generator_();
    }
    TUniqueId conn_id;
    UUIDToTUniqueId(conn_uuid, &conn_id);

    return conn_id;
  }

  const bool InternalServer::GetSessionDataSafe(const TUniqueId session_id,
      std::shared_ptr<SessionData>& session_data, const bool erase) {
    lock_guard<mutex> l(this->sessions_lock_);

    const auto sd = this->sessions_.find(session_id);
    if (sd == this->sessions_.end()) {
      return false;
    }
      
    session_data = sd->second;
    if (erase) {
      this->sessions_.erase(sd);
    }

    return true;
  }

} // namespace impala