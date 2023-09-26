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

#include "common/logging.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include "runtime/query-driver.h"
#include "service/impala-server.h"
#include "service/internal-server.h"
#include "service/query-result-set.h"
#include "util/debug-util.h"
#include "util/uid-util.h"

using boost::uuids::random_generator;
using boost::uuids::uuid;

namespace impala {

  InternalServer::InternalServer(std::shared_ptr<ImpalaServer> impala_server) {
    this->impala_server_ = impala_server;
    this->sessions_ = std::map<TUniqueId, shared_ptr<SessionData>>();
  }

  InternalServer::~InternalServer() {
    // currently no-op
  }

  Status InternalServer::ExecuteAndWait(const std::string& sql) {
    return Status::OK();
  }

  Status InternalServer::ExecuteAndWait(const std::string& sql,
      TUniqueId& session_id, TUniqueId& query_id) {
    return Status::OK();
  }

  TUniqueId InternalServer::OpenSession(const std::string& user_name) {
    std::shared_ptr<ThriftServer::ConnectionContext> conn_ctx =
        make_shared<ThriftServer::ConnectionContext>();
    conn_ctx->connection_id = this->RandomUUID();
    conn_ctx->server_name = this->impala_server_->BEESWAX_SERVER_NAME;
    conn_ctx->username = user_name;
    conn_ctx->network_address.hostname = "in-memory.localhost";

    this->impala_server_->ConnectionStart(*conn_ctx.get());

    TUniqueId session_id;
    {
      lock_guard<mutex> l(this->impala_server_->connection_to_sessions_map_lock_);
      session_id = *this->impala_server_->connection_to_sessions_map_[conn_ctx->connection_id].cbegin();
    }

    shared_ptr<ImpalaServer::SessionState> session_state;
    {
      lock_guard<mutex> l(this->impala_server_->session_state_map_lock_);
      session_state = this->impala_server_->session_state_map_[session_id];
    }

    this->impala_server_->MarkSessionActive(session_state);

    {
      lock_guard<mutex> l(this->sessions_lock_);
      this->sessions_.insert(make_pair(session_id,
          make_shared<SessionData>(conn_ctx, session_state)));
    }

    return session_id;
  }

  void InternalServer::CloseSession(const TUniqueId& session_id) {
    shared_ptr<SessionData> session_data;

    {
      lock_guard<mutex> l(this->sessions_lock_);
      auto sd = this->sessions_.find(session_id);
      if (sd == this->sessions_.end()) {
        LOG(INFO) << "Attempted to close session " << PrintId(session_id) << 
            " but session was not found";
        return ;
      } else {
        session_data = sd->second;
        this->sessions_.erase(sd);
      }

      this->impala_server_->MarkSessionInactive(session_data->session_state);
      this->impala_server_->ConnectionEnd(*(session_data->connection_context.get()));
    }
  }

  Status InternalServer::SubmitQuery(const std::string sql, const TUniqueId session_id,
      const TUniqueId& query_id) {
    
    std::shared_ptr<SessionData> session_data = this->GetSessionDataSafe(session_id);
    if (session_data == NULL) {
      return Status::OK(); //todo - something else
    }
    // build a query context
    TQueryCtx query_context;
    query_context.client_request.stmt = "create table if not exists default.foo(id INT) stored as iceberg";
    session_data->session_state->ToThrift(session_id, &query_context.session);

    Status stat;
    std::string stat_msg;

    // build a query handle
    QueryHandle handle;

    ABORT_IF_ERROR(this->impala_server_->Execute(&query_context,
        session_data->session_state, &handle, nullptr));

    ABORT_IF_ERROR(this->impala_server_->SetQueryInflight(session_data->session_state,
        handle));

    return Status::OK();
  }

  Status InternalServer::WaitForQuery(const TUniqueId& query_id) {
    return Status::OK();
  }

  Status InternalServer::FetchRows(const int32_t max_rows, QueryResultSet* fetched_rows,
      const int64_t block_on_wait_time_us) {
    return Status::OK();
  }

  Status InternalServer::CloseQuery(const TUniqueId query_id) {
    return Status::OK();
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

  const std::shared_ptr<InternalServer::SessionData> InternalServer::GetSessionDataSafe
      (TUniqueId session_id) {
    shared_ptr<SessionData> session_data;

    {
      lock_guard<mutex> l(this->sessions_lock_);
      auto sd = this->sessions_.find(session_id);
      if (sd == this->sessions_.end()) {
        LOG(INFO) << "Attempted to close session " << PrintId(session_id) << 
            " but session was not found";
        return NULL;
      } else {
        session_data = sd->second;
        this->sessions_.erase(sd);
      }
    }

    return session_data;
  }

} // namespace impala