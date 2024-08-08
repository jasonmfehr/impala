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

#include <mutex>
#include <string>

namespace impala {

namespace workload_management {

enum InitState {
  // Initial state.
  NOT_RUNNING,

  // The lead coordinator is being negotiated.
  DETERMINE_LEADER,

  // The current coordinator is the lead and is running initialization.
  LEADER_INIT_RUNNING,

  // The current coordinator is the lead and has completed its initialization.
  LEADER_INIT_DONE,

  // The current coordinator is not the lead and it waiting for the lead to finish init.
  NONLEADER_INIT_WAITING,

  // The current coordinator is not the lead and is running initialization.
  NONLEADER_INIT_RUNNING,

  // Init process is complete.
  DONE
};

struct InitContext {
  public:
    std::mutex  guard;
    InitState   state;
    std::string id;

    InitContext() {
      state = NOT_RUNNING;
    }
}; // struct InitContext


} //namespace workload_management

} // namespace impala