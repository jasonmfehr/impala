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

#include "common/compiler-util.h"
#include "runtime/mem-pool.h"

#pragma once

namespace impala {
class ResettingMemPool : public MemPool {
public:
  ResettingMemPool(MemPool* parent, const int64_t reset_threshold)
      : MemPool(parent->mem_tracker()),
        parent_(parent),
        reset_threshold_(reset_threshold),
        consumption(parent_->mem_tracker()->consumption()) {
  }

  uint8_t* Allocate(int64_t size) noexcept override {
    if (UNLIKELY(consumption > reset_threshold_)) {
      parent_->FreeAll();
      consumption = 0;
    }

    uint8_t* res = parent_->Allocate(size);

    if (LIKELY(res != NULL)) {
      consumption += size;
    }

    return res;
  }
private:
  MemPool* parent_;
  const int64_t reset_threshold_;
  int64_t consumption;

}; // class ResettingMemPool

} // namespace impala
