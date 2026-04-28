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

#include "runtime/mem-pool.h"

namespace impala {

class MemPoolResetting : public MemPoolIface {
 public:
  MemPoolResetting(MemPool* decorated_pool, const int64_t max_bytes) :
      decorated_pool_(decorated_pool),
      max_bytes_(max_bytes) {}

  uint8_t* Allocate(int64_t size) noexcept override {
    FreeIfNeeded(size);
    return decorated_pool_->Allocate(size);
  }

  uint8_t* TryAllocate(int64_t size) noexcept override {
    FreeIfNeeded(size);
    return decorated_pool_->TryAllocate(size);
  }

  uint8_t* TryAllocateAligned(int64_t size, int alignment) noexcept override {
    FreeIfNeeded(size);
    return decorated_pool_->TryAllocateAligned(size, alignment);
  }

  uint8_t* TryAllocateUnaligned(int64_t size) noexcept override {
    FreeIfNeeded(size);
    return decorated_pool_->TryAllocateUnaligned(size);
  }

  void ReturnPartialAllocation(int64_t byte_size) override {
    return decorated_pool_->ReturnPartialAllocation(byte_size);
  }

  void Clear() override {
    decorated_pool_->Clear();
  }

  void FreeAll() override {
    decorated_pool_->FreeAll();
  }

  void AcquireData(MemPool* src, bool keep_current) override {
    decorated_pool_->AcquireData(src, keep_current);
  }

  void SetMemTracker(MemTracker* new_tracker) override {
    decorated_pool_->SetMemTracker(new_tracker);
  }

  std::string DebugString() override {
    return decorated_pool_->DebugString();
  }

  int64_t total_allocated_bytes() const override {
    return decorated_pool_->total_allocated_bytes();
  }

  int64_t total_reserved_bytes() const override {
    return decorated_pool_->total_reserved_bytes();
  }

  MemTracker* mem_tracker() override {
    return decorated_pool_->mem_tracker();
  }

  int64_t GetTotalChunkSizes() const override {
    return decorated_pool_->GetTotalChunkSizes();
  }

  MemPoolCounters GetMemPoolCounters() const override {
    return decorated_pool_->GetMemPoolCounters();
  }

 private:
  MemPool* decorated_pool_;
  const int64_t max_bytes_;

  void FreeIfNeeded(const int64_t size) {
    if (decorated_pool_->total_allocated_bytes() + size > max_bytes_) {
      decorated_pool_->FreeAll();
    }
  }
   
}; // class MemPoolResetting

} // namespace impala