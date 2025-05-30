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

#include "runtime/bufferpool/buffer-allocator.h"

#include <mutex>

#include <boost/bind.hpp>

#include "common/atomic.h"
#include "runtime/bufferpool/system-allocator.h"
#include "util/bit-util.h"
#include "util/cpu-info.h"
#include "util/histogram-metric.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

// Collect statistics once ALLOC_STAT_SAMPLE_RATE allocations. This allows collecting
// various useful statistics that would be too expensive to update every allocation.
// This should be kept as a power-of-two to allow efficient mod calculation.
static constexpr int64_t ALLOC_STAT_SAMPLE_RATE = 64;

// Maximum buffer size for the purposes of histogram sizing in stats collection. Buffers
// > 4GB are extremely rare so we don't need to collect precise stats on them.
static constexpr int64_t STATS_MAX_BUFFER_SIZE = 4L * 1024L * 1024L * 1024L;

/// Decrease 'bytes_remaining' by up to 'max_decrease', down to a minimum of 0.
/// If 'require_full_decrease' is true, only decrease if we can decrease it
/// 'max_decrease'. Returns the amount it was decreased by.
static int64_t DecreaseBytesRemaining(
    int64_t max_decrease, bool require_full_decrease, AtomicInt64* bytes_remaining);

/// An arena containing free buffers and clean pages that are associated with a
/// particular core. All public methods are thread-safe.
class BufferPool::FreeBufferArena : public CacheLineAligned {
 public:
  FreeBufferArena(
      BufferAllocator* parent, MetricGroup* metrics, const std::string& arena_name);

  // Destructor should only run in backend tests.
  ~FreeBufferArena();

  /// Add a free buffer to the free lists. May free buffers to the system allocator
  /// if the list becomes full. Caller should not hold 'lock_'
  void AddFreeBuffer(BufferHandle&& buffer);

  /// Try to get a free buffer of 'buffer_len' bytes from this arena. Returns true and
  /// sets 'buffer' if found or false if not found. Caller should not hold 'lock_'.
  bool PopFreeBuffer(int64_t buffer_len, BufferHandle* buffer);

  /// Try to get a buffer of 'buffer_len' bytes from this arena by evicting a clean page.
  /// Returns true and sets 'buffer' if a clean page was evicted or false otherwise.
  /// Caller should not hold 'lock_'
  bool EvictCleanPage(int64_t buffer_len, BufferHandle* buffer);

  /// Try to free 'target_bytes' of memory from this arena back to the system allocator.
  /// Up to 'target_bytes_to_claim' will be given back to the caller, so it can allocate
  /// a buffer of that size from the system. Any bytes freed in excess of
  /// 'target_bytes_to_claim' are added to 'system_bytes_remaining_'. Returns the actual
  /// number of bytes freed and the actual number of bytes claimed.
  ///
  /// Caller should not hold 'lock_'. If 'arena_lock' is non-null, ownership of the
  /// arena lock is transferred to the caller.
  pair<int64_t, int64_t> FreeSystemMemory(int64_t target_bytes_to_free,
      int64_t target_bytes_to_claim, std::unique_lock<SpinLock>* arena_lock);

  /// Add a clean page to the arena. Caller must hold the page's client's lock and not
  /// hold 'lock_' or any Page::lock_.
  void AddCleanPage(Page* page);

  /// Removes the clean page from the arena if present. Returns true if removed. If
  /// 'claim_buffer' is true, the buffer is returned with the page, otherwise it is
  /// added to the free buffer list. Caller must hold the page's client's lock and
  /// not hold 'lock_' or any Page::lock_.
  bool RemoveCleanPage(bool claim_buffer, Page* page);

  /// Called periodically. Shrinks free lists that are holding onto more memory than
  /// needed.
  void Maintenance();

  /// Test helper: gets the current size of the free list for buffers of 'len' bytes
  /// on core 'core'.
  int GetFreeListSize(int64_t len);

  /// Return the total number of free buffers in the arena. May be approximate since
  /// it doesn't acquire the arena lock.
  int64_t GetNumFreeBuffers();

  /// Return the total bytes of free buffers in the arena. May be approximate since
  /// it doesn't acquire the arena lock.
  int64_t GetFreeBufferBytes();

  /// Return the total number of clean pages in the arena. May be approximate since
  /// it doesn't acquire the arena lock.
  int64_t GetNumCleanPages();

  string DebugString();

  IntCounter* system_alloc_time() const { return system_alloc_time_; }
  IntCounter* local_arena_free_buffer_hits() const {
    return local_arena_free_buffer_hits_;
  }
  IntCounter* direct_alloc_count() const { return direct_alloc_count_; }
  HistogramMetric* buffer_size_stats() const { return buffer_size_stats_; }
  IntCounter* numa_arena_free_buffer_hits() const { return numa_arena_free_buffer_hits_; }
  IntCounter* clean_page_hits() const { return clean_page_hits_; }
  IntCounter* num_scavenges() const { return num_scavenges_; }
  IntCounter* num_final_scavenges() const { return num_final_scavenges_; }

 private:
  /// The data structures for each power-of-two size of buffers/pages.
  /// All members are protected by FreeBufferArena::lock_ unless otherwise mentioned.
  struct PerSizeLists {
    PerSizeLists() : num_free_buffers(0), low_water_mark(0), num_clean_pages(0) {}

    /// Helper to add a free buffer and increment the counter.
    /// FreeBufferArena::lock_ must be held by the caller.
    void AddFreeBuffer(BufferHandle&& buffer) {
      DCHECK_EQ(num_free_buffers.Load(), free_buffers.Size());
      num_free_buffers.Add(1);
      free_buffers.AddFreeBuffer(move(buffer));
    }

    /// The number of entries in 'free_buffers'. Can be read without holding a lock to
    /// allow threads to quickly skip over empty lists when trying to find a buffer.
    AtomicInt64 num_free_buffers;

    /// Buffers that are not in use that were originally allocated on the core
    /// corresponding to this arena.
    FreeList free_buffers;

    /// The minimum size of 'free_buffers' since the last Maintenance() call.
    int low_water_mark;

    /// The number of entries in 'clean_pages'.
    /// Can be read without holding a lock to allow threads to quickly skip over empty
    /// lists when trying to find a buffer in a different arena.
    AtomicInt64 num_clean_pages;

    /// Unpinned pages that have had their contents written to disk. These pages can be
    /// evicted to reclaim a buffer for any client. Pages are evicted in FIFO order,
    /// so that pages are evicted in approximately the same order that the clients wrote
    /// them to disk. Protected by FreeBufferArena::lock_.
    InternalList<Page> clean_pages;
  };

  /// Return the number of buffer sizes for this allocator.
  int NumBufferSizes() const {
    return parent_->log_max_buffer_len_ - parent_->log_min_buffer_len_ + 1;
  }

  /// Return the lists of buffers for buffers of the given length.
  PerSizeLists* GetListsForSize(int64_t buffer_len) {
    DCHECK(BitUtil::IsPowerOf2(buffer_len));
    int idx = BitUtil::Log2Ceiling64(buffer_len) - parent_->log_min_buffer_len_;
    DCHECK_LT(idx, NumBufferSizes());
    return &buffer_sizes_[idx];
  }

  /// Compute a sum over all the lists in the arena. Does not lock the arena.
  int64_t SumOverSizes(
      const std::function<int64_t(PerSizeLists* lists, int64_t buffer_size)>& compute_fn);

  BufferAllocator* const parent_;

  /// Protects all data structures in the arena. See buffer-pool-internal.h for lock
  /// order.
  SpinLock lock_;

  /// Free buffers and clean pages for each buffer size for this arena.
  /// Indexed by log2(bytes) - log2(min_buffer_len_).
  PerSizeLists buffer_sizes_[LOG_MAX_BUFFER_BYTES + 1];

  // Total time spent in the system allocator.
  IntCounter* const system_alloc_time_;

  // Counts the number of free buffer hits in the local arena of the current core.
  IntCounter* const local_arena_free_buffer_hits_;

  // Counts the number the allocator directly allocates a new buffer on the current core.
  IntCounter* const direct_alloc_count_;

  // Statistics about the buffer sizes allocated from the system allocator.
  HistogramMetric* const buffer_size_stats_;

  // Counts the number of hits in an arena for the same NUMA node as the current core.
  IntCounter* const numa_arena_free_buffer_hits_;

  // Counts the number of times a page of the right size was evicted.
  IntCounter* const clean_page_hits_;

  // Counts the number of times we had to scavenge buffers.
  IntCounter* const num_scavenges_;

  // Counts the number of times we had to lock all arenas for a final scavenge of buffers.
  IntCounter* const num_final_scavenges_;
};

int64_t BufferPool::BufferAllocator::CalcMaxBufferLen(
    int64_t min_buffer_len, int64_t system_bytes_limit) {
  // Find largest power of 2 smaller than 'system_bytes_limit'.
  int64_t upper_bound = system_bytes_limit == 0 ? 1L : 1L
          << BitUtil::Log2Floor64(system_bytes_limit);
  upper_bound = min(MAX_BUFFER_BYTES, upper_bound);
  return max(min_buffer_len, upper_bound); // Can't be < min_buffer_len.
}

BufferPool::BufferAllocator::BufferAllocator(BufferPool* pool, MetricGroup* metrics,
    int64_t min_buffer_len, int64_t system_bytes_limit, int64_t clean_page_bytes_limit)
  : pool_(pool),
    system_allocator_(new SystemAllocator(min_buffer_len)),
    min_buffer_len_(min_buffer_len),
    max_buffer_len_(CalcMaxBufferLen(min_buffer_len, system_bytes_limit)),
    log_min_buffer_len_(BitUtil::Log2Ceiling64(min_buffer_len_)),
    log_max_buffer_len_(BitUtil::Log2Ceiling64(max_buffer_len_)),
    system_bytes_limit_(system_bytes_limit),
    system_bytes_remaining_(system_bytes_limit),
    clean_page_bytes_limit_(clean_page_bytes_limit),
    clean_page_bytes_remaining_(clean_page_bytes_limit),
    per_core_arenas_(CpuInfo::GetMaxNumCores()),
    max_scavenge_attempts_(MAX_SCAVENGE_ATTEMPTS) {
  DCHECK(BitUtil::IsPowerOf2(min_buffer_len_)) << min_buffer_len_;
  DCHECK(BitUtil::IsPowerOf2(max_buffer_len_)) << max_buffer_len_;
  DCHECK_LE(0, min_buffer_len_);
  DCHECK_LE(min_buffer_len_, max_buffer_len_);
  DCHECK_LE(max_buffer_len_, MAX_BUFFER_BYTES);
  DCHECK_LE(max_buffer_len_, max(system_bytes_limit_, min_buffer_len_));

  MetricGroup* buffer_pool_metrics = metrics->GetOrCreateChildGroup("buffer-pool");
  for (int i = 0; i < per_core_arenas_.size(); ++i) {
    per_core_arenas_[i].reset(
        new FreeBufferArena(this, buffer_pool_metrics, Substitute("arena-$0", i)));
  }
}

BufferPool::BufferAllocator::~BufferAllocator() {
  per_core_arenas_.clear(); // Release all the memory.
  // Check for accounting leaks.
  DCHECK_EQ(system_bytes_limit_, system_bytes_remaining_.Load());
  DCHECK_EQ(clean_page_bytes_limit_, clean_page_bytes_remaining_.Load());
}

Status BufferPool::BufferAllocator::Allocate(
    ClientHandle* client, int64_t len, BufferHandle* buffer) {
  SCOPED_TIMER(client->impl_->counters().alloc_time);
  COUNTER_ADD(client->impl_->counters().cumulative_bytes_alloced, len);
  COUNTER_ADD(client->impl_->counters().cumulative_allocations, 1);

  RETURN_IF_ERROR(AllocateInternal(client->impl_, len, buffer));
  DCHECK(buffer->is_open());
  buffer->client_ = client;
  return Status::OK();
}

Status BufferPool::BufferAllocator::AllocateInternal(
    BufferPool::Client* client, int64_t len, BufferHandle* buffer) {
  DCHECK(!buffer->is_open());
  DCHECK_GE(len, min_buffer_len_);
  DCHECK(BitUtil::IsPowerOf2(len)) << len;

  if (UNLIKELY(len > MAX_BUFFER_BYTES)) {
    return Status(Substitute(
        "Tried to allocate buffer of $0 bytes > max of $1 bytes", len, MAX_BUFFER_BYTES));
  }
  if (UNLIKELY(len > system_bytes_limit_)) {
    return Status(Substitute("Tried to allocate buffer of $0 bytes > buffer pool limit "
        "of $1 bytes", len, system_bytes_limit_));
  }

  const int current_core = CpuInfo::GetCurrentCore();
  // Fast path: recycle a buffer of the correct size from this core's arena.
  FreeBufferArena* current_core_arena = per_core_arenas_[current_core].get();
  if (current_core_arena->PopFreeBuffer(len, buffer)) {
    current_core_arena->local_arena_free_buffer_hits()->Increment(1);
    return Status::OK();
  }

  // Fast-ish path: allocate a new buffer if there is room in 'system_bytes_remaining_'.
  int64_t delta = DecreaseBytesRemaining(len, true, &system_bytes_remaining_);
  // Whether to record stats about the system alloc (we don't want to do this every
  // allocation because of the overhead).
  bool sample_sys_alloc_stats = false;
  if (delta == len) {
    int64_t count = current_core_arena->direct_alloc_count()->Increment(1);
    sample_sys_alloc_stats = count % ALLOC_STAT_SAMPLE_RATE == 0;
  } else {
    DCHECK_EQ(0, delta);
    const vector<int>& numa_node_cores = CpuInfo::GetCoresOfSameNumaNode(current_core);
    const int numa_node_core_idx = CpuInfo::GetNumaNodeCoreIdx(current_core);

    // Fast-ish path: find a buffer of the right size from another core on the same
    // NUMA node. Avoid getting a buffer from another NUMA node - prefer reclaiming
    // a clean page on this NUMA node or scavenging then reallocating a new buffer.
    // We don't want to get into a state where allocations between the nodes are
    // unbalanced and one node is stuck reusing memory allocated on the other node.
    for (int i = 1; i < numa_node_cores.size(); ++i) {
      // Each core should start searching from a different point to avoid hot-spots.
      int other_core = numa_node_cores[(numa_node_core_idx + i) % numa_node_cores.size()];
      FreeBufferArena* other_core_arena = per_core_arenas_[other_core].get();
      if (other_core_arena->PopFreeBuffer(len, buffer)) {
        current_core_arena->numa_arena_free_buffer_hits()->Increment(1);
        return Status::OK();
      }
    }

    // Fast-ish path: evict a clean page of the right size from the current NUMA node.
    for (int i = 0; i < numa_node_cores.size(); ++i) {
      int other_core = numa_node_cores[(numa_node_core_idx + i) % numa_node_cores.size()];
      FreeBufferArena* other_core_arena = per_core_arenas_[other_core].get();
      if (other_core_arena->EvictCleanPage(len, buffer)) {
        current_core_arena->clean_page_hits()->Increment(1);
        return Status::OK();
      }
    }

    // Slow path: scavenge buffers of different sizes from free buffer lists and clean
    // pages. Make initial, fast attempts to gather the required buffers, before
    // finally making a slower, but guaranteed-to-succeed attempt.
    // TODO: IMPALA-4703: add a stress option where we vary the number of attempts
    // randomly.
    int attempt = 0;
    int64_t count = current_core_arena->num_scavenges()->Increment(1);
    sample_sys_alloc_stats = count % ALLOC_STAT_SAMPLE_RATE == 0;
    while (attempt < max_scavenge_attempts_ && delta < len) {
      bool final_attempt = attempt == max_scavenge_attempts_ - 1;
      if (final_attempt) current_core_arena->num_final_scavenges()->Increment(1);
      delta += ScavengeBuffers(final_attempt, current_core, len - delta);
      ++attempt;
    }
    if (delta < len) {
      system_bytes_remaining_.Add(delta);
      // This indicates an accounting bug - we should be able to always get the memory.
      return Status(TErrorCode::INTERNAL_ERROR, Substitute(
          "Could not allocate $0 bytes: was only able to free up $1 bytes after $2 "
          "attempts:\n$3", len, delta, max_scavenge_attempts_, pool_->DebugString()));
    }
  }
  // We have headroom to allocate a new buffer at this point.
  DCHECK_EQ(delta, len);
  MonotonicStopWatch sys_alloc_sw;
  sys_alloc_sw.Start();
  Status status = system_allocator_->Allocate(len, buffer);
  if (!status.ok()) {
    system_bytes_remaining_.Add(len);
    return status;
  }
  int64_t sys_alloc_time = sys_alloc_sw.ElapsedTime();
  if (sample_sys_alloc_stats) current_core_arena->buffer_size_stats()->Update(len);
  current_core_arena->system_alloc_time()->Increment(sys_alloc_time);
  client->counters().sys_alloc_time->Add(sys_alloc_time);
  return Status::OK();
}

int64_t DecreaseBytesRemaining(
    int64_t max_decrease, bool require_full_decrease, AtomicInt64* bytes_remaining) {
  while (true) {
    int64_t old_value = bytes_remaining->Load();
    if (require_full_decrease && old_value < max_decrease) return 0;
    int64_t decrease = min(old_value, max_decrease);
    int64_t new_value = old_value - decrease;
    if (bytes_remaining->CompareAndSwap(old_value, new_value)) {
      return decrease;
    }
  }
}

int64_t BufferPool::BufferAllocator::ScavengeBuffers(
    bool slow_but_sure, int current_core, int64_t target_bytes) {
  // There are two strategies for scavenging buffers:
  // 1) Fast, opportunistic: Each arena is searched in succession. Although reservations
  //    guarantee that the memory we need is available somewhere, this may fail if we
  //    we race with another thread that returned buffers to an arena that we've already
  //    searched and took the buffers from an arena we haven't yet searched.
  // 2) Slow, guaranteed to succeed: In order to ensure that we can find the memory in a
  //    single pass, we hold locks for all arenas we've already examined. That way, other
  //    threads can't take the memory that we need from an arena that we haven't yet
  //    examined (or from 'system_bytes_available_') because in order to do so, it would
  //    have had to return the equivalent amount of memory to an earlier arena or added
  //    it back into 'systems_bytes_reamining_'. The former can't happen since we're
  //    still holding those locks, and the latter is solved by trying to decrease
  //    system_bytes_remaining_ with DecreaseBytesRemaining() at the end.
  DCHECK_GT(target_bytes, 0);
  // First make sure we've used up all the headroom in the buffer limit.
  int64_t bytes_found =
      DecreaseBytesRemaining(target_bytes, false, &system_bytes_remaining_);
  if (bytes_found == target_bytes) return bytes_found;

  // In 'slow_but_sure' mode, we will hold locks for multiple arenas at the same time and
  // therefore must start at 0 to respect the lock order. Otherwise we start with the
  // current core's arena for locality and to avoid excessive contention on arena 0.
  int start_core = slow_but_sure ? 0 : current_core;
  vector<std::unique_lock<SpinLock>> arena_locks;
  if (slow_but_sure) arena_locks.resize(per_core_arenas_.size());

  for (int i = 0; i < per_core_arenas_.size(); ++i) {
    int core_to_check = (start_core + i) % per_core_arenas_.size();
    FreeBufferArena* arena = per_core_arenas_[core_to_check].get();
    int64_t bytes_needed = target_bytes - bytes_found;
    bytes_found += arena->FreeSystemMemory(bytes_needed, bytes_needed,
         slow_but_sure ? &arena_locks[i] : nullptr).second;
    if (bytes_found == target_bytes) break;
  }
  DCHECK_LE(bytes_found, target_bytes);

  // Decrement 'system_bytes_remaining_' while still holding the arena locks to avoid
  // the window for a race with another thread that removes a buffer from a list and
  // then increments 'system_bytes_remaining_'. The race is prevented because the other
  // thread holds the lock while decrementing 'system_bytes_remaining_' in the cases
  // where it may not have reservation corresponding to that memory.
  if (slow_but_sure && bytes_found < target_bytes) {
    bytes_found += DecreaseBytesRemaining(
        target_bytes - bytes_found, true, &system_bytes_remaining_);
    DCHECK_EQ(bytes_found, target_bytes) << DebugString();
  }
  return bytes_found;
}

void BufferPool::BufferAllocator::Free(BufferHandle&& handle) {
  DCHECK(handle.is_open());
  handle.client_ = nullptr; // Buffer is no longer associated with a client.
  FreeBufferArena* arena = per_core_arenas_[handle.home_core_].get();
  handle.Poison();
  arena->AddFreeBuffer(move(handle));
}

void BufferPool::BufferAllocator::AddCleanPage(
    const unique_lock<mutex>& client_lock, Page* page) {
  page->client->DCheckHoldsLock(client_lock);
  FreeBufferArena* arena = per_core_arenas_[page->buffer.home_core_].get();
  arena->AddCleanPage(page);
}

bool BufferPool::BufferAllocator::RemoveCleanPage(
    const unique_lock<mutex>& client_lock, bool claim_buffer, Page* page) {
  page->client->DCheckHoldsLock(client_lock);
  FreeBufferArena* arena;
  {
    lock_guard<SpinLock> pl(page->buffer_lock);
    // Page may be evicted - in which case it has no home core and is not in an arena.
    if (!page->buffer.is_open()) return false;
    arena = per_core_arenas_[page->buffer.home_core_].get();
  }
  return arena->RemoveCleanPage(claim_buffer, page);
}

void BufferPool::BufferAllocator::Maintenance() {
  for (unique_ptr<FreeBufferArena>& arena : per_core_arenas_) arena->Maintenance();
}

void BufferPool::BufferAllocator::ReleaseMemory(int64_t bytes_to_free) {
  int64_t bytes_freed = 0;
  int current_core = CpuInfo::GetCurrentCore();
  for (int i = 0; i < per_core_arenas_.size(); ++i) {
    int core_to_check = (current_core + i) % per_core_arenas_.size();
    FreeBufferArena* arena = per_core_arenas_[core_to_check].get();
    // Free but don't claim any memory.
    bytes_freed += arena->FreeSystemMemory(bytes_to_free - bytes_freed, 0, nullptr).first;
    if (bytes_freed >= bytes_to_free) break;
  }
  VLOG(1) << "BufferAllocator::ReleaseMemory(): Freed "
          << PrettyPrinter::PrintBytes(bytes_freed);
}

int BufferPool::BufferAllocator::GetFreeListSize(int core, int64_t len) {
  return per_core_arenas_[core]->GetFreeListSize(len);
}

int64_t BufferPool::BufferAllocator::FreeToSystem(vector<BufferHandle>&& buffers) {
  int64_t bytes_freed = 0;
  for (BufferHandle& buffer : buffers) {
    bytes_freed += buffer.len();
    // Ensure that the memory is unpoisoned when it's next allocated by the system.
    buffer.Unpoison();
    system_allocator_->Free(move(buffer));
  }
  return bytes_freed;
}

int64_t BufferPool::BufferAllocator::SumOverArenas(
    const std::function<int64_t(FreeBufferArena* arena)>& compute_fn) const {
  int64_t total = 0;
  for (const unique_ptr<FreeBufferArena>& arena : per_core_arenas_) {
    total += compute_fn(arena.get());
  }
  return total;
}

int64_t BufferPool::BufferAllocator::GetNumFreeBuffers() const {
  return SumOverArenas([](FreeBufferArena* arena) { return arena->GetNumFreeBuffers(); });
}

int64_t BufferPool::BufferAllocator::GetFreeBufferBytes() const {
  return SumOverArenas(
      [](FreeBufferArena* arena) { return arena->GetFreeBufferBytes(); });
}

int64_t BufferPool::BufferAllocator::GetNumCleanPages() const {
  return SumOverArenas([](FreeBufferArena* arena) { return arena->GetNumCleanPages(); });
}

int64_t BufferPool::BufferAllocator::GetCleanPageBytesLimit() const {
  return clean_page_bytes_limit_;
}

int64_t BufferPool::BufferAllocator::GetCleanPageBytes() const {
  return clean_page_bytes_limit_ - clean_page_bytes_remaining_.Load();
}

string BufferPool::BufferAllocator::DebugString() {
  stringstream ss;
  ss << "<BufferAllocator> " << this << " min_buffer_len: " << min_buffer_len_
     << " system_bytes_limit: " << system_bytes_limit_
     << " system_bytes_remaining: " << system_bytes_remaining_.Load() << "\n"
     << " clean_page_bytes_limit: " << clean_page_bytes_limit_
     << " clean_page_bytes_remaining: " << clean_page_bytes_remaining_.Load() << "\n";
  for (int i = 0; i < per_core_arenas_.size(); ++i) {
    ss << "  Arena " << i << " " << per_core_arenas_[i]->DebugString() << "\n";
  }
  return ss.str();
}

BufferPool::FreeBufferArena::FreeBufferArena(
    BufferAllocator* parent, MetricGroup* metrics, const std::string& arena_name)
  : parent_(parent),
    system_alloc_time_(
        metrics->AddCounter("buffer-pool.$0.system-alloc-time", 0, arena_name)),
    local_arena_free_buffer_hits_(metrics->AddCounter(
        "buffer-pool.$0.local-arena-free-buffer-hits", 0, arena_name)),
    direct_alloc_count_(
        metrics->AddCounter("buffer-pool.$0.direct-alloc-count", 0, arena_name)),
    buffer_size_stats_(metrics->RegisterMetric(new HistogramMetric(
        MetricDefs::Get("buffer-pool.$0.allocated-buffer-sizes", arena_name),
        STATS_MAX_BUFFER_SIZE, 3))),
    numa_arena_free_buffer_hits_(
        metrics->AddCounter("buffer-pool.$0.numa-arena-free-buffer-hits", 0, arena_name)),
    clean_page_hits_(
        metrics->AddCounter("buffer-pool.$0.clean-page-hits", 0, arena_name)),
    num_scavenges_(metrics->AddCounter("buffer-pool.$0.num-scavenges", 0, arena_name)),
    num_final_scavenges_(
        metrics->AddCounter("buffer-pool.$0.num-final-scavenges", 0, arena_name)) {}

BufferPool::FreeBufferArena::~FreeBufferArena() {
  for (int i = 0; i < NumBufferSizes(); ++i) {
    // Clear out the free lists.
    FreeList* list = &buffer_sizes_[i].free_buffers;
    vector<BufferHandle> buffers = list->GetBuffersToFree(list->Size());
    parent_->system_bytes_remaining_.Add(parent_->FreeToSystem(move(buffers)));

    // All pages should have been destroyed.
    DCHECK_EQ(0, buffer_sizes_[i].clean_pages.size());
  }
}

void BufferPool::FreeBufferArena::AddFreeBuffer(BufferHandle&& buffer) {
  lock_guard<SpinLock> al(lock_);
  PerSizeLists* lists = GetListsForSize(buffer.len());
  lists->AddFreeBuffer(move(buffer));
}

bool BufferPool::FreeBufferArena::RemoveCleanPage(bool claim_buffer, Page* page) {
  lock_guard<SpinLock> al(lock_);
  PerSizeLists* lists = GetListsForSize(page->len);
  DCHECK_EQ(lists->num_clean_pages.Load(), lists->clean_pages.size());
  if (!lists->clean_pages.Remove(page)) return false;
  lists->num_clean_pages.Add(-1);
  parent_->clean_page_bytes_remaining_.Add(page->len);
  if (!claim_buffer) {
    BufferHandle buffer;
    {
      lock_guard<SpinLock> pl(page->buffer_lock);
      buffer = move(page->buffer);
    }
    lists->AddFreeBuffer(move(buffer));
  }
  return true;
}

bool BufferPool::FreeBufferArena::PopFreeBuffer(
    int64_t buffer_len, BufferHandle* buffer) {
  PerSizeLists* lists = GetListsForSize(buffer_len);
  // Check before acquiring lock.
  if (lists->num_free_buffers.Load() == 0) return false;

  lock_guard<SpinLock> al(lock_);
  FreeList* list = &lists->free_buffers;
  DCHECK_EQ(lists->num_free_buffers.Load(), list->Size());
  if (!list->PopFreeBuffer(buffer)) return false;
  buffer->Unpoison();
  lists->num_free_buffers.Add(-1);
  lists->low_water_mark = min<int>(lists->low_water_mark, list->Size());
  return true;
}

bool BufferPool::FreeBufferArena::EvictCleanPage(
    int64_t buffer_len, BufferHandle* buffer) {
  PerSizeLists* lists = GetListsForSize(buffer_len);
  // Check before acquiring lock.
  if (lists->num_clean_pages.Load() == 0) return false;

  lock_guard<SpinLock> al(lock_);
  DCHECK_EQ(lists->num_clean_pages.Load(), lists->clean_pages.size());
  Page* page = lists->clean_pages.Dequeue();
  if (page == nullptr) return false;
  lists->num_clean_pages.Add(-1);
  parent_->clean_page_bytes_remaining_.Add(buffer_len);
  lock_guard<SpinLock> pl(page->buffer_lock);
  *buffer = move(page->buffer);
  return true;
}

pair<int64_t, int64_t> BufferPool::FreeBufferArena::FreeSystemMemory(
    int64_t target_bytes_to_free, int64_t target_bytes_to_claim,
    std::unique_lock<SpinLock>* arena_lock) {
  DCHECK_GT(target_bytes_to_free, 0);
  DCHECK_GE(target_bytes_to_free, target_bytes_to_claim);
  int64_t bytes_freed = 0;
  // If the caller is acquiring the lock, just lock for the whole method.
  // Otherwise lazily acquire the lock the first time we find some memory
  // to free.
  std::unique_lock<SpinLock> al(lock_, std::defer_lock_t());
  if (arena_lock != nullptr) al.lock();

  vector<BufferHandle> buffers;
  // Search from largest to smallest to avoid freeing many small buffers unless
  // necessary.
  for (int i = NumBufferSizes() - 1; i >= 0; --i) {
    PerSizeLists* lists = &buffer_sizes_[i];
    // Check before acquiring lock to avoid expensive lock acquisition and make scanning
    // empty lists much cheaper.
    if (lists->num_free_buffers.Load() == 0 && lists->num_clean_pages.Load() == 0) {
      continue;
    }
    if (!al.owns_lock()) al.lock();
    FreeList* free_buffers = &lists->free_buffers;
    InternalList<Page>* clean_pages = &lists->clean_pages;
    DCHECK_EQ(lists->num_free_buffers.Load(), free_buffers->Size());
    DCHECK_EQ(lists->num_clean_pages.Load(), clean_pages->size());

    // Figure out how many of the buffers in the free list we should free.
    DCHECK_GT(target_bytes_to_free, bytes_freed);
    const int64_t buffer_len = 1L << (i + parent_->log_min_buffer_len_);
    int64_t buffers_to_free = min(free_buffers->Size(),
        BitUtil::Ceil(target_bytes_to_free - bytes_freed, buffer_len));
    int64_t buffer_bytes_to_free = buffers_to_free * buffer_len;

    // Evict clean pages by moving their buffers to the free page list before freeing
    // them. This ensures that they are freed based on memory address in the expected
    // order.
    int num_pages_evicted = 0;
    int64_t page_bytes_evicted = 0;
    while (bytes_freed + buffer_bytes_to_free < target_bytes_to_free) {
      Page* page = clean_pages->Dequeue();
      if (page == nullptr) break;
      BufferHandle page_buffer;
      {
        lock_guard<SpinLock> pl(page->buffer_lock);
        page_buffer = move(page->buffer);
      }
      ++buffers_to_free;
      buffer_bytes_to_free += page_buffer.len();
      ++num_pages_evicted;
      page_bytes_evicted += page_buffer.len();
      free_buffers->AddFreeBuffer(move(page_buffer));
    }
    lists->num_free_buffers.Add(num_pages_evicted);
    lists->num_clean_pages.Add(-num_pages_evicted);
    parent_->clean_page_bytes_remaining_.Add(page_bytes_evicted);

    if (buffers_to_free > 0) {
      int64_t buffer_bytes_freed =
          parent_->FreeToSystem(free_buffers->GetBuffersToFree(buffers_to_free));
      DCHECK_EQ(buffer_bytes_to_free, buffer_bytes_freed);
      bytes_freed += buffer_bytes_to_free;
      lists->num_free_buffers.Add(-buffers_to_free);
      lists->low_water_mark = min<int>(lists->low_water_mark, free_buffers->Size());
      if (bytes_freed >= target_bytes_to_free) break;
    }
    // Should have cleared out all lists if we don't have enough memory at this point.
    DCHECK_EQ(0, free_buffers->Size());
    DCHECK_EQ(0, clean_pages->size());
  }
  int64_t bytes_claimed = min(bytes_freed, target_bytes_to_claim);
  if (bytes_freed > bytes_claimed) {
    // Add back the extra for other threads before releasing the lock to avoid race
    // where the other thread may not be able to find enough buffers.
    parent_->system_bytes_remaining_.Add(bytes_freed - bytes_claimed);
  }
  if (arena_lock != nullptr) *arena_lock = move(al);
  return make_pair(bytes_freed, bytes_claimed);
}

void BufferPool::FreeBufferArena::AddCleanPage(Page* page) {
  bool eviction_needed = DecreaseBytesRemaining(
        page->len, true, &parent_->clean_page_bytes_remaining_) == 0;
  lock_guard<SpinLock> al(lock_);
  PerSizeLists* lists = GetListsForSize(page->len);
  DCHECK_EQ(lists->num_clean_pages.Load(), lists->clean_pages.size());
  if (eviction_needed) {
    if (lists->clean_pages.empty()) {
      // No other pages to evict, must evict 'page' instead of adding it.
      lists->AddFreeBuffer(move(page->buffer));
    } else {
      // Evict an older page (FIFO eviction) to make space for this one.
      Page* page_to_evict = lists->clean_pages.Dequeue();
      lists->clean_pages.Enqueue(page);
      BufferHandle page_to_evict_buffer;
      {
        lock_guard<SpinLock> pl(page_to_evict->buffer_lock);
        page_to_evict_buffer = move(page_to_evict->buffer);
      }
      lists->AddFreeBuffer(move(page_to_evict_buffer));
    }
  } else {
    lists->clean_pages.Enqueue(page);
    lists->num_clean_pages.Add(1);
  }
}

void BufferPool::FreeBufferArena::Maintenance() {
  lock_guard<SpinLock> al(lock_);
  for (int i = 0; i < NumBufferSizes(); ++i) {
    PerSizeLists* lists = &buffer_sizes_[i];
    DCHECK_LE(lists->low_water_mark, lists->free_buffers.Size());
    if (lists->low_water_mark != 0) {
      // We haven't needed the buffers below the low water mark since the previous
      // Maintenance() call. Discard half of them to free up memory. By always discarding
      // at least one, we guarantee that an idle list will shrink to zero entries.
      int num_to_free = max(1, lists->low_water_mark / 2);
      parent_->system_bytes_remaining_.Add(
          parent_->FreeToSystem(lists->free_buffers.GetBuffersToFree(num_to_free)));
      lists->num_free_buffers.Add(-num_to_free);
    }
    lists->low_water_mark = lists->free_buffers.Size();
  }
}

int BufferPool::FreeBufferArena::GetFreeListSize(int64_t len) {
  lock_guard<SpinLock> al(lock_);
  PerSizeLists* lists = GetListsForSize(len);
  DCHECK_EQ(lists->num_free_buffers.Load(), lists->free_buffers.Size());
  return lists->free_buffers.Size();
}

int64_t BufferPool::FreeBufferArena::SumOverSizes(
    const std::function<int64_t(PerSizeLists* lists, int64_t buffer_size)>& compute_fn) {
  int64_t total = 0;
  for (int i = 0; i < NumBufferSizes(); ++i) {
    int64_t buffer_size = (1L << i) * parent_->min_buffer_len_;
    total += compute_fn(&buffer_sizes_[i], buffer_size);
  }
  return total;
}

int64_t BufferPool::FreeBufferArena::GetNumFreeBuffers() {
  return SumOverSizes([](PerSizeLists* lists, int64_t buffer_size) {
    return lists->num_free_buffers.Load();
  });
}

int64_t BufferPool::FreeBufferArena::GetFreeBufferBytes() {
  return SumOverSizes([](PerSizeLists* lists, int64_t buffer_size) {
    return lists->num_free_buffers.Load() * buffer_size;
  });
}

int64_t BufferPool::FreeBufferArena::GetNumCleanPages() {
  return SumOverSizes([](PerSizeLists* lists, int64_t buffer_size) {
    return lists->num_clean_pages.Load();
  });
}

string BufferPool::FreeBufferArena::DebugString() {
  lock_guard<SpinLock> al(lock_);
  stringstream ss;
  ss << "<FreeBufferArena> " << this << "\n";
  for (int i = 0; i < NumBufferSizes(); ++i) {
    int64_t buffer_len = 1L << (parent_->log_min_buffer_len_ + i);
    PerSizeLists& lists = buffer_sizes_[i];
    ss << "  " << PrettyPrinter::PrintBytes(buffer_len) << ":"
       << " free buffers: " << lists.num_free_buffers.Load()
       << " low water mark: " << lists.low_water_mark
       << " clean pages: " << lists.num_clean_pages.Load() << " ";
    lists.clean_pages.Iterate(bind<bool>(Page::DebugStringCallback, &ss, _1));

    ss << "\n";
  }
  return ss.str();
}
}
