/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"

namespace facebook::presto {
struct PrestoTask;
} // namespace facebook::presto

namespace facebook::presto::dpp {

/// Thrown when a DPP filter store/push is rejected because the
/// `dppFilterCache` root pool is at its configured cap and reclaim
/// could not free enough bytes. Callers translate this to an HTTP 503
/// (push path) or a logged drop (build-side store path).
class DppFilterCacheFull : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

/// Process-wide singleton that owns the `dppFilterCache` arbitrator-tracked
/// root pool and an LRU of completed-but-undelivered DPP filters. Each
/// PrestoTask gets a leaf-child of the root pool for byte attribution.
///
/// Filter contents are serialized to JSON bytes on store and held in a
/// pool-allocated buffer so the arbitrator sees their true size; on
/// snapshot the bytes are deserialized back into a `protocol::TupleDomain`.
class DppFilterCache {
 public:
  /// Idempotent initializer. Subsequent calls with the same capacity are
  /// no-ops; calls with a different capacity log and keep the original.
  /// Must be invoked before any PrestoTask is constructed.
  static void initialize(int64_t maxBytes);

  /// Returns the singleton. Lazily initializes with a fallback 2 GB cap
  /// if `initialize()` was never called (only meant for test paths).
  static DppFilterCache* getInstance();

  /// Test-only: drops the singleton instance, releasing its rootPool_
  /// reference against the current MemoryManager. Callers that follow this
  /// with MemoryManager::testingSetInstance avoid the pool-outliving-
  /// manager leak that otherwise trips Memory.cpp's destructor invariant
  /// check across consecutive tests.
  static void testingReset();

  velox::memory::MemoryPool* rootPool() const {
    return rootPool_.get();
  }

  int64_t maxCapacity() const;
  int64_t currentBytes() const;

  int64_t pushRejectedCount() const {
    return pushRejected_.load();
  }
  void incrementPushRejected() {
    pushRejected_.fetch_add(1);
  }

  /// Creates a per-task leaf-child of the root pool. The leaf pool's
  /// destruction returns its bytes to the root.
  std::shared_ptr<velox::memory::MemoryPool> createTaskPool(
      const std::string& taskId);

  /// Eviction callback type. Invoked by the reclaimer on the arbitrator
  /// thread. Implementations must drop the named filter (release its
  /// pool buffer) and update any task-internal indices. Must not block
  /// on PrestoTask::mutex if it could be held by the caller of allocate.
  using EvictCallback =
      std::function<bool(const std::string& filterId)>;

  /// Records that `task` holds `filterId` of `size` bytes. The entry is
  /// appended at the LRU back (most recently used). Subsequent
  /// `touch(...)` moves it back; `forget(...)` removes it. Must be
  /// called AFTER the pool->allocate succeeds so the entry size matches
  /// reality.
  void registerFilter(
      PrestoTask* task,
      const std::string& filterId,
      int64_t size,
      EvictCallback evictCallback);

  /// Mark the entry most recently used. No-op if not present.
  void touch(PrestoTask* task, const std::string& filterId);

  /// Remove the entry. No-op if not present.
  void forget(PrestoTask* task, const std::string& filterId);

  /// Remove all entries owned by `task`. Called from `~PrestoTask`.
  void forgetTask(PrestoTask* task);

  /// Walk the LRU oldest-first invoking eviction callbacks until at
  /// least `targetBytes` have been freed or the LRU is empty. Returns
  /// the total bytes freed. Invoked by the reclaimer; safe to call
  /// directly from tests.
  uint64_t evictUpTo(uint64_t targetBytes);

 private:
  explicit DppFilterCache(int64_t maxBytes);

  struct Entry {
    PrestoTask* task;
    std::string filterId;
    int64_t size;
    EvictCallback evict;
  };

  using EntryList = std::list<Entry>;
  using TaskFilterKey = std::pair<PrestoTask*, std::string>;

  struct TaskFilterKeyHash {
    size_t operator()(const TaskFilterKey& k) const noexcept {
      return std::hash<PrestoTask*>{}(k.first) ^
          (std::hash<std::string>{}(k.second) << 1);
    }
  };

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::atomic<int64_t> pushRejected_{0};

  mutable std::mutex mutex_;
  EntryList lru_;
  std::unordered_map<TaskFilterKey, EntryList::iterator, TaskFilterKeyHash>
      index_;
};

/// Custom reclaimer for the root `dppFilterCache` pool. Translates
/// `reclaim(targetBytes)` into LRU eviction.
class DppFilterCacheReclaimer : public velox::memory::MemoryReclaimer {
 public:
  static std::unique_ptr<MemoryReclaimer> create();

  bool reclaimableBytes(
      const velox::memory::MemoryPool& pool,
      uint64_t& reclaimableBytes) const override;

  uint64_t reclaim(
      velox::memory::MemoryPool* pool,
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      Stats& stats) override;

 private:
  DppFilterCacheReclaimer() : MemoryReclaimer(/*priority=*/0) {}
};

/// Serializes `tupleDomain` to compact JSON bytes. The output is the
/// canonical wire form used both for storage in the cache and for the
/// `GET /v1/task/{id}/dynamicFilters` long-poll response body.
std::string serializeTupleDomain(
    const protocol::TupleDomain<std::string>& tupleDomain);

/// Parses bytes produced by `serializeTupleDomain` back into a
/// `protocol::TupleDomain<std::string>`. Throws on malformed input.
protocol::TupleDomain<std::string> deserializeTupleDomain(
    const char* data,
    size_t size);

} // namespace facebook::presto::dpp
