/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "velox/common/memory/MemoryArbitrator.h"

#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox::memory {

/// Used to achieve dynamic memory sharing among running queries. When a
/// memory pool exceeds its current memory capacity, the arbitrator tries to
/// grow its capacity by reclaim the overused memory from the query with
/// more memory usage. We can configure memory arbitrator the way to reclaim
/// memory. For Prestissimo, we can configure it to reclaim memory by
/// aborting a query. For Prestissimo-on-Spark, we can configure it to
/// reclaim from a running query through techniques such as disk-spilling,
/// partial aggregation or persistent shuffle data flushes.
class SharedArbitrator : public memory::MemoryArbitrator {
 public:
  explicit SharedArbitrator(const Config& config);

  ~SharedArbitrator() override;

  static void registerFactory();

  static void unregisterFactory();

  uint64_t growCapacity(MemoryPool* pool, uint64_t targetBytes) final;

  bool growCapacity(
      MemoryPool* pool,
      const std::vector<std::shared_ptr<MemoryPool>>& candidatePools,
      uint64_t targetBytes) final;

  uint64_t shrinkCapacity(MemoryPool* pool, uint64_t targetBytes) final;

  uint64_t shrinkCapacity(
      const std::vector<std::shared_ptr<MemoryPool>>& pools,
      uint64_t targetBytes,
      bool allowSpill = true,
      bool force = false) override final;

  Stats stats() const final;

  std::string kind() const override;

  std::string toString() const final;

  /// The candidate memory pool stats used by arbitration.
  struct Candidate {
    int64_t reclaimableBytes{0};
    int64_t freeBytes{0};
    int64_t currentBytes{0};
    MemoryPool* pool;

    std::string toString() const;
  };

  /// Returns 'freeCapacity' back to the arbitrator for testing.
  void testingFreeCapacity(uint64_t freeCapacity);

  uint64_t testingNumRequests() const;

  /// Operator level runtime stats that are reported during a shared arbitration
  /// attempt.
  static inline const std::string kMemoryArbitrationWallNanos{
      "memoryArbitrationWallNanos"};
  static inline const std::string kGlobalArbitrationCount{
      "globalArbitrationCount"};
  static inline const std::string kLocalArbitrationCount{
      "localArbitrationCount"};
  static inline const std::string kLocalArbitrationQueueWallNanos{
      "localArbitrationQueueWallNanos"};
  static inline const std::string kLocalArbitrationLockWaitWallNanos{
      "localArbitrationLockWaitWallNanos"};
  static inline const std::string kGlobalArbitrationLockWaitWallNanos{
      "globalArbitrationLockWaitWallNanos"};

 private:
  // The kind string of shared arbitrator.
  inline static const std::string kind_{"SHARED"};

  // Contains the execution state of an arbitration operation.
  struct ArbitrationOperation {
    MemoryPool* const requestPool;
    MemoryPool* const requestRoot;
    const std::vector<std::shared_ptr<MemoryPool>>& candidatePools;
    const uint64_t targetBytes;
    // The start time of this arbitration operation.
    const std::chrono::steady_clock::time_point startTime;

    // The stats of candidate memory pools used for memory arbitration.
    std::vector<Candidate> candidates;

    // The time that waits in local arbitration queue.
    uint64_t localArbitrationQueueTimeUs{0};

    // The time that waits to acquire the local arbitration lock.
    uint64_t localArbitrationLockWaitTimeUs{0};

    // The time that waits to acquire the global arbitration lock.
    uint64_t globalArbitrationLockWaitTimeUs{0};

    ArbitrationOperation(
        uint64_t targetBytes,
        const std::vector<std::shared_ptr<MemoryPool>>& candidatePools)
        : ArbitrationOperation(nullptr, targetBytes, candidatePools) {}

    ArbitrationOperation(
        MemoryPool* _requestor,
        uint64_t _targetBytes,
        const std::vector<std::shared_ptr<MemoryPool>>& _candidatePools)
        : requestPool(_requestor),
          requestRoot(_requestor == nullptr ? nullptr : _requestor->root()),
          candidatePools(_candidatePools),
          targetBytes(_targetBytes),
          startTime(std::chrono::steady_clock::now()) {}

    uint64_t waitTimeUs() const {
      return localArbitrationQueueTimeUs + localArbitrationLockWaitTimeUs +
          globalArbitrationLockWaitTimeUs;
    }

    void enterArbitration();
    void leaveArbitration();
  };

  // Used to start and finish an arbitration operation initiated from a memory
  // pool or memory capacity shrink request sent through shrinkPools() API.
  class ScopedArbitration {
   public:
    ScopedArbitration(SharedArbitrator* arbitrator, ArbitrationOperation* op);

    ~ScopedArbitration();

   private:
    ArbitrationOperation* const operation_;
    SharedArbitrator* const arbitrator_;
    const ScopedMemoryArbitrationContext arbitrationCtx_;
    const std::chrono::steady_clock::time_point startTime_;
  };

  // The arbitration running queue for arbitration requests from the same query
  // pool.
  struct ArbitrationQueue {
    // Points to the current running arbitration.
    ArbitrationOperation* current;

    // The promises of the arbitration requests from the same query pool waiting
    // for the serial execution.
    std::vector<ContinuePromise> waitPromises;

    explicit ArbitrationQueue(ArbitrationOperation* op) : current(op) {
      VELOX_CHECK_NOT_NULL(current);
    }
  };

  // Invoked to check if the memory growth will exceed the memory pool's max
  // capacity limit or the arbitrator's node capacity limit.
  bool checkCapacityGrowth(ArbitrationOperation* op) const;

  // Invoked to ensure the memory growth request won't exceed the request memory
  // pool's max capacity as well as the arbitrator's node capacity. If it does,
  // then we first need to reclaim the used memory from the request memory pool
  // itself to ensure the memory growth won't exceed the capacity limit, and
  // then proceed with the memory arbitration process across queries.
  bool ensureCapacity(ArbitrationOperation* op);

  // Invoked to reclaim the memory from the other query memory pools to grow the
  // request memory pool's capacity.
  bool arbitrateMemory(ArbitrationOperation* op);

  // Invoked to start next memory arbitration request, and it will wait for
  // the serialized execution if there is a running or other waiting
  // arbitration requests.
  void startArbitration(ArbitrationOperation* op);

  // Invoked by a finished memory arbitration request to kick off the next
  // arbitration request execution if there are any ones waiting.
  void finishArbitration(ArbitrationOperation* op);

  // Invoked to run local arbitration on the request memory pool. It first
  // ensures the memory growth is within both memory pool and arbitrator
  // capacity limits. This step might reclaim the used memory from the request
  // memory pool itself. Then it tries to allocate free capacity from the
  // arbitrator. At last, it tries to reclaim free memory from the other queries
  // before it falls back to the global arbitration. The local arbitration run
  // is protected by shared lock of 'arbitrationLock_' which can run in parallel
  // for different query pools. The free memory reclamation is protected by
  // arbitrator 'mutex_' which is an in-memory fast operation. The function
  // returns false on failure. Otherwise, it needs to further check if
  // 'needGlobalArbitration' is true or not. If true, needs to proceed with the
  // global arbitration run.
  bool runLocalArbitration(
      ArbitrationOperation* op,
      bool& needGlobalArbitration);

  // Invoked to run global arbitration to reclaim free or used memory from the
  // other queries. The global arbitration run is protected by the exclusive
  // lock of 'arbitrationLock_' for serial execution. The function returns true
  // on success, false on failure.
  bool runGlobalArbitration(ArbitrationOperation* op);

  // Gets the mim/max memory capacity growth targets for 'op'.
  void getGrowTargets(
      ArbitrationOperation* op,
      uint64_t& maxGrowTarget,
      uint64_t& minGrowTarget);

  // Invoked to get or refresh the memory stats of the candidate memory pools
  // for arbitration. If 'freeCapacityOnly' is true, then we only get free
  // capacity stats for each candidate memory pool.
  void getCandidateStats(
      ArbitrationOperation* op,
      bool freeCapacityOnly = false);

  // Invoked to reclaim free memory capacity from 'candidates' without
  // actually freeing used memory.
  //
  // NOTE: the function might sort 'candidates' based on each candidate's free
  // capacity internally.
  uint64_t reclaimFreeMemoryFromCandidates(
      ArbitrationOperation* op,
      uint64_t reclaimTargetBytes,
      bool isLocalArbitration);

  // Invoked to reclaim used memory capacity from 'candidates' by spilling.
  //
  // NOTE: the function might sort 'candidates' based on each candidate's
  // reclaimable memory internally.
  uint64_t reclaimUsedMemoryFromCandidatesBySpill(
      ArbitrationOperation* op,
      uint64_t reclaimTargetBytes);

  // Invoked to reclaim used memory capacity from 'candidates' by aborting the
  // top memory users' queries.
  uint64_t reclaimUsedMemoryFromCandidatesByAbort(
      ArbitrationOperation* op,
      uint64_t reclaimTargetBytes);

  // Checks if request pool has been aborted or not.
  void checkIfAborted(ArbitrationOperation* op);

  // Checks if the request pool already has enough free capacity for the growth.
  // This could happen if there are multiple arbitration operations from the
  // same query. When the first served operation succeeds, it might have
  // reserved enough capacity for the followup operations.
  bool maybeGrowFromSelf(ArbitrationOperation* op);

  // Invoked to grow 'pool' capacity by 'growBytes' and commit used reservation
  // by 'reservationBytes'. The function throws if the growth fails.
  void
  checkedGrow(MemoryPool* pool, uint64_t growBytes, uint64_t reservationBytes);

  // Invoked to reclaim used memory from 'targetPool' with specified
  // 'targetBytes'. The function returns the actually freed capacity.
  // 'isLocalArbitration' is true when the reclaim attempt is within a local
  // arbitration.
  uint64_t reclaim(
      MemoryPool* targetPool,
      uint64_t targetBytes,
      bool isLocalArbitration) noexcept;

  // Invoked to abort memory 'pool'.
  void abort(MemoryPool* pool, const std::exception_ptr& error);

  // Invoked to handle the memory arbitration failure to abort the memory pool
  // with the largest capacity to free up memory. The function returns true on
  // success and false if the requestor itself has been selected as the
  // victim. We don't abort the requestor itself but just fails the
  // arbitration to let the user decide to either proceed with the query or
  // fail it.
  bool handleOOM(ArbitrationOperation* op);

  // Decrements free capacity from the arbitrator with up to
  // 'maxBytesToReserve'. The arbitrator might have less free available
  // capacity. The function returns the actual decremented free capacity
  // bytes. If 'minBytesToReserve' is not zero and there is less than
  // 'minBytes' available in non-reserved capacity, then the arbitrator tries
  // to decrement up to 'minBytes' from the reserved capacity.
  uint64_t decrementFreeCapacity(
      uint64_t maxBytesToReserve,
      uint64_t minBytesToReserve);
  uint64_t decrementFreeCapacityLocked(
      uint64_t maxBytesToReserve,
      uint64_t minBytesToReserve);

  // Increment free capacity by 'bytes'.
  void incrementFreeCapacity(uint64_t bytes);
  void incrementFreeCapacityLocked(uint64_t bytes);
  // Increments the free reserved capacity up to 'bytes' until reaches to the
  // reserved capacity limit. 'bytes' is updated accordingly.
  void incrementFreeReservedCapacityLocked(uint64_t& bytes);

  void incrementGlobalArbitrationCount();
  void incrementLocalArbitrationCount();

  std::string toStringLocked() const;

  Stats statsLocked() const;

  // Returns the max reclaimable capacity from 'pool' which includes both used
  // and free capacities. If 'isSelfReclaim' true, we reclaim memory from the
  // request pool itself so that we can bypass the reserved free capacity
  // reclaim restriction.
  int64_t maxReclaimableCapacity(const MemoryPool& pool, bool isSelfReclaim)
      const;

  // Returns the free memory capacity that can be reclaimed from 'pool' by
  // shrink. If 'isSelfReclaim' true, we reclaim memory from the request pool
  // itself so that we can bypass the reserved free capacity reclaim
  // restriction.
  int64_t reclaimableFreeCapacity(const MemoryPool& pool, bool isSelfReclaim)
      const;

  // Returns the used memory capacity that can be reclaimed from 'pool' by
  // disk spill. If 'isSelfReclaim' true, we reclaim memory from the request
  // pool itself so that we can bypass the reserved free capacity reclaim
  // restriction.
  int64_t reclaimableUsedCapacity(const MemoryPool& pool, bool isSelfReclaim)
      const;

  // Returns the minimal amount of memory capacity to grow for 'pool' to have
  // the reserved capacity as specified by 'memoryPoolReservedCapacity_'.
  int64_t minGrowCapacity(const MemoryPool& pool) const;

  // Returns true if 'pool' is under memory arbitration.
  bool isUnderArbitration(MemoryPool* pool) const;
  bool isUnderArbitrationLocked(MemoryPool* pool) const;

  void updateArbitrationRequestStats();
  void updateArbitrationFailureStats();

  // Lock used to protect the arbitrator state.
  mutable std::mutex mutex_;
  tsan_atomic<uint64_t> freeReservedCapacity_{0};
  tsan_atomic<uint64_t> freeNonReservedCapacity_{0};

  // Contains the arbitration running queues with one per each query memory
  // pool.
  std::unordered_map<MemoryPool*, std::unique_ptr<ArbitrationQueue>>
      arbitrationQueues_;

  // R/W lock used to control local and global arbitration runs. A local
  // arbitration run needs to hold a shared lock while the latter needs to hold
  // an exclusive lock. Hence, multiple local arbitration runs from different
  // query memory pools can run in parallel but the global ones has to run with
  // one at a time.
  mutable std::shared_mutex arbitrationLock_;

  std::atomic_uint64_t numRequests_{0};
  std::atomic_uint32_t numPending_{0};
  tsan_atomic<uint64_t> numAborted_{0};
  std::atomic_uint64_t numFailures_{0};
  std::atomic_uint64_t waitTimeUs_{0};
  tsan_atomic<uint64_t> arbitrationTimeUs_{0};
  tsan_atomic<uint64_t> reclaimedFreeBytes_{0};
  tsan_atomic<uint64_t> reclaimedUsedBytes_{0};
  tsan_atomic<uint64_t> reclaimTimeUs_{0};
  tsan_atomic<uint64_t> numNonReclaimableAttempts_{0};
  tsan_atomic<uint64_t> numReserves_{0};
  tsan_atomic<uint64_t> numReleases_{0};
};
} // namespace facebook::velox::memory
