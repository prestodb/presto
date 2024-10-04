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

#include <shared_mutex>

#include "velox/common/base/Counters.h"
#include "velox/common/base/GTestMacros.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"

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
  struct ExtraConfig {
    /// The memory capacity reserved to ensure each running query has minimal
    /// capacity of 'memoryPoolReservedCapacity' to run.
    static constexpr std::string_view kReservedCapacity{"reserved-capacity"};
    static constexpr std::string_view kDefaultReservedCapacity{"0B"};
    static int64_t reservedCapacity(
        const std::unordered_map<std::string, std::string>& configs);

    /// The initial memory capacity to reserve for a newly created query memory
    /// pool.
    static constexpr std::string_view kMemoryPoolInitialCapacity{
        "memory-pool-initial-capacity"};
    static constexpr std::string_view kDefaultMemoryPoolInitialCapacity{
        "256MB"};
    static uint64_t memoryPoolInitialCapacity(
        const std::unordered_map<std::string, std::string>& configs);

    /// The minimal amount of memory capacity reserved for each query to run.
    static constexpr std::string_view kMemoryPoolReservedCapacity{
        "memory-pool-reserved-capacity"};
    static constexpr std::string_view kDefaultMemoryPoolReservedCapacity{"0B"};
    static uint64_t memoryPoolReservedCapacity(
        const std::unordered_map<std::string, std::string>& configs);

    /// Specifies the max time to wait for memory reclaim by arbitration. The
    /// memory reclaim might fail if the max time has exceeded. This prevents
    /// the memory arbitration from getting stuck when the memory reclaim waits
    /// for a hanging query task to pause. If it is zero, then there is no
    /// timeout.
    static constexpr std::string_view kMemoryReclaimMaxWaitTime{
        "memory-reclaim-max-wait-time"};
    static constexpr std::string_view kDefaultMemoryReclaimMaxWaitTime{"0ms"};
    static uint64_t memoryReclaimMaxWaitTimeMs(
        const std::unordered_map<std::string, std::string>& configs);

    /// When shrinking capacity, the shrink bytes will be adjusted in a way such
    /// that AFTER shrink, the stricter (whichever is smaller) of the following
    /// conditions is met, in order to better fit the pool's current memory
    /// usage:
    /// - Free capacity is greater or equal to capacity *
    /// 'memoryPoolMinFreeCapacityPct'
    /// - Free capacity is greater or equal to 'memoryPoolMinFreeCapacity'
    ///
    /// NOTE: In the conditions when original requested shrink bytes ends up
    /// with more free capacity than above 2 conditions, the adjusted shrink
    /// bytes is not respected.
    ///
    /// NOTE: Capacity shrink adjustment is enabled when both
    /// 'memoryPoolMinFreeCapacityPct' and 'memoryPoolMinFreeCapacity' are set.
    static constexpr std::string_view kMemoryPoolMinFreeCapacity{
        "memory-pool-min-free-capacity"};
    static constexpr std::string_view kDefaultMemoryPoolMinFreeCapacity{
        "128MB"};
    static uint64_t memoryPoolMinFreeCapacity(
        const std::unordered_map<std::string, std::string>& configs);

    static constexpr std::string_view kMemoryPoolMinFreeCapacityPct{
        "memory-pool-min-free-capacity-pct"};
    static constexpr double kDefaultMemoryPoolMinFreeCapacityPct{0.25};
    static double memoryPoolMinFreeCapacityPct(
        const std::unordered_map<std::string, std::string>& configs);

    /// If true, it allows memory arbitrator to reclaim used memory cross query
    /// memory pools.
    static constexpr std::string_view kGlobalArbitrationEnabled{
        "global-arbitration-enabled"};
    static constexpr bool kDefaultGlobalArbitrationEnabled{false};
    static bool globalArbitrationEnabled(
        const std::unordered_map<std::string, std::string>& configs);

    /// When growing capacity, the growth bytes will be adjusted in the
    /// following way:
    ///  - If 2 * current capacity is less than or equal to
    ///    'fastExponentialGrowthCapacityLimit', grow through fast path by at
    ///    least doubling the current capacity, when conditions allow (see below
    ///    NOTE section).
    ///  - If 2 * current capacity is greater than
    ///    'fastExponentialGrowthCapacityLimit', grow through slow path by
    ///    growing capacity by at least 'slowCapacityGrowPct' * current capacity
    ///    if allowed (see below NOTE section).
    ///
    /// NOTE: If original requested growth bytes is larger than the adjusted
    /// growth bytes or adjusted growth bytes reaches max capacity limit, the
    /// adjusted growth bytes will not be respected.
    ///
    /// NOTE: Capacity growth adjust is only enabled if both
    /// 'fastExponentialGrowthCapacityLimit' and 'slowCapacityGrowPct' are set,
    /// otherwise it is disabled.
    static constexpr std::string_view kFastExponentialGrowthCapacityLimit{
        "fast-exponential-growth-capacity-limit"};
    static constexpr std::string_view
        kDefaultFastExponentialGrowthCapacityLimit{"512MB"};
    static uint64_t fastExponentialGrowthCapacityLimitBytes(
        const std::unordered_map<std::string, std::string>& configs);

    static constexpr std::string_view kSlowCapacityGrowPct{
        "slow-capacity-grow-pct"};
    static constexpr double kDefaultSlowCapacityGrowPct{0.25};
    static double slowCapacityGrowPct(
        const std::unordered_map<std::string, std::string>& configs);

    /// If true, do sanity check on the arbitrator state on destruction.
    ///
    /// TODO: deprecate this flag after all the existing memory leak use cases
    /// have been fixed.
    static constexpr std::string_view kCheckUsageLeak{"check-usage-leak"};
    static constexpr bool kDefaultCheckUsageLeak{true};
    static bool checkUsageLeak(
        const std::unordered_map<std::string, std::string>& configs);
  };

  explicit SharedArbitrator(const Config& config);

  ~SharedArbitrator() override;

  static void registerFactory();

  static void unregisterFactory();

  void addPool(const std::shared_ptr<MemoryPool>& pool) final;

  void removePool(MemoryPool* pool) final;

  bool growCapacity(MemoryPool* pool, uint64_t requestBytes) final;

  uint64_t shrinkCapacity(MemoryPool* pool, uint64_t requestBytes = 0) final;

  uint64_t shrinkCapacity(
      uint64_t requestBytes,
      bool allowSpill = true,
      bool force = false) override final;

  Stats stats() const final;

  std::string kind() const override;

  std::string toString() const final;

  /// Returns 'freeCapacity' back to the arbitrator for testing.
  void testingFreeCapacity(uint64_t freeCapacity);

  uint64_t testingNumRequests() const;

  /// Enables/disables global arbitration accordingly.
  void testingSetGlobalArbitration(bool enableGlobalArbitration) {
    *const_cast<bool*>(&globalArbitrationEnabled_) = enableGlobalArbitration;
  }

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

  /// The candidate memory pool stats used by arbitration.
  struct Candidate {
    std::shared_ptr<MemoryPool> pool;
    int64_t reclaimableBytes{0};
    int64_t freeBytes{0};
    int64_t reservedBytes{0};

    std::string toString() const;
  };

 private:
  // The kind string of shared arbitrator.
  inline static const std::string kind_{"SHARED"};

  // Contains the execution state of an arbitration operation.
  struct ArbitrationOperation {
    MemoryPool* const requestPool;
    const uint64_t requestBytes;

    // The adjusted grow bytes based on 'requestBytes'. This 'targetBytes' is a
    // best effort target, and hence will not be guaranteed. The adjustment is
    // based on 'SharedArbitrator::fastExponentialGrowthCapacityLimit_'
    // 'SharedArbitrator::slowCapacityGrowPct_'
    const std::optional<uint64_t> targetBytes;

    // The start time of this arbitration operation.
    const std::chrono::steady_clock::time_point startTime;

    // The candidate memory pools.
    std::vector<Candidate> candidates;

    // The time that waits in local arbitration queue.
    uint64_t localArbitrationQueueTimeUs{0};

    // The time that waits to acquire the local arbitration lock.
    uint64_t localArbitrationLockWaitTimeUs{0};

    // The time that waits to acquire the global arbitration lock.
    uint64_t globalArbitrationLockWaitTimeUs{0};

    explicit ArbitrationOperation(uint64_t requestBytes)
        : ArbitrationOperation(nullptr, requestBytes, std::nullopt) {}

    ArbitrationOperation(
        MemoryPool* _requestor,
        uint64_t _requestBytes,
        std::optional<uint64_t> _targetBytes)
        : requestPool(_requestor),
          requestBytes(_requestBytes),
          targetBytes(_targetBytes),
          startTime(std::chrono::steady_clock::now()) {
      VELOX_CHECK(requestPool == nullptr || requestPool->isRoot());
    }

    uint64_t waitTimeUs() const {
      return localArbitrationQueueTimeUs + localArbitrationLockWaitTimeUs +
          globalArbitrationLockWaitTimeUs;
    }
  };

  // Used to start and finish an arbitration operation initiated from a memory
  // pool or memory capacity shrink request sent through shrinkPools() API.
  class ScopedArbitration {
   public:
    ScopedArbitration(SharedArbitrator* arbitrator, ArbitrationOperation* op);

    ~ScopedArbitration();

   private:
    ArbitrationOperation* const operation_{nullptr};
    SharedArbitrator* const arbitrator_;
    const std::unique_ptr<ScopedMemoryArbitrationContext> arbitrationCtx_;
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
  // memory pool itself. Then it tries to obtain free capacity from the
  // arbitrator. At last, it tries to reclaim free memory from itself before it
  // falls back to the global arbitration. The local arbitration run is
  // protected by shared lock of 'arbitrationLock_' which can run in parallel
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
  // lock of 'arbitrationLock_' for serial execution mode. The function returns
  // true on success, false on failure.
  bool runGlobalArbitration(ArbitrationOperation* op);

  // Gets the mim/max memory capacity growth targets for 'op'. The min and max
  // targets are calculated based on memoryPoolReservedCapacity_ requirements
  // and the pool's max capacity.
  void getGrowTargets(
      ArbitrationOperation* op,
      uint64_t& maxGrowTarget,
      uint64_t& minGrowTarget);

  // Invoked to get or refresh the candidate memory pools for arbitration. If
  // 'freeCapacityOnly' is true, then we only get free capacity stats for each
  // candidate memory pool.
  void getCandidates(ArbitrationOperation* op, bool freeCapacityOnly = false);

  // Sorts 'candidates' based on reclaimable free capacity in descending order.
  static void sortCandidatesByReclaimableFreeCapacity(
      std::vector<Candidate>& candidates);

  // Sorts 'candidates' based on reclaimable used capacity in descending order.
  static void sortCandidatesByReclaimableUsedCapacity(
      std::vector<Candidate>& candidates);

  // Sorts 'candidates' based on actual used memory in descending order.
  static void sortCandidatesByUsage(std::vector<Candidate>& candidates);

  // Finds the candidate with the largest capacity. For 'requestor', the
  // capacity for comparison including its current capacity and the capacity to
  // grow.
  static const SharedArbitrator::Candidate& findCandidateWithLargestCapacity(
      MemoryPool* requestor,
      uint64_t targetBytes,
      const std::vector<Candidate>& candidates);

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
  void reclaimUsedMemoryFromCandidatesBySpill(
      ArbitrationOperation* op,
      uint64_t& freedBytes);

  // Invoked to reclaim used memory capacity from 'candidates' by aborting the
  // top memory users' queries.
  void reclaimUsedMemoryFromCandidatesByAbort(
      ArbitrationOperation* op,
      uint64_t& freedBytes);

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

  // The capacity growth target is set to have a coarser granularity. It can
  // help to reduce the number of future grow calls, and hence reducing the
  // number of unnecessary memory arbitration requests.
  uint64_t getCapacityGrowthTarget(
      const MemoryPool& pool,
      uint64_t requestBytes) const;

  // The capacity shrink target is adjusted from request shrink bytes to give
  // the memory pool more headroom free capacity after shrink. It can help to
  // reduce the number of future grow calls, and hence reducing the number of
  // unnecessary memory arbitration requests.
  uint64_t getCapacityShrinkTarget(
      const MemoryPool& pool,
      uint64_t requestBytes) const;

  // Returns true if 'pool' is under memory arbitration.
  bool isUnderArbitrationLocked(MemoryPool* pool) const;

  void updateArbitrationRequestStats();

  void updateArbitrationFailureStats();

  const uint64_t reservedCapacity_;
  const uint64_t memoryPoolInitialCapacity_;
  const uint64_t memoryPoolReservedCapacity_;
  const uint64_t memoryReclaimWaitMs_;
  const bool globalArbitrationEnabled_;
  const bool checkUsageLeak_;

  const uint64_t fastExponentialGrowthCapacityLimit_;
  const double slowCapacityGrowPct_;
  const uint64_t memoryPoolMinFreeCapacity_;
  const double memoryPoolMinFreeCapacityPct_;

  mutable folly::SharedMutex poolLock_;
  std::unordered_map<MemoryPool*, std::weak_ptr<MemoryPool>> candidates_;

  // Lock used to protect the arbitrator state.
  mutable std::mutex stateLock_;
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
  tsan_atomic<uint64_t> reclaimedFreeBytes_{0};
  tsan_atomic<uint64_t> reclaimedUsedBytes_{0};
  tsan_atomic<uint64_t> numNonReclaimableAttempts_{0};
};
} // namespace facebook::velox::memory
