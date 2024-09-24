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
#include "velox/common/base/GTestMacros.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox::memory {

class ArbitrationOperation;
class ScopedArbitrationParticipant;

/// Manages the memory arbitration operations on a query memory pool. It also
/// tracks the arbitration stats during the query memory pool's lifecycle.
class ArbitrationParticipant
    : public std::enable_shared_from_this<ArbitrationParticipant> {
 public:
  struct Config {
    /// The minimum capacity of a query memory pool.
    uint64_t minCapacity;

    /// When growing a query memory pool capacity, the growth bytes will be
    /// adjusted in the following way:
    ///  - If 2 * current capacity is less than or equal to
    ///    'fastExponentialGrowthCapacityLimit', grow through fast path by at
    ///    least doubling the current capacity, when conditions allow (see below
    ///    NOTE section).
    ///  - If 2 * current capacity is greater than
    ///    'fastExponentialGrowthCapacityLimit', grow through slow path by
    ///    growing capacity by at least 'slowCapacityGrowRatio' * current
    ///    capacity if allowed (see below NOTE section).
    ///
    /// NOTE: if original requested growth bytes is larger than the adjusted
    /// growth bytes or adjusted growth bytes reaches max capacity limit, the
    /// adjusted growth bytes will not be respected.
    ///
    /// NOTE: capacity growth adjust is only enabled if both
    /// 'fastExponentialGrowthCapacityLimit' and 'slowCapacityGrowRatio' are
    /// set, otherwise it is disabled.
    uint64_t fastExponentialGrowthCapacityLimit;
    double slowCapacityGrowRatio;

    /// When shrinking a memory pool capacity, the shrink bytes will be adjusted
    /// in a way such that AFTER shrink, the stricter (whichever is smaller) of
    /// the following conditions is met, in order to better fit the query memory
    /// pool's current memory usage:
    /// - Free capacity is greater or equal to capacity *
    /// 'minFreeCapacityRatio'
    /// - Free capacity is greater or equal to 'minFreeCapacity'
    ///
    /// NOTE: in the conditions when original requested shrink bytes ends up
    /// with more free capacity than above 2 conditions, the adjusted shrink
    /// bytes is not respected.
    ///
    /// NOTE: capacity shrink adjustment is enabled when both
    /// 'minFreeCapacityRatio' and 'minFreeCapacity' are set.
    uint64_t minFreeCapacity;
    double minFreeCapacityRatio;

    Config(
        uint64_t _minCapacity,
        uint64_t _fastExponentialGrowthCapacityLimit,
        double _slowCapacityGrowRatio,
        uint64_t _minFreeCapacity,
        double _minFreeCapacityRatio);

    std::string toString() const;
  };

  static std::shared_ptr<ArbitrationParticipant> create(
      uint64_t id,
      const std::shared_ptr<MemoryPool>& pool,
      const Config* config);

  ~ArbitrationParticipant();

  /// Returns the query memory pool name of this arbitration participant.
  std::string name() const {
    return pool_->name();
  }

  /// Returns the id of this arbitration participant assigned by the arbitrator.
  /// The id is monotonically increasing and unique across all the alive
  /// arbitration participants.
  uint64_t id() const {
    return id_;
  }

  /// Returns the max capacity of the underlying query memory pool.
  uint64_t maxCapacity() const {
    return maxCapacity_;
  }

  /// Returns the min capacity of the underlying query memory pool.
  uint64_t minCapacity() const {
    return config_->minCapacity;
  }

  /// Returns the duration of this arbitration participant since its creation.
  uint64_t durationUs() const {
    const auto now = getCurrentTimeMicro();
    VELOX_CHECK_GE(now, createTimeUs_);
    return now - createTimeUs_;
  }

  /// Invoked to acquire a shared reference to this arbitration participant
  /// which ensures the liveness of underlying query memory pool. If the query
  /// memory pool is being destroyed, then this function returns std::nullopt.
  ///
  // NOTE: it is not safe to directly access arbitration participant as it only
  // holds a weak ptr to the query memory pool. Use 'lock()' to get a scoped
  // arbitration participant for access.
  std::optional<ScopedArbitrationParticipant> lock();

  /// Returns the corresponding query memory pool.
  MemoryPool* pool() const {
    return pool_;
  }

  /// Returns the current capacity of the query memory pool.
  uint64_t capacity() const {
    return pool_->capacity();
  }

  /// Gets the capacity growth targets based on 'requestBytes' and the query
  /// memory pool's current capacity. 'maxGrowBytes' is set to allow fast
  /// exponential growth when the query memory pool is small and switch to the
  /// slow incremental growth after the query memory pool has grown big.
  /// 'minGrowBytes' is set to ensure the query memory pool has the minimum
  /// capacity and certain headroom free capacity after shrink. Both targets are
  /// set to a coarser granularity to reduce the number of unnecessary future
  /// memory arbitration requests. The parameters used to set the targets are
  /// defined in 'config_'.
  void getGrowTargets(
      uint64_t requestBytes,
      uint64_t& maxGrowBytes,
      uint64_t& minGrowBytes) const;

  /// Returns the unused free memory capacity that can be reclaimed from the
  /// query memory pool by shrink.
  uint64_t reclaimableFreeCapacity() const;

  /// Returns the used memory capacity that can be reclaimed from the query
  /// memory pool through disk spilling.
  uint64_t reclaimableUsedCapacity() const;

  /// Checks if the query memory pool can grow 'requestBytes' from its current
  /// capacity under the max capacity limit.
  bool checkCapacityGrowth(uint64_t requestBytes) const;

  /// Invoked to grow the query memory pool capacity by 'growBytes' and commit
  /// used reservation by 'reservationBytes'. The function throws if the growth
  /// fails.
  bool grow(uint64_t growBytes, uint64_t reservationBytes);

  /// Invoked to release the unused memory capacity by reducing its capacity. If
  /// 'reclaimAll' is true, the function releases all the unused memory capacity
  /// from the query memory pool without regarding to the minimum free capacity
  /// restriction.
  uint64_t shrink(bool reclaimAll = false);

  // Invoked to reclaim used memory from this memory pool with specified
  // 'targetBytes'. The function returns the actually freed capacity.
  uint64_t reclaim(uint64_t targetBytes, uint64_t maxWaitTimeMs) noexcept;

  /// Invoked to abort the query memory pool and returns the reclaimed bytes
  /// after abort.
  uint64_t abort(const std::exception_ptr& error) noexcept;

  /// Returns true if the query memory pool has been aborted.
  bool aborted() const {
    std::lock_guard<std::mutex> l(stateLock_);
    return aborted_;
  }

  /// Invoked to wait for the pending memory reclaim or abort operation to
  /// complete within a 'maxWaitTimeMs' time window. The function returns false
  /// if the wait has timed out.
  bool waitForReclaimOrAbort(uint64_t maxWaitTimeMs) const;

  /// Invoked to start arbitration operation 'op'. The operation needs to wait
  /// for the prior arbitration operations to finish first before executing to
  /// ensure the serialized execution of arbitration operations from the same
  /// query memory pool.
  void startArbitration(ArbitrationOperation* op);

  /// Invoked by a finished arbitration operation 'op' to kick off the next
  /// waiting operation to start execution if there is one.
  void finishArbitration(ArbitrationOperation* op);

  /// Returns true if there is a running arbitration operation on this
  /// participant.
  bool hasRunningOp() const;

  /// Returns the number of waiting arbitration operations on this participant.
  size_t numWaitingOps() const;

  struct Stats {
    uint64_t durationUs{0};
    uint32_t numRequests{0};
    uint32_t numReclaims{0};
    uint32_t numShrinks{0};
    uint32_t numGrows{0};
    uint64_t reclaimedBytes{0};
    uint64_t growBytes{0};
    bool aborted{false};

    std::string toString() const;
  };

  Stats stats() const {
    Stats stats;
    stats.durationUs = durationUs();
    stats.aborted = aborted_;
    stats.numRequests = numRequests_;
    stats.numGrows = numGrows_;
    stats.numShrinks = numShrinks_;
    stats.numReclaims = numReclaims_;
    stats.reclaimedBytes = reclaimedBytes_;
    stats.growBytes = growBytes_;
    return stats;
  }

 private:
  ArbitrationParticipant(
      uint64_t id,
      const std::shared_ptr<MemoryPool>& pool,
      const Config* config);

  // Indicates if the query memory pool is actively used by a query execution or
  // not.
  bool inactivePool() const;

  // Returns the max capacity to reclaim from the query memory pool assuming all
  // the query memory is reclaimable.
  uint64_t maxReclaimableCapacity() const;

  // Returns the max capacity to shrink from the query memory pool. It ensures
  // the memory pool having headroom free capacity after shrink as specified by
  // 'minFreeCapacityRatio' and 'minFreeCapacity' in 'config_'. This helps to
  // reduce the number of unnecessary memory arbitration requests.
  uint64_t maxShrinkCapacity() const;

  // Returns the max capacity to grow of the query memory pool as specified by
  // 'fastExponentialGrowthCapacityLimit' and 'slowCapacityGrowRatio' in
  // 'config_'.
  uint64_t maxGrowCapacity() const;

  // Returns the min capacity to grow the query memory pool to have the minnimal
  // capacity as specified by 'minCapacity' in 'config_'.
  uint64_t minGrowCapacity() const;

  // Aborts the query memory pool and returns the reclaimed bytes after abort.
  uint64_t abortLocked(const std::exception_ptr& error) noexcept;

  const uint64_t id_;
  const std::weak_ptr<MemoryPool> poolWeakPtr_;
  MemoryPool* const pool_;
  const Config* const config_;
  const uint64_t maxCapacity_;
  const size_t createTimeUs_;

  mutable std::mutex stateLock_;
  bool aborted_{false};

  // Points to the current running arbitration operation on this participant.
  ArbitrationOperation* runningOp_{nullptr};

  struct WaitOp {
    ArbitrationOperation* op;
    ContinuePromise waitPromise;
  };
  /// The resume promises of the arbitration operations on this participant
  /// waiting for serial execution.
  std::deque<WaitOp> waitOps_;

  tsan_atomic<uint32_t> numRequests_{0};
  tsan_atomic<uint32_t> numReclaims_{0};
  tsan_atomic<uint32_t> numShrinks_{0};
  tsan_atomic<uint32_t> numGrows_{0};
  tsan_atomic<uint64_t> reclaimedBytes_{0};
  tsan_atomic<uint64_t> growBytes_{0};

  mutable std::timed_mutex reclaimLock_;

  friend class ScopedArbitrationParticipant;
};

/// The wrapper of the arbitration participant which holds a shared reference to
/// the query memory pool to ensure its liveness during memory arbitration
/// execution.
class ScopedArbitrationParticipant {
 public:
  ScopedArbitrationParticipant(
      std::shared_ptr<ArbitrationParticipant> ArbitrationParticipant,
      std::shared_ptr<MemoryPool> pool);

  ArbitrationParticipant* operator->() const {
    return ArbitrationParticipant_.get();
  }

  ArbitrationParticipant& operator*() const {
    return *ArbitrationParticipant_;
  }

  ArbitrationParticipant& operator()() const {
    return *ArbitrationParticipant_;
  }

  ArbitrationParticipant* get() const {
    return ArbitrationParticipant_.get();
  }

 private:
  std::shared_ptr<ArbitrationParticipant> ArbitrationParticipant_;
  std::shared_ptr<MemoryPool> pool_;
};

/// The candidate participant stats used by arbitrator to make arbitration
/// decisions.
struct ArbitrationCandidate {
  ScopedArbitrationParticipant participant;
  int64_t reclaimableUsedCapacity{0};
  int64_t reclaimableFreeCapacity{0};

  /// If 'freeCapacityOnly' is true, the candidate is only used to reclaim free
  /// capacity so only collects the free capacity stats.
  ArbitrationCandidate(
      ScopedArbitrationParticipant&& _participant,
      bool freeCapacityOnly);

  std::string toString() const;
};
} // namespace facebook::velox::memory
