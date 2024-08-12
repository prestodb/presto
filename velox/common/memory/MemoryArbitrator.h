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

#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Portability.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/time/Timer.h"

namespace facebook::velox::memory {

class MemoryPool;

using MemoryArbitrationStateCheckCB = std::function<void(MemoryPool&)>;

/// The memory arbitrator interface. There is one memory arbitrator object per
/// memory manager which is responsible for arbitrating memory usage among the
/// query memory pools for query memory isolation. When a memory pool exceeds
/// its capacity limit, it sends a memory grow request to the memory manager
/// which forwards the request to the memory arbitrator behind along with all
/// the query memory pools as the arbitration candidates. The arbitrator tries
/// to free up the overused memory from the selected candidates according to the
/// supported query memory isolation policy. The memory arbitrator can free up
/// memory by either reclaiming the used memory from a running query through
/// techniques such as disk spilling or aborting a query. Different memory
/// arbitrator implementations achieve different query memory isolation policy
/// (see Kind definition below).
class MemoryArbitrator {
 public:
  struct Config {
    /// The string kind of this memory arbitrator.
    ///
    /// NOTE: If kind is not set, a noop arbitrator is created which grants the
    /// maximum capacity to each newly created memory pool.
    std::string kind{};

    /// The total memory capacity in bytes of all the running queries.
    ///
    /// NOTE: this should be same capacity as we set in the associated memory
    /// manager.
    int64_t capacity;

    /// callback to check if a memory arbitration request is issued from a
    /// driver thread, then the driver should be put in suspended state to avoid
    /// the potential deadlock when reclaim memory from the task of the request
    /// memory pool.
    MemoryArbitrationStateCheckCB arbitrationStateCheckCb{nullptr};

    /// Additional configs that are arbitrator implementation specific.
    std::unordered_map<std::string, std::string> extraConfigs;
  };

  using Factory = std::function<std::unique_ptr<MemoryArbitrator>(
      const MemoryArbitrator::Config& config)>;

  /// Registers factory for a specific 'kind' of memory arbitrator
  /// MemoryArbitrator::Create looks up the registry to find the factory to
  /// create arbitrator instance based on the kind specified in arbitrator
  /// config.
  ///
  /// NOTE: we only allow the same 'kind' of memory arbitrator to be registered
  /// once. The function returns false if 'kind' is already registered.
  static bool registerFactory(const std::string& kind, Factory factory);

  /// Unregisters the registered factory for a specifc kind.
  ///
  /// NOTE: the function throws if the specified arbitrator 'kind' is not
  /// registered.
  static void unregisterFactory(const std::string& kind);

  /// Invoked by the memory manager to create an instance of memory arbitrator
  /// based on the kind specified in 'config'. The arbitrator kind must be
  /// registered through MemoryArbitrator::registerFactory(), otherwise the
  /// function throws a velox user exception error.
  ///
  /// NOTE: if arbitrator kind is not set in 'config', then the function returns
  /// nullptr, and the memory arbitration function is disabled.
  static std::unique_ptr<MemoryArbitrator> create(const Config& config);

  virtual std::string kind() const = 0;

  uint64_t capacity() const {
    return capacity_;
  }

  virtual ~MemoryArbitrator() = default;

  /// Invoked by the memory manager to add a newly created memory pool. The
  /// memory arbitrator allocates the initial capacity for 'pool' and
  /// dynamically adjusts its capacity based query memory needs through memory
  /// arbitration.
  virtual void addPool(const std::shared_ptr<MemoryPool>& pool) = 0;

  /// Invoked by the memory manager to remove a destroyed memory pool. The
  /// memory arbitrator frees up all its capacity and stops memory arbitration
  /// operation on it.
  virtual void removePool(MemoryPool* pool) = 0;

  /// Invoked by the memory manager to grow a memory pool's capacity.
  /// 'pool' is the memory pool to request to grow. The memory arbitrator picks
  /// up a number of pools to either shrink its memory capacity without actually
  /// freeing memory or reclaim its used memory to free up enough memory for
  /// 'requestor' to grow.
  virtual bool growCapacity(MemoryPool* pool, uint64_t targetBytes) = 0;

  /// Invoked by the memory manager to shrink up to 'targetBytes' free capacity
  /// from a memory 'pool', and returns them back to the arbitrator. If
  /// 'targetBytes' is zero, we shrink all the free capacity from the memory
  /// pool. The function returns the actual freed capacity from 'pool'.
  virtual uint64_t shrinkCapacity(MemoryPool* pool, uint64_t targetBytes) = 0;

  /// Invoked by the memory manager to shrink memory capacity from memory pools
  /// by reclaiming free and used memory. The freed memory capacity is given
  /// back to the arbitrator.  If 'targetBytes' is zero, then try to reclaim all
  /// the memory from 'pools'. The function returns the actual freed memory
  /// capacity in bytes. If 'allowSpill' is true, it reclaims the used memory by
  /// spilling. If 'allowAbort' is true, it reclaims the used memory by aborting
  /// the queries with the most memory usage. If both are true, it first
  /// reclaims the used memory by spilling and then abort queries to reach the
  /// reclaim target.
  virtual uint64_t shrinkCapacity(
      uint64_t targetBytes,
      bool allowSpill = true,
      bool allowAbort = false) = 0;

  /// The internal execution stats of the memory arbitrator.
  struct Stats {
    /// The number of arbitration requests.
    uint64_t numRequests{0};
    /// The number of succeeded arbitration requests.
    uint64_t numSucceeded{0};
    /// The number of aborted arbitration requests.
    uint64_t numAborted{0};
    /// The number of arbitration request failures.
    uint64_t numFailures{0};
    /// The sum of all the arbitration request queue times in microseconds.
    uint64_t queueTimeUs{0};
    /// The sum of all the arbitration run times in microseconds.
    uint64_t arbitrationTimeUs{0};
    /// The amount of memory bytes freed by reducing the memory pool's capacity
    /// without actually freeing memory.
    uint64_t numShrunkBytes{0};
    /// The amount of memory bytes freed by memory reclamation.
    uint64_t numReclaimedBytes{0};
    /// The max memory capacity in bytes.
    uint64_t maxCapacityBytes{0};
    /// The free memory capacity in bytes.
    uint64_t freeCapacityBytes{0};
    /// The free reserved memory capacity in bytes.
    uint64_t freeReservedCapacityBytes{0};
    /// The sum of all reclaim operation durations during arbitration in
    /// microseconds.
    uint64_t reclaimTimeUs{0};
    /// The total number of times of the reclaim attempts that end up failing
    /// due to reclaiming at non-reclaimable stage.
    uint64_t numNonReclaimableAttempts{0};
    /// The total number of memory capacity shrinks.
    uint64_t numShrinks{0};

    Stats(
        uint64_t _numRequests,
        uint64_t _numSucceeded,
        uint64_t _numAborted,
        uint64_t _numFailures,
        uint64_t _queueTimeUs,
        uint64_t _arbitrationTimeUs,
        uint64_t _numShrunkBytes,
        uint64_t _numReclaimedBytes,
        uint64_t _maxCapacityBytes,
        uint64_t _freeCapacityBytes,
        uint64_t _freeReservedCapacityBytes,
        uint64_t _reclaimTimeUs,
        uint64_t _numNonReclaimableAttempts,
        uint64_t _numShrinks);

    Stats() = default;

    Stats operator-(const Stats& other) const;
    bool operator==(const Stats& other) const;
    bool operator!=(const Stats& other) const;
    bool operator<(const Stats& other) const;
    bool operator>(const Stats& other) const;
    bool operator>=(const Stats& other) const;
    bool operator<=(const Stats& other) const;

    bool empty() const {
      return numRequests == 0;
    }

    /// Returns the debug string of this stats.
    std::string toString() const;
  };

  virtual Stats stats() const = 0;

  /// Returns the debug string of this memory arbitrator.
  virtual std::string toString() const = 0;

 protected:
  explicit MemoryArbitrator(const Config& config)
      : capacity_(config.capacity),
        arbitrationStateCheckCb_(config.arbitrationStateCheckCb) {}

  /// Helper utilities used by the memory arbitrator implementations to call
  /// protected methods of memory pool.
  static bool
  growPool(MemoryPool* pool, uint64_t growBytes, uint64_t reservationBytes);

  static uint64_t shrinkPool(MemoryPool* pool, uint64_t targetBytes);

  const uint64_t capacity_;
  const MemoryArbitrationStateCheckCB arbitrationStateCheckCb_;
};

/// Formatter for fmt.
FOLLY_ALWAYS_INLINE std::string format_as(MemoryArbitrator::Stats stats) {
  return stats.toString();
}

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& o,
    const MemoryArbitrator::Stats& stats) {
  return o << stats.toString();
}

/// The memory reclaimer interface is used by memory pool to participate in
/// the memory arbitration execution (enter/leave arbitration process) as well
/// as reclaim memory from the associated query object. We have default
/// implementation that always reclaim memory from the child memory pool with
/// most reclaimable memory. This is used by the query and plan node memory
/// pools which don't need customized operations. A task memory pool needs to
/// to pause a task execution before reclaiming memory from its child pools.
/// This avoids any potential race condition between concurrent memory
/// reclamation operation and the task activities. An operator memory pool needs
/// to to put the the associated task driver thread into suspension state before
/// entering into an arbitration process. It is because the memory arbitrator
/// needs to pause a task execution before reclaim memory from the task. It is
/// possible that the memory arbitration tries to reclaim memory from the task
/// which initiates the memory arbitration request. If we don't put the driver
/// thread into suspension state, then the memory arbitration process might
/// run into deadlock as the task will never be paused. The operator memory pool
/// also needs to reclaim the actually used memory from the associated operator
/// through techniques such as disks spilling.
class MemoryReclaimer {
 public:
  /// Used to collect memory reclaim execution stats.
  struct Stats {
    /// The total number of times of the reclaim attempts that end up failing
    /// due to reclaiming at non-reclaimable stage.
    uint64_t numNonReclaimableAttempts{0};

    /// The total execution time to do the reclaim in microseconds.
    uint64_t reclaimExecTimeUs{0};

    /// The total reclaimed memory bytes.
    uint64_t reclaimedBytes{0};

    /// The total time of task pause during reclaim in microseconds.
    uint64_t reclaimWaitTimeUs{0};

    void reset();

    bool operator==(const Stats& other) const;
    bool operator!=(const Stats& other) const;
    Stats& operator+=(const Stats& other);
  };

  virtual ~MemoryReclaimer() = default;

  static std::unique_ptr<MemoryReclaimer> create();

  /// Invoked memory reclaim function from 'pool' and record execution 'stats'.
  static uint64_t run(const std::function<int64_t()>& func, Stats& stats);

  /// Invoked by the memory arbitrator before entering the memory arbitration
  /// processing. The default implementation does nothing but user can override
  /// this if needs. For example, an operator memory reclaimer needed to put the
  /// associated driver execution into suspension state so that the task which
  /// initiates the memory arbitration request, can be paused for memory
  /// reclamation during the memory arbitration processing.
  virtual void enterArbitration() {}

  /// Invoked by the memory arbitrator after finishes the memory arbitration
  /// processing and is used in pair with 'enterArbitration'. For example, an
  /// operator memory pool needs to moves the associated driver execution out of
  /// the suspension state.
  ///
  /// NOTE: it is guaranteed to be called also on failure path if
  /// enterArbitration has been called.
  virtual void leaveArbitration() noexcept {}

  /// Invoked by the memory arbitrator to get the amount of memory bytes that
  /// can be reclaimed from 'pool'. The function returns true if 'pool' is
  /// reclaimable and returns the estimated reclaimable bytes in
  /// 'reclaimableBytes'.
  virtual bool reclaimableBytes(
      const MemoryPool& pool,
      uint64_t& reclaimableBytes) const;

  /// Invoked by the memory arbitrator to reclaim from memory 'pool' with
  /// specified 'targetBytes'. It is expected to reclaim at least that amount of
  /// memory bytes but there is no guarantees. If 'targetBytes' is zero, then it
  /// reclaims all the reclaimable memory from the memory 'pool'. 'maxWaitMs'
  /// specifies the max time to wait for reclaim if not zero. The memory
  /// reclaim might fail if exceeds the timeout. The function returns the actual
  /// reclaimed memory bytes.
  ///
  /// NOTE: 'maxWaitMs' is optional and the actual memory reclaim implementation
  /// can choose to respect this timeout or not on its own.
  virtual uint64_t reclaim(
      MemoryPool* pool,
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      Stats& stats);

  /// Invoked by the memory arbitrator to abort memory 'pool' and the associated
  /// query execution when encounters non-recoverable memory reclaim error or
  /// fails to reclaim enough free capacity. The abort is a synchronous
  /// operation and we expect most of used memory to be freed after the abort
  /// completes. 'error' should be passed in as the direct cause of the
  /// abortion. It will be propagated all the way to task level for accurate
  /// error exposure.
  virtual void abort(MemoryPool* pool, const std::exception_ptr& error);

 protected:
  MemoryReclaimer() = default;
};

/// Helper class used to measure the memory bytes reclaimed from a memory pool
/// by a memory reclaim function.
class ScopedReclaimedBytesRecorder {
 public:
  ScopedReclaimedBytesRecorder(MemoryPool* pool, int64_t* reclaimedBytes);

  ~ScopedReclaimedBytesRecorder();

 private:
  MemoryPool* const pool_;
  int64_t* const reclaimedBytes_;
  const int64_t reservedBytesBeforeReclaim_;
};

/// The object is used to set/clear non-reclaimable section of an operation in
/// the middle of its execution. It allows the memory arbitrator to reclaim
/// memory from a running operator which is waiting for memory arbitration.
/// 'nonReclaimableSection' points to the corresponding flag of the associated
/// operator.
class ReclaimableSectionGuard {
 public:
  explicit ReclaimableSectionGuard(tsan_atomic<bool>* nonReclaimableSection)
      : nonReclaimableSection_(nonReclaimableSection),
        oldNonReclaimableSectionValue_(*nonReclaimableSection_) {
    *nonReclaimableSection_ = false;
  }

  ~ReclaimableSectionGuard() {
    *nonReclaimableSection_ = oldNonReclaimableSectionValue_;
  }

 private:
  tsan_atomic<bool>* const nonReclaimableSection_;
  const bool oldNonReclaimableSectionValue_;
};

class NonReclaimableSectionGuard {
 public:
  explicit NonReclaimableSectionGuard(tsan_atomic<bool>* nonReclaimableSection)
      : nonReclaimableSection_(nonReclaimableSection),
        oldNonReclaimableSectionValue_(*nonReclaimableSection_) {
    *nonReclaimableSection_ = true;
  }

  ~NonReclaimableSectionGuard() {
    *nonReclaimableSection_ = oldNonReclaimableSectionValue_;
  }

 private:
  tsan_atomic<bool>* const nonReclaimableSection_;
  const bool oldNonReclaimableSectionValue_;
};

/// The memory arbitration context which is set on per-thread local variable by
/// memory arbitrator. It is used to indicate a running thread is under memory
/// arbitration processing or not. This helps to enable sanity check such as all
/// the memory reservations during memory arbitration should come from the
/// spilling memory pool.
struct MemoryArbitrationContext {
  const MemoryPool* requestor;
};

/// Object used to set/restore the memory arbitration context when a thread is
/// under memory arbitration processing.
class ScopedMemoryArbitrationContext {
 public:
  explicit ScopedMemoryArbitrationContext(const MemoryPool* requestor);

  /// Can be used to restore a previously captured MemoryArbitrationContext.
  /// contextToRestore can be nullptr if there was no context at the time it was
  /// captured, in which case arbitrationCtx is unchanged upon
  /// contruction/destruction of this object.
  explicit ScopedMemoryArbitrationContext(
      const MemoryArbitrationContext* contextToRestore);

  ~ScopedMemoryArbitrationContext();

 private:
  MemoryArbitrationContext* const savedArbitrationCtx_{nullptr};
  MemoryArbitrationContext currentArbitrationCtx_;
};

/// Object used to setup arbitration context for a memory pool.
class ScopedMemoryPoolArbitrationCtx {
 public:
  explicit ScopedMemoryPoolArbitrationCtx(MemoryPool* pool);

  ~ScopedMemoryPoolArbitrationCtx();

 private:
  MemoryPool* const pool_;
};

/// Returns the memory arbitration context set by a per-thread local variable if
/// the running thread is under memory arbitration processing.
const MemoryArbitrationContext* memoryArbitrationContext();

/// Returns true if the running thread is under memory arbitration or not.
bool underMemoryArbitration();

/// The function triggers memory arbitration by shrinking memory pools from
/// 'manager' by invoking shrinkPools API. If 'manager' is not set, then it
/// shrinks from the process wide memory manager. If 'targetBytes' is zero, then
/// it reclaims all the memory from 'manager' if possible. If 'allowSpill' is
/// true, then it allows to reclaim the used memory by spilling.
class MemoryManager;
void testingRunArbitration(
    uint64_t targetBytes = 0,
    bool allowSpill = true,
    MemoryManager* manager = nullptr);

/// The function triggers memory arbitration by shrinking memory pools from
/// 'manager' of 'pool' by invoking its shrinkPools API. If 'targetBytes' is
/// zero, then it reclaims all the memory from 'manager' if possible. If
/// 'allowSpill' is true, then it allows to reclaim the used memory by spilling.
void testingRunArbitration(
    MemoryPool* pool,
    uint64_t targetBytes = 0,
    bool allowSpill = true);
} // namespace facebook::velox::memory

#if FMT_VERSION < 100100
template <>
struct fmt::formatter<facebook::velox::memory::MemoryArbitrator::Stats>
    : formatter<std::string> {
  auto format(
      facebook::velox::memory::MemoryArbitrator::Stats s,
      format_context& ctx) {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};
#endif
