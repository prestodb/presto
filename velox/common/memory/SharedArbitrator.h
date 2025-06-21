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

#include <folly/executors/CPUThreadPoolExecutor.h>
#include "velox/common/base/Counters.h"
#include "velox/common/base/GTestMacros.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/ArbitrationOperation.h"
#include "velox/common/memory/ArbitrationParticipant.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryArbitrator.h"

namespace facebook::velox::memory {
namespace test {
class SharedArbitratorTestHelper;
}

/// Used to achieve dynamic memory sharing among running queries. When a query
/// memory pool exceeds its current memory capacity, the arbitrator tries to
/// grow its capacity through memory arbitration. If the query memory pool
/// exceeds its max memory capacity, then the arbitrator reclaims used memory
/// from the the query itself which is the local arbitration. If not, the
/// arbitrator tries to grow its capacity with the free unused capacity or
/// reclaim the unused memory from other running queries. If there is still
/// not enough free capacity, the arbitrator kicks off the global arbitration
/// running at the background to reclaim used memory from other running queries.
/// The request query memory pool waits until the global arbitration reclaims
/// enough memory to grow its capacity or fails if exceeds the max arbitration
/// time limit. The background global arbitration runs by a single thread while
/// the actual memory reclaim is executed by a thread pool to parallelize the
/// memory reclamation from multiple running queries at the same time. The
/// global arbitration first tries to reclaim memory by disk spilling and if it
/// can't quickly reclaim enough memory, it then switchs to abort the younger
/// queries which also have more memory usage.
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
    static constexpr std::string_view kMaxMemoryArbitrationTime{
        "max-memory-arbitration-time"};
    static constexpr std::string_view kDefaultMaxMemoryArbitrationTime{"5m"};
    static uint64_t maxMemoryArbitrationTimeNs(
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

    /// Specifies the minimum bytes to reclaim from a participant at a time. The
    /// bigger of 'memory-pool-min-reclaim-bytes' and
    /// 'memory-pool-min-reclaim-pct' will be applied as the minimum reclaim
    /// bytes. The global arbitration also avoids to reclaim from a participant
    /// if its reclaimable used capacity is less than this threshold. This is to
    /// prevent inefficient memory reclaim operations on a participant with
    /// small reclaimable used capacity which could causes a large number of
    /// small spilled file on disk.
    static constexpr std::string_view kMemoryPoolMinReclaimBytes{
        "memory-pool-min-reclaim-bytes"};
    static constexpr std::string_view kDefaultMemoryPoolMinReclaimBytes{
        "128MB"};
    static uint64_t memoryPoolMinReclaimBytes(
        const std::unordered_map<std::string, std::string>& configs);

    static constexpr std::string_view kMemoryPoolMinReclaimPct{
        "memory-pool-min-reclaim-pct"};
    static constexpr double kDefaultMemoryPoolMinReclaimPct{0.25};
    static double memoryPoolMinReclaimPct(
        const std::unordered_map<std::string, std::string>& configs);

    /// Specifies the starting memory capacity limit for global arbitration to
    /// search for victim participant to reclaim used memory by spill. For
    /// participants with reclaimable used capacity larger than the limit, the
    /// global arbitration choose to spill the lowest priority participant with
    /// highest reclaimable used capacity. The spill capacity limit is reduced
    /// by half if couldn't find a victim participant until reaches to zero.
    ///
    /// NOTE: the limit must be zero or a power of 2.
    static constexpr std::string_view kMemoryPoolSpillCapacityLimit{
        "memory-pool-spill-capacity-limit"};
    static constexpr std::string_view kDefaultMemoryPoolSpillCapacityLimit{
        "4GB"};
    static uint64_t memoryPoolSpillCapacityLimit(
        const std::unordered_map<std::string, std::string>& configs);

    /// Specifies the starting memory capacity limit for global arbitration to
    /// search for victim participant to reclaim used memory by abort. For
    /// participants with capacity larger than the limit, the global arbitration
    /// choose to abort the participant that has lowest priority and shortest
    /// execution time (largest participant id). This helps to let the low
    /// priority queries to be aborted first, as well as old queries to run to
    /// completion. The abort capacity limit is reduced by half if couldn't find
    /// a victim participant until reaches to zero.
    ///
    /// NOTE: the limit must be zero or a power of 2.
    static constexpr std::string_view kMemoryPoolAbortCapacityLimit{
        "memory-pool-abort-capacity-limit"};
    static constexpr std::string_view kDefaultMemoryPoolAbortCapacityLimit{
        "1GB"};
    static uint64_t memoryPoolAbortCapacityLimit(
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

    /// Floating point number used in calculating how many threads we would use
    /// for memory reclaim execution: hw_concurrency x multiplier. 0.5 is
    /// default.
    static constexpr std::string_view kMemoryReclaimThreadsHwMultiplier{
        "memory-reclaim-threads-hw-multiplier"};
    static constexpr double kDefaultMemoryReclaimThreadsHwMultiplier{0.5};
    static double memoryReclaimThreadsHwMultiplier(
        const std::unordered_map<std::string, std::string>& configs);

    /// If true, allows memory arbitrator to reclaim used memory cross query
    /// memory pools.
    static constexpr std::string_view kGlobalArbitrationEnabled{
        "global-arbitration-enabled"};
    static constexpr bool kDefaultGlobalArbitrationEnabled{true};
    static bool globalArbitrationEnabled(
        const std::unordered_map<std::string, std::string>& configs);

    /// If not zero, specifies the minimum amount of memory to reclaim by global
    /// memory arbitration as percentage of total arbitrator memory capacity.
    static constexpr std::string_view kGlobalArbitrationMemoryReclaimPct{
        "global-arbitration-memory-reclaim-pct"};
    static constexpr uint32_t kDefaultGlobalMemoryArbitrationReclaimPct{10};
    static uint32_t globalArbitrationMemoryReclaimPct(
        const std::unordered_map<std::string, std::string>& configs);

    /// The ratio used with 'memory-reclaim-max-wait-time', beyond which, global
    /// arbitration will no longer reclaim memory by spilling, but instead
    /// directly abort. It is only in effect when 'global-arbitration-enabled'
    /// is true
    static constexpr std::string_view kGlobalArbitrationAbortTimeRatio{
        "global-arbitration-abort-time-ratio"};
    static constexpr double kDefaultGlobalArbitrationAbortTimeRatio{0.5};
    static double globalArbitrationAbortTimeRatio(
        const std::unordered_map<std::string, std::string>& configs);

    /// If true, global arbitration will not reclaim memory by spilling, but
    /// only by aborting. This flag is only effective if
    /// 'global-arbitration-enabled' is true
    static constexpr std::string_view kGlobalArbitrationWithoutSpill{
        "global-arbitration-without-spill"};
    static constexpr bool kDefaultGlobalArbitrationWithoutSpill{false};
    static bool globalArbitrationWithoutSpill(
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

  void shutdown() override;

  void addPool(const std::shared_ptr<MemoryPool>& pool) final;

  void removePool(MemoryPool* pool) final;

  void growCapacity(MemoryPool* pool, uint64_t requestBytes) final;

  /// NOTE: only support shrinking away all the unused free capacity for now.
  uint64_t shrinkCapacity(MemoryPool* pool, uint64_t requestBytes) final;

  uint64_t shrinkCapacity(
      uint64_t requestBytes,
      bool allowSpill = true,
      bool force = false) override final;

  Stats stats() const final;

  std::string kind() const override;

  std::string toString() const final;

  /// Operator level runtime stats reported for an arbitration operation
  /// execution.
  static inline const std::string kMemoryArbitrationWallNanos{
      "memoryArbitrationWallNanos"};
  static inline const std::string kLocalArbitrationCount{
      "localArbitrationCount"};
  static inline const std::string kLocalArbitrationWaitWallNanos{
      "localArbitrationWaitWallNanos"};
  static inline const std::string kLocalArbitrationExecutionWallNanos{
      "localArbitrationExecutionWallNanos"};
  static inline const std::string kGlobalArbitrationWaitCount{
      "globalArbitrationWaitCount"};
  static inline const std::string kGlobalArbitrationWaitWallNanos{
      "globalArbitrationWaitWallNanos"};

 private:
  // Define the internal execution states of the arbitrator.
  enum class State {
    kRunning,
    // Indicates the arbitrator is shutting down. The arbitrator doesn't accept
    // any new arbitration requests under this state except remove pool as the
    // last pool reference might be still held by the background global memory
    // arbitration.
    kShutdown,
  };

  // The kind string of shared arbitrator.
  inline static const std::string kind_{"SHARED"};

  // Used to manage an arbitration operation execution. It starts 'op' execution
  // in ctor and finishes its exection in dtor.
  class ScopedArbitration {
   public:
    explicit ScopedArbitration(
        SharedArbitrator* arbitrator,
        ArbitrationOperation* op);

    ~ScopedArbitration();

   private:
    SharedArbitrator* const arbitrator_;
    ArbitrationOperation* const operation_;
    const ScopedMemoryArbitrationContext arbitrationCtx_;
    const std::chrono::steady_clock::time_point startTime_;
  };

  // The scoped object to cover the global arbitration execution. It ensures
  // the setups and teardowns of 'arbitrator' global arbitration state and
  // thread_local 'arbitrationCtx' global context.
  class GlobalArbitrationSection {
   public:
    explicit GlobalArbitrationSection(SharedArbitrator* arbitrator);
    ~GlobalArbitrationSection();

   private:
    SharedArbitrator* const arbitrator_;

    // Default to global arbitration context.
    const memory::ScopedMemoryArbitrationContext arbitrationCtx_{};
  };

  FOLLY_ALWAYS_INLINE void checkRunning() {
    std::lock_guard<std::mutex> l(stateMutex_);
    VELOX_CHECK(!hasShutdownLocked(), "SharedArbitrator is not running");
  }

  FOLLY_ALWAYS_INLINE bool hasShutdownLocked() const {
    return state_ == State::kShutdown;
  }

  FOLLY_ALWAYS_INLINE void checkGlobalArbitrationEnabled() const {
    VELOX_CHECK(globalArbitrationEnabled_, "Global arbitration is not enabled");
  }

  // Invoked to get the arbitration participant by 'name'. The function returns
  // std::nullopt if the underlying query memory pool is destroyed.
  std::optional<ScopedArbitrationParticipant> getParticipant(
      const std::string& name) const;

  // Invoked to create an operation for an arbitration request from given query
  // memory 'pool'.
  ArbitrationOperation createArbitrationOperation(
      MemoryPool* pool,
      uint64_t requestBytes);

  // Run arbitration to grow capacity for 'op'. The function returns true on
  // success.
  void growCapacity(ArbitrationOperation& op);

  // Invoked to start execution of 'op'. It waits for the serialized execution
  // on the same arbitration participant and returns when 'op' is ready to run.
  void startArbitration(ArbitrationOperation* op);

  // Invoked when 'op' has finished. The function kicks off the next arbitration
  // operation waiting on the same participant to run if there is one.
  void finishArbitration(ArbitrationOperation* op);

  // Invoked to check if the capacity growth exceeds the participant's max
  // capacity limit or the arbitrator's capacity limit.
  bool checkCapacityGrowth(ArbitrationOperation& op) const;

  // Invoked to ensure the capacity growth won't exceed the participant's max
  // capacity limit by reclaiming used memory from the participant itself.
  bool ensureCapacity(ArbitrationOperation& op);

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

  // Invoked to initialize the global arbitration on arbitrator start-up. It
  // starts the background threads to used memory from running queries
  // on-demand.
  void setupGlobalArbitration(
      uint64_t spillCapacityLimit,
      uint64_t abortCapacityLimit);

  // Invoked to stop the global arbitration threads on shut-down.
  void shutdownGlobalArbitration();

  // The main function of the global arbitration control thread.
  void globalArbitrationMain();

  // Invoked by arbitration operation to wake up the global arbitration control
  // thread to reclaim used memory when there is no free capacity in the system.
  void wakeupGlobalArbitrationThread();

  // Invoked by global arbitration control thread to run global arbitration.
  void runGlobalArbitration();

  // Helper method used by 'runGlobalArbitration()' to decide if current
  // iteration of global run should directly reclaim capacity by aborting
  // queries.
  bool globalArbitrationShouldReclaimByAbort(
      uint64_t globalRunElapsedTimeNs,
      bool hasReclaimedByAbort,
      bool allParticipantsReclaimed,
      uint64_t lastReclaimedBytes) const;

  // Invoked to get the global arbitration target in bytes.
  uint64_t getGlobalArbitrationTarget();

  // Invoked to run global arbitration to reclaim free or used memory from other
  // queries. The global arbitration run is protected by the exclusive lock of
  // 'arbitrationLock_' for serial execution mode. The function throws on
  // failure.
  void startAndWaitGlobalArbitration(ArbitrationOperation& op);

  // Invoked to get stats of candidate participants for arbitration. If
  // 'freeCapacityOnly' is true, then we only get reclaimable free capacity from
  // each participant.
  std::vector<ArbitrationCandidate> getCandidates(
      bool freeCapacityOnly = false);

  // Invoked to reclaim unused memory capacity from participants without
  // actually freeing used memory. The function returns the actually reclaimed
  // free capacity in bytes.
  uint64_t reclaimUnusedCapacity();

  // Sorts 'candidates' based on reclaimable free capacity in descending order.
  static void sortCandidatesByReclaimableFreeCapacity(
      std::vector<ArbitrationCandidate>& candidates);

  // Invoked to reclaim the specified used memory capacity from one or more
  // participants in parallel by spilling.
  //
  // 'reclaimedParticipants' keeps track of the participants that have been
  // reclaimed by spilling. It will be taken as input to avoid reclaiming from
  // these participants again. It will also be updated when additional
  // participants are reclaimed. From caller's perspective, it should be kept
  // and provided from across multiple global arbitration runs.
  //
  // 'failedParticipants' keeps track of the participants that have failed to
  // reclaim any memory by spilling. This could happen if there is some unknown
  // bug or limitation in specific spillable operator implementation. It will be
  // taken as input to avoid reclaiming from these participants again. It will
  // also be updated when additional participants fail to be reclaimed any
  // memory. From caller's perspective, it should be kept and provided from
  // across multiple global arbitration runs.
  //
  // 'allParticipantsReclaimed' returns if all participants have been
  // reclaimed by spilling so far. It is used by gllobal arbitration to decide
  // if need to switch to abort to reclaim used memory in the next arbitration
  // round. The function returns the actually reclaimed used capacity in bytes.
  //
  // NOTE: the function sorts participants based on their reclaimable used
  // memory capacity, and reclaims from participants with larger reclaimable
  // used memory first.
  uint64_t reclaimUsedMemoryBySpill(
      uint64_t targetBytes,
      std::unordered_set<uint64_t>& reclaimedParticipants,
      std::unordered_set<uint64_t>& failedParticipants,
      bool& allParticipantsReclaimed);

  uint64_t reclaimUsedMemoryBySpill(uint64_t targetBytes);

  // Invoked to reclaim the used memory capacity to abort the participant with
  // the largest capacity to free up memory. The function returns the actually
  // reclaimed capacity in bytes.
  //
  // The function returns the total of released and to be released capacity,
  // including the soon to be released capacity from the victim query. Returns
  // zero if there is no eligible participant to abort. If 'force' is true,
  // it picks up the youngest participant which has largest participant id to
  // abort if there is no eligible one.
  uint64_t reclaimUsedMemoryByAbort(bool force);

  // Sorts and groups 'candidates' for spilling. The sort firstly groups
  // candidates first based on 'globalArbitrationSpillCapacityLimits_', larger
  // bucket in the front. Then order each group based on priority and
  // reclaimable used capacity in descending order, with lower priority (higher
  // priority value) and higher reclaimable used capacity ones in front.
  // Priority takes precedence over reclaimable used capacity.
  std::vector<std::vector<ArbitrationCandidate>> sortAndGroupSpillCandidates(
      std::vector<ArbitrationCandidate>&& candidates);

  // Sorts 'candidates' based on participant's reclaimer priority in descending
  // order, putting lower priority ones (with higher priority value) first, and
  // high priority ones (with lower priority value) later.
  static std::vector<std::vector<ArbitrationCandidate>>
  sortAndGroupAbortCandidates(std::vector<ArbitrationCandidate>&& candidates);

  // Finds the participant victim to abort to free used memory based on the
  // participant's memory capacity and age. The function returns std::nullopt if
  // there is no eligible candidate. If 'force' is true, it picks up the
  // youngest participant to abort if there is no eligible one.
  std::optional<ArbitrationCandidate> findAbortCandidate(bool force);

  // Invoked to use free capacity from arbitrator to grow participant's
  // capacity.
  bool growWithFreeCapacity(ArbitrationOperation& op);

  // Checks if the operation has been aborted or not. The function throws if
  // aborted.
  void checkIfAborted(ArbitrationOperation& op);

  // Checks if the operation has timed out or not. The function throws if timed
  // out.
  void checkIfTimeout(ArbitrationOperation& op);

  // Checks if the request participant already has enough free capacity for the
  // growth. This could happen if there are multiple arbitration operations from
  // the same participant. When the first served operation succeeds, it might
  // have reserved enough capacity for the followup operations.
  bool maybeGrowFromSelf(ArbitrationOperation& op);

  // Invoked to grow 'participant' capacity by 'growBytes' and commit used
  // reservation by 'reservationBytes'. The function throws if the growth fails.
  void checkedGrow(
      const ScopedArbitrationParticipant& participant,
      uint64_t growBytes,
      uint64_t reservationBytes);

  // Invoked to reclaim used memory from 'participant' with specified
  // 'targetBytes'. The function returns the actually freed capacity.
  // 'localArbitration' is true when the reclaim attempt is for a local
  // arbitration.
  uint64_t reclaim(
      const ScopedArbitrationParticipant& participant,
      uint64_t targetBytes,
      uint64_t timeoutNs,
      bool localArbitration) noexcept;

  uint64_t shrink(
      const ScopedArbitrationParticipant& participant,
      bool reclaimAll);

  // Invoked to abort 'participant' with 'error'.
  uint64_t abort(
      const ScopedArbitrationParticipant& participant,
      const std::exception_ptr& error);

  // Allocates capacity for a given participant with 'requestBytes'. The
  // arbitrator might allocate up to 'maxAllocateBytes'. If there is not enough
  // capacity in non-reserved free capacity pool, then the arbitrator tries to
  // allocate up to 'minAllocateBytes' from the reserved capacity pool. The
  // function returns the allocated bytes. It is set to a value no less than
  // 'requestBytes' on success and zero on failure.
  uint64_t allocateCapacity(
      uint64_t participantId,
      uint64_t requestBytes,
      uint64_t maxAllocateBytes,
      uint64_t minAllocateBytes);

  uint64_t allocateCapacityLocked(
      uint64_t participantId,
      uint64_t requestBytes,
      uint64_t maxAllocateBytes,
      uint64_t minAllocateBytes);

  // Invoked to free capacity back to the arbitrator, and wake up the global
  // arbitration waiters if there is sufficient free capacity.
  void freeCapacity(uint64_t bytes);

  // 'resumes' contains the global arbitration waiters to resume.
  void freeCapacityLocked(
      uint64_t bytes,
      std::vector<ContinuePromise>& resumes);

  // Frees reserved capacity up to 'bytes' until reaches to the reserved
  // capacity limit. 'bytes' is updated accordingly.
  void freeReservedCapacityLocked(uint64_t& bytes);

  // Invoked by freeCapacity() to resume a set of oldest global arbitration
  // waiters that could be fulfilled their global arbitration requests from
  // current available free capacity.
  void resumeGlobalArbitrationWaitersLocked(
      std::vector<ContinuePromise>& resumes);

  // Removes the arbitration operation with 'id' from the global arbitration
  // wait list. It is invoked by participant abort or global arbitration wait
  // time out.
  void removeGlobalArbitrationWaiter(uint64_t id);

  // Increments the global arbitration wait count in both arbitrator and the
  // corresponding operator's runtime stats.
  void incrementGlobalArbitrationWaitCount();

  // Increments the local arbitration count in both arbitrator and the
  // corresponding operator's runtime stats.
  void incrementLocalArbitrationCount();

  Stats statsLocked() const;

  void updateMemoryReclaimStats(
      uint64_t reclaimedBytes,
      uint64_t reclaimTimeNs,
      bool localArbitration,
      const MemoryReclaimer::Stats& stats);

  void updateArbitrationRequestStats();

  void updateArbitrationFailureStats();

  void updateGlobalArbitrationStats(
      uint64_t arbitrationTimeNs,
      uint64_t arbitrationBytes);

  const uint64_t capacity_;
  const MemoryArbitrationStateCheckCB arbitrationStateCheckCb_;
  const uint64_t reservedCapacity_;
  const bool checkUsageLeak_;
  const uint64_t maxArbitrationTimeNs_;
  const ArbitrationParticipant::Config participantConfig_;
  const double memoryReclaimThreadsHwMultiplier_;
  const bool globalArbitrationEnabled_;
  const uint32_t globalArbitrationMemoryReclaimPct_;
  const double globalArbitrationAbortTimeRatio_;
  const bool globalArbitrationWithoutSpill_;

  // The executor used to reclaim memory from multiple participants in parallel
  // at the background for global arbitration or external memory reclamation.
  std::unique_ptr<folly::CPUThreadPoolExecutor> memoryReclaimExecutor_;

  std::atomic_uint64_t nextParticipantId_{0};
  mutable folly::SharedMutex participantLock_;
  std::unordered_map<std::string, std::shared_ptr<ArbitrationParticipant>>
      participants_;

  // Mutex used to protect the arbitrator internal state.
  mutable std::mutex stateMutex_;
  State state_{State::kRunning};

  tsan_atomic<uint64_t> freeReservedCapacity_{0};
  tsan_atomic<uint64_t> freeNonReservedCapacity_{0};

  // Indicates if the global arbitration is currently running or not.
  tsan_atomic<bool> globalArbitrationRunning_{false};

  // The abort capacity limits listed in descending order. It is used by global
  // arbitration to choose the victim to abort. It starts with the largest limit
  // and abort the youngest participant whose capacity is larger than the limit.
  // If there is no such participant, it goes to the next limit and so on.
  std::vector<uint64_t> globalArbitrationAbortCapacityLimits_;

  // The spill capacity limits listed in descending order. It is used by global
  // arbitration to choose the victim to spill. It starts with the largest limit
  // and spill the largest reclaimable used capacity participant whose
  // reclaimable used capacity is larger than the limit. If there is no such
  // participant, it goes to the next limit and so on.
  std::vector<uint64_t> globalArbitrationSpillCapacityLimits_;

  // The global arbitration control thread which runs the global arbitration at
  // the background, and dispatch the actual memory reclaim work on different
  // participants to 'globalArbitrationExecutor_' and collects the results back.
  std::unique_ptr<std::thread> globalArbitrationController_;

  // Signal used to wakeup 'globalArbitrationController_' to run global
  // arbitration on-demand.
  std::condition_variable_any globalArbitrationThreadCv_;

  // Records an arbitration operation waiting for global memory arbitration.
  struct ArbitrationWait {
    ArbitrationOperation* op;
    ContinuePromise resumePromise;
    uint64_t allocatedBytes{0};

    ArbitrationWait(ArbitrationOperation* _op, ContinuePromise&& _resumePromise)
        : op(_op), resumePromise(std::move(_resumePromise)) {}
  };

  // The map of global arbitration waiters. The key is the arbitration operation
  // id which is set to the id of the corresponding arbitration participant.
  // This ensures to satisfy the arbitration request in the order of the age of
  // arbitration participants with old participants being served first.
  std::map<uint64_t, ArbitrationWait*> globalArbitrationWaiters_;

  tsan_atomic<uint64_t> globalArbitrationRuns_{0};
  tsan_atomic<uint64_t> globalArbitrationTimeNs_{0};
  tsan_atomic<uint64_t> globalArbitrationBytes_{0};

  std::atomic_uint64_t numRequests_{0};
  std::atomic_uint32_t numRunning_{0};
  std::atomic_uint64_t numAborted_{0};
  std::atomic_uint64_t numFailures_{0};
  std::atomic_uint64_t reclaimedFreeBytes_{0};
  std::atomic_uint64_t reclaimedUsedBytes_{0};
  std::atomic_uint64_t numNonReclaimableAttempts_{0};

  friend class GlobalArbitrationSection;
  friend class test::SharedArbitratorTestHelper;
};
} // namespace facebook::velox::memory
