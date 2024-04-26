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

 private:
  // The kind string of shared arbitrator.
  inline static const std::string kind_{"SHARED"};

  class ScopedArbitration {
   public:
    // Used by arbitration request NOT initiated from memory pool, e.g. through
    // shrinkPools() API.
    explicit ScopedArbitration(SharedArbitrator* arbitrator);

    // Used by arbitration request initiated from a memory pool.
    explicit ScopedArbitration(
        MemoryPool* requestor,
        SharedArbitrator* arbitrator);

    ~ScopedArbitration();

   private:
    MemoryPool* const requestor_;
    SharedArbitrator* const arbitrator_;
    const std::chrono::steady_clock::time_point startTime_;
    const ScopedMemoryArbitrationContext arbitrationCtx_;
  };

  // Invoked to check if the memory growth will exceed the memory pool's max
  // capacity limit or the arbitrator's node capacity limit.
  bool checkCapacityGrowth(const MemoryPool& pool, uint64_t targetBytes) const;

  // Invoked to ensure the memory growth request won't exceed the requestor's
  // max capacity as well as the arbitrator's node capacity. If it does, then we
  // first need to reclaim the used memory from the requestor itself to ensure
  // the memory growth won't exceed the capacity limit, and then proceed with
  // the memory arbitration process. The reclaimed memory capacity returns to
  // the arbitrator, and let the memory arbitration process to grow the
  // requestor capacity accordingly.
  bool ensureCapacity(MemoryPool* requestor, uint64_t targetBytes);

  bool arbitrateMemory(
      MemoryPool* requestor,
      std::vector<Candidate>& candidates,
      uint64_t targetBytes);

  // Invoked to start next memory arbitration request, and it will wait for the
  // serialized execution if there is a running or other waiting arbitration
  // requests.
  void startArbitration(const std::string& contextMsg);

  // Invoked by a finished memory arbitration request to kick off the next
  // arbitration request execution if there are any ones waiting.
  void finishArbitration();

  // Invoked to get the memory stats of the candidate memory pools for
  // arbitration. If 'freeCapacityOnly' is true, then we only get free capacity
  // stats for each candidate memory pool.
  std::vector<SharedArbitrator::Candidate> getCandidateStats(
      const std::vector<std::shared_ptr<MemoryPool>>& pools,
      bool freeCapacityOnly = false);

  // Invoked to reclaim free memory capacity from 'candidates' without actually
  // freeing used memory.
  //
  // NOTE: the function might sort 'candidates' based on each candidate's free
  // capacity internally.
  uint64_t reclaimFreeMemoryFromCandidates(
      std::vector<Candidate>& candidates,
      uint64_t targetBytes);

  // Invoked to reclaim used memory capacity from 'candidates' by spilling.
  //
  // NOTE: the function might sort 'candidates' based on each candidate's
  // reclaimable memory internally.
  uint64_t reclaimUsedMemoryFromCandidatesBySpill(
      MemoryPool* requestor,
      std::vector<Candidate>& candidates,
      uint64_t targetBytes);

  // Invoked to reclaim used memory capacity from 'candidates' by aborting the
  // top memory users' queries.
  uint64_t reclaimUsedMemoryFromCandidatesByAbort(
      std::vector<Candidate>& candidates,
      uint64_t targetBytes);

  // Invoked to grow 'pool' capacity by 'growBytes' and commit used reservation
  // by 'reservationBytes'. The function throws if the grow fails from memory
  // pool.
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
  // success and false if the requestor itself has been selected as the victim.
  // We don't abort the requestor itself but just fails the arbitration to let
  // the user decide to either proceed with the query or fail it.
  bool handleOOM(
      MemoryPool* requestor,
      uint64_t targetBytes,
      std::vector<Candidate>& candidates);

  // Decrements free capacity from the arbitrator with up to
  // 'maxBytesToReserve'. The arbitrator might have less free available
  // capacity. The function returns the actual decremented free capacity bytes.
  // If 'minBytesToReserve' is not zero and there is less than 'minBytes'
  // available in non-reserved capacity, then the arbitrator tries to decrement
  // up to 'minBytes' from the reserved capacity.
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

  std::string toStringLocked() const;

  Stats statsLocked() const;

  void incrementGlobalArbitrationCount();
  void incrementLocalArbitrationCount();

  // Returns the max reclaimable capacity from 'pool' which includes both used
  // and free capacities.
  int64_t maxReclaimableCapacity(const MemoryPool& pool) const;

  // Returns the free memory capacity that can be reclaimed from 'pool' by
  // shrink.
  int64_t reclaimableFreeCapacity(const MemoryPool& pool) const;

  // Returns the used memory capacity that can be reclaimed from 'pool' by disk
  // spill.
  int64_t reclaimableUsedCapacity(const MemoryPool& pool) const;

  // Returns the minimal amount of memory capacity to grow for 'pool' to have
  // the reserved capacity as specified by 'memoryPoolReservedCapacity_'.
  int64_t minGrowCapacity(const MemoryPool& pool) const;

  // Updates the free capacity metrics on capacity changes.
  //
  // TODO: move this update to velox runtime monitoring service once available.
  void updateFreeCapacityMetrics() const;

  mutable std::mutex mutex_;
  tsan_atomic<uint64_t> freeReservedCapacity_{0};
  tsan_atomic<uint64_t> freeNonReservedCapacity_{0};
  // Indicates if there is a running arbitration request or not.
  bool running_{false};

  // The promises of the arbitration requests waiting for the serialized
  // execution.
  std::vector<ContinuePromise> waitPromises_;

  tsan_atomic<uint64_t> numRequests_{0};
  std::atomic<uint64_t> numSucceeded_{0};
  tsan_atomic<uint64_t> numAborted_{0};
  tsan_atomic<uint64_t> numFailures_{0};
  tsan_atomic<uint64_t> queueTimeUs_{0};
  tsan_atomic<uint64_t> arbitrationTimeUs_{0};
  tsan_atomic<uint64_t> numShrunkBytes_{0};
  tsan_atomic<uint64_t> numReclaimedBytes_{0};
  tsan_atomic<uint64_t> reclaimTimeUs_{0};
  tsan_atomic<uint64_t> numNonReclaimableAttempts_{0};
  tsan_atomic<uint64_t> numReserves_{0};
  tsan_atomic<uint64_t> numReleases_{0};
};
} // namespace facebook::velox::memory
