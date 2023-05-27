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
class SharedArbitrator : public MemoryArbitrator {
 public:
  explicit SharedArbitrator(const Config& config)
      : MemoryArbitrator(config), freeCapacity_(capacity_) {
    VELOX_CHECK_EQ(kind_, Kind::kShared);
  }

  ~SharedArbitrator() override;

  void reserveMemory(MemoryPool* pool, uint64_t /*unused*/) final;

  void releaseMemory(MemoryPool* pool) final;

  bool growMemory(
      MemoryPool* pool,
      const std::vector<std::shared_ptr<MemoryPool>>& candidatePools,
      uint64_t targetBytes) final;

  Stats stats() const final;

  std::string toString() const final;

  // The candidate memory pool stats used by arbitration.
  struct Candidate {
    bool reclaimable{false};
    uint64_t reclaimableBytes{0};
    uint64_t freeBytes{0};
    MemoryPool* pool;
  };

 private:
  class ScopedArbitration {
   public:
    ScopedArbitration(MemoryPool* requestor, SharedArbitrator* arbitrator);

    ~ScopedArbitration();

   private:
    MemoryPool* const requestor_;
    SharedArbitrator* const arbitrator_;
    const std::chrono::steady_clock::time_point startTime_;
  };

  // Invoked to capture the candidate memory pools stats for arbitration.
  static std::vector<Candidate> getCandidateStats(
      const std::vector<std::shared_ptr<MemoryPool>>& pools);

  void sortCandidatesByReclaimableMemory(std::vector<Candidate>& candidates);

  void sortCandidatesByFreeCapacity(std::vector<Candidate>& candidates);

  bool arbitrateMemory(
      MemoryPool* requestor,
      std::vector<Candidate>& candidates,
      uint64_t targetBytes);

  // Invoked to start next memory arbitration request, and it will wait for the
  // serialized execution if there is a running or other waiting arbitration
  // requests.
  void startArbitration(MemoryPool* requestor);

  // Invoked by a finished memory arbitration request to kick off the next
  // arbitration request execution if there are any ones waiting.
  void finishArbitration();

  // Invoked to reclaim free memory capacity from 'candidates' without actually
  // freeing used memory.
  //
  // NOTE: the function might sort 'candidates' based on each candidate's free
  // capacity internally.
  uint64_t reclaimFreeMemoryFromCandidates(
      std::vector<Candidate>& candidates,
      uint64_t targetBytes);

  // Invoked to reclaim used memory capacity from 'candidates'.
  //
  // NOTE: the function might sort 'candidates' based on each candidate's
  // reclaimable memory internally.
  uint64_t reclaimUsedMemoryFromCandidates(
      MemoryPool* requestor,
      std::vector<Candidate>& candidates,
      uint64_t targetBytes);

  // Invoked to reclaim used memory from 'pool' with specified 'targetBytes'.
  // The function returns the actually freed capacity.
  uint64_t reclaim(MemoryPool* pool, uint64_t targetBytes) noexcept;

  // Invoked to abort memory 'pool'.
  void abort(MemoryPool* pool);

  // Decrement free capacity from the arbitrator with up to 'bytes'. The
  // arbitrator might have less free available capacity. The function returns
  // the actual decremented free capacity bytes.
  uint64_t decrementFreeCapacity(uint64_t bytes);
  uint64_t decrementFreeCapacityLocked(uint64_t bytes);

  // Increment free capacity by 'bytes'.
  void incrementFreeCapacity(uint64_t bytes);
  void incrementFreeCapacityLocked(uint64_t bytes);

  std::string toStringLocked() const;

  Stats statsLocked() const;

  mutable std::mutex mutex_;
  uint64_t freeCapacity_{0};
  // Indicates if there is a running arbitration request or not.
  bool running_{false};

  // The promises of the arbitration requests waiting for the serialized
  // execution.
  std::vector<ContinuePromise> waitPromises_;

  tsan_atomic<uint64_t> numRequests_{0};
  tsan_atomic<uint64_t> numAborted_{0};
  std::atomic<uint64_t> numFailures_{0};
  tsan_atomic<uint64_t> queueTimeUs_{0};
  tsan_atomic<uint64_t> arbitrationTimeUs_{0};
  tsan_atomic<uint64_t> numShrunkBytes_{0};
  tsan_atomic<uint64_t> numReclaimedBytes_{0};
};
} // namespace facebook::velox::memory
