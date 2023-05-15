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

#include "velox/common/memory/SharedArbitrator.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/Timer.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::memory {
void SharedArbitrator::sortCandidatesByFreeCapacity(
    std::vector<Candidate>& candidates) {
  std::sort(
      candidates.begin(),
      candidates.end(),
      [&](const Candidate& lhs, const Candidate& rhs) {
        return lhs.freeBytes > rhs.freeBytes;
      });

  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::sortCandidatesByFreeCapacity",
      &candidates);
}

void SharedArbitrator::sortCandidatesByReclaimableMemory(
    std::vector<Candidate>& candidates) {
  std::sort(
      candidates.begin(),
      candidates.end(),
      [](const Candidate& lhs, const Candidate& rhs) {
        if (!lhs.reclaimable) {
          return false;
        }
        if (!rhs.reclaimable) {
          return true;
        }
        return lhs.reclaimableBytes > rhs.reclaimableBytes;
      });

  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::sortCandidatesByReclaimableMemory",
      &candidates);
}

SharedArbitrator::~SharedArbitrator() {
  VELOX_CHECK_EQ(freeCapacity_, capacity_, "{}", toString());
}

void SharedArbitrator::reserveMemory(MemoryPool* pool, uint64_t /*unused*/) {
  std::lock_guard<std::mutex> l(mutex_);
  const uint64_t reserveBytes =
      decrementFreeCapacityLocked(initMemoryPoolCapacity_);
  pool->grow(reserveBytes);
}

void SharedArbitrator::releaseMemory(MemoryPool* pool) {
  std::lock_guard<std::mutex> l(mutex_);
  const uint64_t freedBytes = pool->shrink(0);
  incrementFreeCapacityLocked(freedBytes);
}

std::vector<SharedArbitrator::Candidate> SharedArbitrator::getCandidateStats(
    const std::vector<std::shared_ptr<MemoryPool>>& pools) {
  std::vector<SharedArbitrator::Candidate> candidates;
  candidates.reserve(pools.size());
  for (const auto& pool : pools) {
    uint64_t reclaimableBytes;
    const bool reclaimable = pool->reclaimableBytes(reclaimableBytes);
    candidates.push_back(
        {reclaimable, reclaimableBytes, pool->freeBytes(), pool.get()});
  }
  return candidates;
}

bool SharedArbitrator::growMemory(
    MemoryPool* pool,
    const std::vector<std::shared_ptr<MemoryPool>>& candidatePools,
    uint64_t targetBytes) {
  ScopedArbitration scopedArbitration(pool, this);
  std::vector<Candidate> candidates = getCandidateStats(candidatePools);
  const bool success = arbitrateMemory(pool->root(), candidates, targetBytes);
  if (!success) {
    ++numFailures_;
  }
  return success;
}

bool SharedArbitrator::arbitrateMemory(
    MemoryPool* requestor,
    std::vector<Candidate>& candidates,
    uint64_t targetBytes) {
  const int64_t growTarget =
      std::max(minMemoryPoolCapacityTransferSize_, targetBytes);
  uint64_t freedBytes = decrementFreeCapacity(growTarget);
  if (freedBytes >= targetBytes) {
    requestor->grow(freedBytes);
    return true;
  }
  VELOX_CHECK_LT(freedBytes, growTarget);

  freedBytes +=
      reclaimFreeMemoryFromCandidates(candidates, growTarget - freedBytes);
  if (freedBytes >= targetBytes) {
    requestor->grow(freedBytes);
    return true;
  }

  VELOX_CHECK_LT(freedBytes, growTarget);
  freedBytes +=
      reclaimUsedMemoryFromCandidates(candidates, growTarget - freedBytes);
  if (freedBytes >= targetBytes) {
    requestor->grow(freedBytes);
    return true;
  }

  VELOX_MEM_LOG(WARNING)
      << "Failed to arbitrate sufficient memory for memory pool "
      << requestor->name() << ", request " << succinctBytes(targetBytes)
      << ", only " << succinctBytes(freedBytes)
      << " has been freed, Arbitrator state: " << toStringLocked();

  // Returns the unused freed memory capacity back to the arbitrator.
  incrementFreeCapacity(freedBytes);
  return false;
}

uint64_t SharedArbitrator::reclaimFreeMemoryFromCandidates(
    std::vector<Candidate>& candidates,
    uint64_t targetBytes) {
  // Sort candidate memory pools based on their free capacity.
  sortCandidatesByFreeCapacity(candidates);

  uint64_t freedBytes{0};
  for (const auto& candidate : candidates) {
    VELOX_CHECK_LT(freedBytes, targetBytes);
    if (candidate.freeBytes == 0) {
      break;
    }
    const int64_t bytesToShrink =
        std::min<int64_t>(targetBytes - freedBytes, candidate.freeBytes);
    if (bytesToShrink <= 0) {
      break;
    }
    freedBytes += candidate.pool->shrink(bytesToShrink);
    if (freedBytes >= targetBytes) {
      break;
    }
  }
  numShrunkBytes_ += freedBytes;
  return freedBytes;
}

uint64_t SharedArbitrator::reclaimUsedMemoryFromCandidates(
    std::vector<Candidate>& candidates,
    uint64_t targetBytes) {
  // Sort candidate memory pools based on their reclaimable memory.
  sortCandidatesByReclaimableMemory(candidates);

  int64_t freedBytes{0};
  for (const auto& candidate : candidates) {
    VELOX_CHECK_LT(freedBytes, targetBytes);
    if (!candidate.reclaimable || candidate.reclaimableBytes == 0) {
      break;
    }
    const int64_t bytesToReclaim = std::max<int64_t>(
        targetBytes - freedBytes, minMemoryPoolCapacityTransferSize_);
    VELOX_CHECK_GT(bytesToReclaim, 0);
    // TODO: add to handle the exception raised from memory pool reclaim.
    freedBytes += candidate.pool->reclaim(bytesToReclaim);
    if (freedBytes >= targetBytes) {
      break;
    }
  }
  numReclaimedBytes_ += freedBytes;
  return freedBytes;
}

uint64_t SharedArbitrator::decrementFreeCapacity(uint64_t bytes) {
  std::lock_guard<std::mutex> l(mutex_);
  return decrementFreeCapacityLocked(bytes);
}

uint64_t SharedArbitrator::decrementFreeCapacityLocked(uint64_t bytes) {
  const uint64_t targetBytes = std::min(freeCapacity_, bytes);
  VELOX_CHECK_LE(targetBytes, freeCapacity_);
  freeCapacity_ -= targetBytes;
  return targetBytes;
}

void SharedArbitrator::incrementFreeCapacity(uint64_t bytes) {
  std::lock_guard<std::mutex> l(mutex_);
  incrementFreeCapacityLocked(bytes);
}

void SharedArbitrator::incrementFreeCapacityLocked(uint64_t bytes) {
  freeCapacity_ += bytes;
  if (FOLLY_UNLIKELY(freeCapacity_ > capacity_)) {
    VELOX_FAIL(
        "The free capacity {} is larger than the max capacity {}, {}",
        succinctBytes(freeCapacity_),
        succinctBytes(capacity_),
        toStringLocked());
  }
}

MemoryArbitrator::Stats SharedArbitrator::stats() const {
  std::lock_guard<std::mutex> l(mutex_);
  return statsLocked();
}

MemoryArbitrator::Stats SharedArbitrator::statsLocked() const {
  Stats stats;
  stats.numRequests = numRequests_;
  stats.numFailures = numFailures_;
  stats.queueTimeUs = queueTimeUs_;
  stats.arbitrationTimeUs = arbitrationTimeUs_;
  stats.numShrunkBytes = numShrunkBytes_;
  stats.numReclaimedBytes = numReclaimedBytes_;
  stats.maxCapacityBytes = capacity_;
  stats.freeCapacityBytes = freeCapacity_;
  return stats;
}

std::string SharedArbitrator::toString() const {
  std::lock_guard<std::mutex> l(mutex_);
  return toStringLocked();
}

std::string SharedArbitrator::toStringLocked() const {
  return fmt::format(
      "ARBITRATOR[{}] CAPACITY {} {}",
      kindString(kind_),
      succinctBytes(capacity_),
      statsLocked().toString());
}

SharedArbitrator::ScopedArbitration::ScopedArbitration(
    MemoryPool* requestor,
    SharedArbitrator* arbitrator)
    : requestor_(requestor),
      arbitrator_(arbitrator),
      startTime_(std::chrono::steady_clock::now()) {
  VELOX_CHECK_NOT_NULL(requestor_);
  VELOX_CHECK_NOT_NULL(arbitrator_);
  requestor_->enterArbitration();
  arbitrator_->startArbitration(requestor);
  started_ = true;
}

SharedArbitrator::ScopedArbitration::~ScopedArbitration() {
  if (!started_) {
    return;
  }
  requestor_->leaveArbitration();
  const auto arbitrationTime =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - startTime_);
  arbitrator_->arbitrationTimeUs_ += arbitrationTime.count();
  arbitrator_->finishArbitration();
}

void SharedArbitrator::startArbitration(MemoryPool* requestor) {
  ContinueFuture waitPromise{ContinueFuture::makeEmpty()};
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++numRequests_;
    if (running_) {
      waitPromises_.emplace_back(fmt::format(
          "Wait for arbitration, requestor: {}[{}]",
          requestor->name(),
          requestor->root()->name()));
      waitPromise = waitPromises_.back().getSemiFuture();
    } else {
      VELOX_CHECK(waitPromises_.empty());
      running_ = true;
    }
  }

  if (waitPromise.valid()) {
    uint64_t waitTimeUs{0};
    {
      MicrosecondTimer timer(&waitTimeUs);
      waitPromise.wait();
    }
    queueTimeUs_ += waitTimeUs;
  }
}

void SharedArbitrator::finishArbitration() {
  ContinuePromise resumePromise{ContinuePromise::makeEmpty()};
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(running_);
    if (!waitPromises_.empty()) {
      resumePromise = std::move(waitPromises_.back());
      waitPromises_.pop_back();
    } else {
      running_ = false;
    }
  }
  if (resumePromise.valid()) {
    resumePromise.setValue();
  }
}
} // namespace facebook::velox::memory
