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

#include "velox/common/base/Counters.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/Timer.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::memory {

using namespace facebook::velox::memory;

namespace {

// Returns the max capacity to grow of memory 'pool'. The calculation is based
// on a memory pool's max capacity and its current capacity.
uint64_t maxGrowCapacity(const MemoryPool& pool) {
  return pool.maxCapacity() - pool.capacity();
}

// Returns the capacity of the memory pool with the specified growth target.
uint64_t capacityAfterGrowth(const MemoryPool& pool, uint64_t targetBytes) {
  return pool.capacity() + targetBytes;
}

std::string memoryPoolAbortMessage(
    MemoryPool* victim,
    MemoryPool* requestor,
    size_t growBytes) {
  std::stringstream out;
  VELOX_CHECK(victim->isRoot());
  VELOX_CHECK(requestor->isRoot());
  if (requestor == victim) {
    out << "\nFailed memory pool '" << victim->name()
        << "' aborted by itself when tried to grow " << succinctBytes(growBytes)
        << "\n";
  } else {
    out << "\nFailed memory pool '" << victim->name()
        << "' aborted when requestor '" << requestor->name()
        << "' tried to grow " << succinctBytes(growBytes) << "\n";
  }
  out << "Memory usage of the failed memory pool:\n"
      << victim->treeMemoryUsage();
  return out.str();
}

void sortCandidatesByReclaimableFreeCapacity(
    std::vector<SharedArbitrator::Candidate>& candidates) {
  std::sort(
      candidates.begin(),
      candidates.end(),
      [&](const SharedArbitrator::Candidate& lhs,
          const SharedArbitrator::Candidate& rhs) {
        return lhs.freeBytes > rhs.freeBytes;
      });

  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::sortCandidatesByReclaimableFreeCapacity",
      &candidates);
}

void sortCandidatesByReclaimableUsedCapacity(
    std::vector<SharedArbitrator::Candidate>& candidates) {
  std::sort(
      candidates.begin(),
      candidates.end(),
      [](const SharedArbitrator::Candidate& lhs,
         const SharedArbitrator::Candidate& rhs) {
        return lhs.reclaimableBytes > rhs.reclaimableBytes;
      });

  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::sortCandidatesByReclaimableUsedCapacity",
      &candidates);
}

void sortCandidatesByUsage(
    std::vector<SharedArbitrator::Candidate>& candidates) {
  std::sort(
      candidates.begin(),
      candidates.end(),
      [](const SharedArbitrator::Candidate& lhs,
         const SharedArbitrator::Candidate& rhs) {
        return lhs.currentBytes > rhs.currentBytes;
      });
}

// Finds the candidate with the largest capacity. For 'requestor', the
// capacity for comparison including its current capacity and the capacity to
// grow.
const SharedArbitrator::Candidate& findCandidateWithLargestCapacity(
    MemoryPool* requestor,
    uint64_t targetBytes,
    const std::vector<SharedArbitrator::Candidate>& candidates) {
  VELOX_CHECK(!candidates.empty());
  int32_t candidateIdx{-1};
  int64_t maxCapacity{-1};
  for (int32_t i = 0; i < candidates.size(); ++i) {
    const bool isCandidate = candidates[i].pool == requestor;
    // For capacity comparison, the requestor's capacity should include both its
    // current capacity and the capacity growth.
    const int64_t capacity =
        candidates[i].pool->capacity() + (isCandidate ? targetBytes : 0);
    if (i == 0) {
      candidateIdx = 0;
      maxCapacity = capacity;
      continue;
    }
    if (capacity < maxCapacity) {
      continue;
    }
    if (capacity > maxCapacity) {
      candidateIdx = i;
      maxCapacity = capacity;
      continue;
    }
    // With the same amount of capacity, we prefer to kill the requestor itself
    // without affecting the other query.
    if (isCandidate) {
      candidateIdx = i;
    }
  }
  VELOX_CHECK_NE(candidateIdx, -1);
  return candidates[candidateIdx];
}
} // namespace

SharedArbitrator::SharedArbitrator(const MemoryArbitrator::Config& config)
    : MemoryArbitrator(config),
      freeReservedCapacity_(reservedCapacity_),
      freeNonReservedCapacity_(capacity_ - freeReservedCapacity_) {
  VELOX_CHECK_EQ(kind_, config.kind);
  updateFreeCapacityMetrics();
}

std::string SharedArbitrator::Candidate::toString() const {
  return fmt::format(
      "CANDIDATE[{}] RECLAIMABLE_BYTES[{}] FREE_BYTES[{}]]",
      pool->root()->name(),
      succinctBytes(reclaimableBytes),
      succinctBytes(freeBytes));
}

SharedArbitrator::~SharedArbitrator() {
  if (freeNonReservedCapacity_ + freeReservedCapacity_ != capacity_) {
    const std::string errMsg = fmt::format(
        "Unexpected free capacity leak in arbitrator: freeNonReservedCapacity_[{}] + freeReservedCapacity_[{}] != capacity_[{}])\\n{}",
        freeNonReservedCapacity_,
        freeReservedCapacity_,
        capacity_,
        toString());
    if (checkUsageLeak_) {
      VELOX_FAIL(errMsg);
    } else {
      VELOX_MEM_LOG(ERROR) << errMsg;
    }
  }
}

std::vector<SharedArbitrator::Candidate> SharedArbitrator::getCandidateStats(
    const std::vector<std::shared_ptr<MemoryPool>>& pools,
    bool freeCapacityOnly) {
  std::vector<SharedArbitrator::Candidate> candidates;
  candidates.reserve(pools.size());
  for (const auto& pool : pools) {
    candidates.push_back(
        {freeCapacityOnly ? 0 : reclaimableUsedCapacity(*pool),
         reclaimableFreeCapacity(*pool),
         pool->currentBytes(),
         pool.get()});
  }
  return candidates;
}

void SharedArbitrator::updateFreeCapacityMetrics() const {
  RECORD_METRIC_VALUE(
      kMetricArbitratorFreeCapacityBytes,
      freeNonReservedCapacity_ + freeReservedCapacity_);
  RECORD_METRIC_VALUE(
      kMetricArbitratorFreeReservedCapacityBytes, freeReservedCapacity_);
}

int64_t SharedArbitrator::maxReclaimableCapacity(const MemoryPool& pool) const {
  return std::max<int64_t>(0, pool.capacity() - memoryPoolReservedCapacity_);
}

int64_t SharedArbitrator::reclaimableFreeCapacity(
    const MemoryPool& pool) const {
  return std::min<int64_t>(pool.freeBytes(), maxReclaimableCapacity(pool));
}

int64_t SharedArbitrator::reclaimableUsedCapacity(
    const MemoryPool& pool) const {
  const auto maxReclaimableBytes = maxReclaimableCapacity(pool);
  const auto reclaimableBytes = pool.reclaimableBytes();
  return std::min<int64_t>(maxReclaimableBytes, reclaimableBytes.value_or(0));
}

int64_t SharedArbitrator::minGrowCapacity(const MemoryPool& pool) const {
  return std::max<int64_t>(
      0,
      std::min<int64_t>(pool.maxCapacity(), memoryPoolReservedCapacity_) -
          pool.capacity());
}

uint64_t SharedArbitrator::growCapacity(
    MemoryPool* pool,
    uint64_t targetBytes) {
  const auto freeCapacityMetricUpdateCb =
      folly::makeGuard([this]() { updateFreeCapacityMetrics(); });
  uint64_t reservedBytes{0};
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++numReserves_;
    const int64_t maxBytesToReserve =
        std::min<int64_t>(maxGrowCapacity(*pool), targetBytes);
    const int64_t minBytesToReserve = minGrowCapacity(*pool);
    reservedBytes =
        decrementFreeCapacityLocked(maxBytesToReserve, minBytesToReserve);
    try {
      checkedGrow(pool, reservedBytes, 0);
    } catch (const VeloxRuntimeError& error) {
      reservedBytes = 0;
    }
  }
  return reservedBytes;
}

uint64_t SharedArbitrator::decrementFreeCapacity(
    uint64_t maxBytesToReserve,
    uint64_t minBytesToReserve) {
  uint64_t reservedBytes{0};
  {
    std::lock_guard<std::mutex> l(mutex_);
    reservedBytes =
        decrementFreeCapacityLocked(maxBytesToReserve, minBytesToReserve);
  }
  return reservedBytes;
}

uint64_t SharedArbitrator::decrementFreeCapacityLocked(
    uint64_t maxBytesToReserve,
    uint64_t minBytesToReserve) {
  uint64_t allocatedBytes =
      std::min<uint64_t>(freeNonReservedCapacity_, maxBytesToReserve);
  freeNonReservedCapacity_ -= allocatedBytes;
  if (allocatedBytes < minBytesToReserve) {
    const uint64_t reservedBytes = std::min<uint64_t>(
        minBytesToReserve - allocatedBytes, freeReservedCapacity_);
    freeReservedCapacity_ -= reservedBytes;
    allocatedBytes += reservedBytes;
  }
  return allocatedBytes;
}

uint64_t SharedArbitrator::shrinkCapacity(
    MemoryPool* pool,
    uint64_t targetBytes) {
  const auto freeCapacityUpdateCb =
      folly::makeGuard([this]() { updateFreeCapacityMetrics(); });

  uint64_t freedBytes{0};
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++numReleases_;
    freedBytes = pool->shrink(targetBytes);
    incrementFreeCapacityLocked(freedBytes);
  }
  return freedBytes;
}

uint64_t SharedArbitrator::shrinkCapacity(
    const std::vector<std::shared_ptr<MemoryPool>>& pools,
    uint64_t targetBytes,
    bool allowSpill,
    bool allowAbort) {
  const auto freeCapacityUpdateCb =
      folly::makeGuard([this]() { updateFreeCapacityMetrics(); });

  ScopedArbitration scopedArbitration(this);
  if (targetBytes == 0) {
    targetBytes = capacity_;
  } else {
    targetBytes = std::max(memoryPoolTransferCapacity_, targetBytes);
  }
  std::vector<Candidate> candidates = getCandidateStats(pools);
  auto freedBytes = reclaimFreeMemoryFromCandidates(candidates, targetBytes);
  auto freeGuard = folly::makeGuard([&]() {
    // Returns the freed memory capacity back to the arbitrator.
    if (freedBytes > 0) {
      incrementFreeCapacity(freedBytes);
    }
  });
  if (freedBytes >= targetBytes) {
    return freedBytes;
  }
  if (allowSpill) {
    freedBytes += reclaimUsedMemoryFromCandidatesBySpill(
        nullptr, candidates, targetBytes - freedBytes);
    if (freedBytes >= targetBytes) {
      return freedBytes;
    }
    if (allowAbort) {
      // Candidate stats may change after spilling.
      candidates = getCandidateStats(pools);
    }
  }
  if (allowAbort) {
    freedBytes += reclaimUsedMemoryFromCandidatesByAbort(
        candidates, targetBytes - freedBytes);
  }
  return freedBytes;
}

void SharedArbitrator::testingFreeCapacity(uint64_t capacity) {
  std::lock_guard<std::mutex> l(mutex_);
  incrementFreeCapacityLocked(capacity);
}

uint64_t SharedArbitrator::testingNumRequests() const {
  return numRequests_;
}

bool SharedArbitrator::growCapacity(
    MemoryPool* pool,
    const std::vector<std::shared_ptr<MemoryPool>>& candidatePools,
    uint64_t targetBytes) {
  const auto freeCapacityUpdateCb =
      folly::makeGuard([this]() { updateFreeCapacityMetrics(); });

  ScopedArbitration scopedArbitration(pool, this);
  MemoryPool* requestor = pool->root();
  if (requestor->aborted()) {
    RECORD_METRIC_VALUE(kMetricArbitratorFailuresCount);
    ++numFailures_;
    VELOX_MEM_POOL_ABORTED("The requestor pool has been aborted");
  }

  // Checks if the request pool already has enough free capacity for the new
  // request. This could happen if there is multiple concurrent arbitration
  // requests from the same query. When the first served request succeeds, it
  // might have reserved enough memory capacity for the followup requests.
  if (requestor->freeBytes() >= targetBytes) {
    if (requestor->grow(0, targetBytes)) {
      return true;
    }
  }

  if (!ensureCapacity(requestor, targetBytes)) {
    RECORD_METRIC_VALUE(kMetricArbitratorFailuresCount);
    ++numFailures_;
    VELOX_MEM_LOG(ERROR) << "Can't grow " << requestor->name()
                         << " capacity to "
                         << succinctBytes(requestor->capacity() + targetBytes)
                         << " which exceeds its max capacity "
                         << succinctBytes(requestor->maxCapacity())
                         << ", current capacity "
                         << succinctBytes(requestor->capacity()) << ", request "
                         << succinctBytes(targetBytes);
    return false;
  }

  std::vector<Candidate> candidates;
  int numRetries{0};
  for (;; ++numRetries) {
    // Get refreshed stats before the memory arbitration retry.
    candidates = getCandidateStats(candidatePools);
    if (arbitrateMemory(requestor, candidates, targetBytes)) {
      ++numSucceeded_;
      return true;
    }
    if (numRetries > 0) {
      break;
    }
    VELOX_CHECK(!requestor->aborted());
    if (!handleOOM(requestor, targetBytes, candidates)) {
      break;
    }
  }
  VELOX_MEM_LOG(ERROR)
      << "Failed to arbitrate sufficient memory for memory pool "
      << requestor->name() << ", request " << succinctBytes(targetBytes)
      << " after " << numRetries
      << " retries, Arbitrator state: " << toString();
  RECORD_METRIC_VALUE(kMetricArbitratorFailuresCount);
  ++numFailures_;
  return false;
}

bool SharedArbitrator::checkCapacityGrowth(
    const MemoryPool& pool,
    uint64_t targetBytes) const {
  return (maxGrowCapacity(pool) >= targetBytes) &&
      (capacityAfterGrowth(pool, targetBytes) <= capacity_);
}

bool SharedArbitrator::ensureCapacity(
    MemoryPool* requestor,
    uint64_t targetBytes) {
  if ((targetBytes > capacity_) || (targetBytes > requestor->maxCapacity())) {
    return false;
  }
  if (checkCapacityGrowth(*requestor, targetBytes)) {
    return true;
  }
  const uint64_t reclaimedBytes = reclaim(requestor, targetBytes, true);
  // NOTE: return the reclaimed bytes back to the arbitrator and let the memory
  // arbitration process to grow the requestor's memory capacity accordingly.
  incrementFreeCapacity(reclaimedBytes);
  // Check if the requestor has been aborted in reclaim operation above.
  if (requestor->aborted()) {
    RECORD_METRIC_VALUE(kMetricArbitratorFailuresCount);
    ++numFailures_;
    VELOX_MEM_POOL_ABORTED("The requestor pool has been aborted");
  }
  return checkCapacityGrowth(*requestor, targetBytes);
}

bool SharedArbitrator::handleOOM(
    MemoryPool* requestor,
    uint64_t targetBytes,
    std::vector<Candidate>& candidates) {
  MemoryPool* victim =
      findCandidateWithLargestCapacity(requestor, targetBytes, candidates).pool;
  if (requestor == victim) {
    VELOX_MEM_LOG(ERROR)
        << "Requestor memory pool " << requestor->name()
        << " is selected as victim memory pool so fail the memory arbitration";
    return false;
  }
  VELOX_MEM_LOG(WARNING) << "Aborting victim memory pool " << victim->name()
                         << " to free up memory for requestor "
                         << requestor->name();
  try {
    if (victim == requestor) {
      VELOX_MEM_POOL_CAP_EXCEEDED(
          memoryPoolAbortMessage(victim, requestor, targetBytes));
    } else {
      VELOX_MEM_POOL_ABORTED(
          memoryPoolAbortMessage(victim, requestor, targetBytes));
    }
  } catch (VeloxRuntimeError&) {
    abort(victim, std::current_exception());
  }
  // Free up all the unused capacity from the aborted memory pool and gives back
  // to the arbitrator.
  incrementFreeCapacity(victim->shrink());
  return true;
}

void SharedArbitrator::checkedGrow(
    MemoryPool* pool,
    uint64_t growBytes,
    uint64_t reservationBytes) {
  const auto ret = pool->grow(growBytes, reservationBytes);
  VELOX_CHECK(
      ret,
      "Failed to grow pool {} with {} and commit {} used reservation",
      pool->name(),
      succinctBytes(growBytes),
      succinctBytes(reservationBytes));
}

bool SharedArbitrator::arbitrateMemory(
    MemoryPool* requestor,
    std::vector<Candidate>& candidates,
    uint64_t targetBytes) {
  VELOX_CHECK(!requestor->aborted());
  const uint64_t growTarget = std::min(
      maxGrowCapacity(*requestor),
      std::max(memoryPoolTransferCapacity_, targetBytes));
  const uint64_t minGrowTarget = minGrowCapacity(*requestor);
  uint64_t freedBytes = decrementFreeCapacity(growTarget, minGrowTarget);
  auto freeGuard = folly::makeGuard([&]() {
    // Returns the unused freed memory capacity back to the arbitrator.
    if (freedBytes > 0) {
      incrementFreeCapacity(freedBytes);
    }
  });
  if (freedBytes >= targetBytes) {
    checkedGrow(requestor, freedBytes, targetBytes);
    freedBytes = 0;
    return true;
  }
  VELOX_CHECK_LT(freedBytes, growTarget);

  freedBytes +=
      reclaimFreeMemoryFromCandidates(candidates, growTarget - freedBytes);
  if (freedBytes >= targetBytes) {
    const uint64_t bytesToGrow = std::min(growTarget, freedBytes);
    checkedGrow(requestor, bytesToGrow, targetBytes);
    freedBytes -= bytesToGrow;
    return true;
  }

  VELOX_CHECK_LT(freedBytes, growTarget);
  incrementGlobalArbitrationCount();
  freedBytes += reclaimUsedMemoryFromCandidatesBySpill(
      requestor, candidates, growTarget - freedBytes);
  if (requestor->aborted()) {
    RECORD_METRIC_VALUE(kMetricArbitratorFailuresCount);
    ++numFailures_;
    VELOX_MEM_POOL_ABORTED("The requestor pool has been aborted.");
  }
  VELOX_CHECK(!requestor->aborted());

  if (freedBytes < targetBytes) {
    VELOX_MEM_LOG(WARNING)
        << "Failed to arbitrate sufficient memory for memory pool "
        << requestor->name() << ", request " << succinctBytes(targetBytes)
        << ", only " << succinctBytes(freedBytes)
        << " has been freed, Arbitrator state: " << toString();
    return false;
  }

  const uint64_t bytesToGrow = std::min(freedBytes, growTarget);
  checkedGrow(requestor, bytesToGrow, targetBytes);
  freedBytes -= bytesToGrow;
  return true;
}

uint64_t SharedArbitrator::reclaimFreeMemoryFromCandidates(
    std::vector<Candidate>& candidates,
    uint64_t targetBytes) {
  // Sort candidate memory pools based on their reclaimable free capacity.
  sortCandidatesByReclaimableFreeCapacity(candidates);

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
    VELOX_CHECK_GE(candidate.pool->capacity(), memoryPoolReservedCapacity_);
  }
  numShrunkBytes_ += freedBytes;
  return freedBytes;
}

uint64_t SharedArbitrator::reclaimUsedMemoryFromCandidatesBySpill(
    MemoryPool* requestor,
    std::vector<Candidate>& candidates,
    uint64_t targetBytes) {
  // Sort candidate memory pools based on their reclaimable used capacity.
  sortCandidatesByReclaimableUsedCapacity(candidates);

  uint64_t freedBytes{0};
  for (const auto& candidate : candidates) {
    VELOX_CHECK_LT(freedBytes, targetBytes);
    if (candidate.reclaimableBytes == 0) {
      break;
    }
    freedBytes += reclaim(candidate.pool, targetBytes - freedBytes, false);
    if ((freedBytes >= targetBytes) ||
        (requestor != nullptr && requestor->aborted())) {
      break;
    }
  }
  return freedBytes;
}

uint64_t SharedArbitrator::reclaimUsedMemoryFromCandidatesByAbort(
    std::vector<Candidate>& candidates,
    uint64_t targetBytes) {
  sortCandidatesByUsage(candidates);

  uint64_t freedBytes{0};
  for (const auto& candidate : candidates) {
    VELOX_CHECK_LT(freedBytes, targetBytes);
    if (candidate.pool->capacity() == 0) {
      break;
    }
    try {
      VELOX_MEM_POOL_ABORTED(fmt::format(
          "Memory pool aborted to reclaim used memory, current usage {}, "
          "memory pool details:\n{}\n{}",
          succinctBytes(candidate.currentBytes),
          candidate.pool->toString(),
          candidate.pool->treeMemoryUsage()));
    } catch (VeloxRuntimeError&) {
      abort(candidate.pool, std::current_exception());
    }
    freedBytes += candidate.pool->shrink();
    if (freedBytes >= targetBytes) {
      break;
    }
  }
  return freedBytes;
}

uint64_t SharedArbitrator::reclaim(
    MemoryPool* pool,
    uint64_t targetBytes,
    bool isLocalArbitration) noexcept {
  int64_t bytesToReclaim = std::min<uint64_t>(
      std::max(targetBytes, memoryPoolTransferCapacity_),
      maxReclaimableCapacity(*pool));
  if (bytesToReclaim == 0) {
    return 0;
  }
  uint64_t reclaimDurationUs{0};
  uint64_t reclaimedBytes{0};
  uint64_t reclaimedFreeBytes{0};
  MemoryReclaimer::Stats reclaimerStats;
  {
    const uint64_t oldCapacity = pool->capacity();
    MicrosecondTimer reclaimTimer(&reclaimDurationUs);
    try {
      reclaimedFreeBytes = pool->shrink(bytesToReclaim);
      bytesToReclaim -= reclaimedFreeBytes;
      VELOX_CHECK_GE(bytesToReclaim, 0);
      if (bytesToReclaim > 0) {
        if (isLocalArbitration) {
          incrementLocalArbitrationCount();
        }
        pool->reclaim(bytesToReclaim, memoryReclaimWaitMs_, reclaimerStats);
      }
    } catch (const std::exception& e) {
      VELOX_MEM_LOG(ERROR) << "Failed to reclaim from memory pool "
                           << pool->name() << ", aborting it: " << e.what();
      abort(pool, std::current_exception());
      // Free up all the free capacity from the aborted pool as the associated
      // query has failed at this point.
      pool->shrink();
    }
    const uint64_t newCapacity = pool->capacity();
    VELOX_CHECK_GE(oldCapacity, newCapacity);
    reclaimedBytes = oldCapacity - newCapacity;
  }
  VELOX_CHECK_GE(reclaimedBytes, reclaimedFreeBytes);
  numReclaimedBytes_ += reclaimedBytes - reclaimedFreeBytes;
  numShrunkBytes_ += reclaimedFreeBytes;
  reclaimTimeUs_ += reclaimDurationUs;
  numNonReclaimableAttempts_ += reclaimerStats.numNonReclaimableAttempts;
  VELOX_MEM_LOG(INFO) << "Reclaimed from memory pool " << pool->name()
                      << " with target of " << succinctBytes(targetBytes)
                      << ", actually reclaimed "
                      << succinctBytes(reclaimedFreeBytes)
                      << " free memory and "
                      << succinctBytes(reclaimedBytes - reclaimedFreeBytes)
                      << " used memory, spent "
                      << succinctMicros(reclaimDurationUs);
  return reclaimedBytes;
}

void SharedArbitrator::abort(
    MemoryPool* pool,
    const std::exception_ptr& error) {
  RECORD_METRIC_VALUE(kMetricArbitratorAbortedCount);
  ++numAborted_;
  try {
    pool->abort(error);
  } catch (const std::exception& e) {
    VELOX_MEM_LOG(WARNING) << "Failed to abort memory pool " << pool->toString()
                           << ", error: " << e.what();
  }
  // NOTE: no matter memory pool abort throws or not, it should have been marked
  // as aborted to prevent any new memory arbitration triggered from the aborted
  // memory pool.
  VELOX_CHECK(pool->aborted());
}

void SharedArbitrator::incrementFreeCapacity(uint64_t bytes) {
  std::lock_guard<std::mutex> l(mutex_);
  incrementFreeCapacityLocked(bytes);
}

void SharedArbitrator::incrementFreeCapacityLocked(uint64_t bytes) {
  incrementFreeReservedCapacityLocked(bytes);
  freeNonReservedCapacity_ += bytes;
  if (FOLLY_UNLIKELY(
          freeNonReservedCapacity_ + freeReservedCapacity_ > capacity_)) {
    VELOX_FAIL(
        "The free capacity {}/{} is larger than the max capacity {}, {}",
        succinctBytes(freeNonReservedCapacity_),
        succinctBytes(freeReservedCapacity_),
        succinctBytes(capacity_),
        toStringLocked());
  }
}

void SharedArbitrator::incrementFreeReservedCapacityLocked(uint64_t& bytes) {
  VELOX_CHECK_LE(freeReservedCapacity_, reservedCapacity_);
  const uint64_t freedBytes =
      std::min(bytes, reservedCapacity_ - freeReservedCapacity_);
  freeReservedCapacity_ += freedBytes;
  bytes -= freedBytes;
}

MemoryArbitrator::Stats SharedArbitrator::stats() const {
  std::lock_guard<std::mutex> l(mutex_);
  return statsLocked();
}

MemoryArbitrator::Stats SharedArbitrator::statsLocked() const {
  Stats stats;
  stats.numRequests = numRequests_;
  stats.numSucceeded = numSucceeded_;
  stats.numAborted = numAborted_;
  stats.numFailures = numFailures_;
  stats.queueTimeUs = queueTimeUs_;
  stats.arbitrationTimeUs = arbitrationTimeUs_;
  stats.numShrunkBytes = numShrunkBytes_;
  stats.numReclaimedBytes = numReclaimedBytes_;
  stats.maxCapacityBytes = capacity_;
  stats.freeCapacityBytes = freeNonReservedCapacity_ + freeReservedCapacity_;
  stats.freeReservedCapacityBytes = freeReservedCapacity_;
  stats.reclaimTimeUs = reclaimTimeUs_;
  stats.numNonReclaimableAttempts = numNonReclaimableAttempts_;
  stats.numReserves = numReserves_;
  stats.numReleases = numReleases_;
  return stats;
}

std::string SharedArbitrator::toString() const {
  std::lock_guard<std::mutex> l(mutex_);
  return toStringLocked();
}

std::string SharedArbitrator::toStringLocked() const {
  return fmt::format(
      "ARBITRATOR[{} CAPACITY[{}] RUNNING[{}] QUEUING[{}] {}]",
      kind_,
      succinctBytes(capacity_),
      running_ ? "true" : "false",
      waitPromises_.size(),
      statsLocked().toString());
}

SharedArbitrator::ScopedArbitration::ScopedArbitration(
    SharedArbitrator* arbitrator)
    : requestor_(nullptr),
      arbitrator_(arbitrator),
      startTime_(std::chrono::steady_clock::now()),
      arbitrationCtx_(requestor_) {
  VELOX_CHECK_NOT_NULL(arbitrator_);
  arbitrator_->startArbitration("Wait for arbitration");
}

SharedArbitrator::ScopedArbitration::ScopedArbitration(
    MemoryPool* requestor,
    SharedArbitrator* arbitrator)
    : requestor_(requestor),
      arbitrator_(arbitrator),
      startTime_(std::chrono::steady_clock::now()),
      arbitrationCtx_(requestor_) {
  VELOX_CHECK_NOT_NULL(requestor_);
  VELOX_CHECK_NOT_NULL(arbitrator_);
  requestor_->enterArbitration();
  arbitrator_->startArbitration(fmt::format(
      "Wait for arbitration, requestor: {}[{}]",
      requestor_->name(),
      requestor_->root()->name()));
  if (arbitrator_->arbitrationStateCheckCb_ != nullptr) {
    arbitrator_->arbitrationStateCheckCb_(*requestor_);
  }
}

SharedArbitrator::ScopedArbitration::~ScopedArbitration() {
  if (requestor_ != nullptr) {
    requestor_->leaveArbitration();
  }
  const auto arbitrationTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - startTime_)
          .count();
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricArbitratorArbitrationTimeMs, arbitrationTimeUs / 1'000);
  addThreadLocalRuntimeStat(
      kMemoryArbitrationWallNanos,
      RuntimeCounter(arbitrationTimeUs * 1'000, RuntimeCounter::Unit::kNanos));
  arbitrator_->arbitrationTimeUs_ += arbitrationTimeUs;
  arbitrator_->finishArbitration();
}

void SharedArbitrator::startArbitration(const std::string& contextMsg) {
  ContinueFuture waitPromise{ContinueFuture::makeEmpty()};
  {
    std::lock_guard<std::mutex> l(mutex_);
    RECORD_METRIC_VALUE(kMetricArbitratorRequestsCount);
    ++numRequests_;
    if (running_) {
      waitPromises_.emplace_back(contextMsg);
      waitPromise = waitPromises_.back().getSemiFuture();
    } else {
      VELOX_CHECK(waitPromises_.empty());
      running_ = true;
    }
  }

  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::startArbitration", this);

  if (waitPromise.valid()) {
    uint64_t waitTimeUs{0};
    {
      MicrosecondTimer timer(&waitTimeUs);
      waitPromise.wait();
    }
    RECORD_HISTOGRAM_METRIC_VALUE(
        kMetricArbitratorQueueTimeMs, waitTimeUs / 1'000);
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

std::string SharedArbitrator::kind() const {
  return kind_;
}

void SharedArbitrator::registerFactory() {
  MemoryArbitrator::registerFactory(
      kind_, [](const MemoryArbitrator::Config& config) {
        return std::make_unique<SharedArbitrator>(config);
      });
}

void SharedArbitrator::unregisterFactory() {
  MemoryArbitrator::unregisterFactory(kind_);
}

void SharedArbitrator::incrementGlobalArbitrationCount() {
  RECORD_METRIC_VALUE(kMetricArbitratorGlobalArbitrationCount);
  addThreadLocalRuntimeStat(
      kGlobalArbitrationCount, RuntimeCounter(1, RuntimeCounter::Unit::kNone));
}

void SharedArbitrator::incrementLocalArbitrationCount() {
  RECORD_METRIC_VALUE(kMetricArbitratorLocalArbitrationCount);
  addThreadLocalRuntimeStat(
      kLocalArbitrationCount, RuntimeCounter(1, RuntimeCounter::Unit::kNone));
}
} // namespace facebook::velox::memory
