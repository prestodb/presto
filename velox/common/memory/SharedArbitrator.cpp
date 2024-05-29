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
#include <mutex>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/RuntimeMetrics.h"
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
        return lhs.reservedBytes > rhs.reservedBytes;
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

void SharedArbitrator::getCandidateStats(
    ArbitrationOperation* op,
    bool freeCapacityOnly) {
  op->candidates.clear();
  op->candidates.reserve(op->candidatePools.size());
  for (const auto& pool : op->candidatePools) {
    const bool selfCandidate = op->requestRoot == pool.get();
    op->candidates.push_back(
        {freeCapacityOnly ? 0 : reclaimableUsedCapacity(*pool, selfCandidate),
         reclaimableFreeCapacity(*pool, selfCandidate),
         pool->reservedBytes(),
         pool.get()});
  }
}

void SharedArbitrator::updateArbitrationRequestStats() {
  RECORD_METRIC_VALUE(kMetricArbitratorRequestsCount);
  ++numRequests_;
}

void SharedArbitrator::updateArbitrationFailureStats() {
  RECORD_METRIC_VALUE(kMetricArbitratorFailuresCount);
  ++numFailures_;
}

int64_t SharedArbitrator::maxReclaimableCapacity(
    const MemoryPool& pool,
    bool isSelfReclaim) const {
  // Checks if a query memory pool has finished processing or not. If it has
  // finished, then we don't have to respect the memory pool reserved capacity
  // limit check.
  // NOTE: for query system like Prestissimo, it holds a finished query
  // state in minutes for query stats fetch request from the Presto coordinator.
  if (isSelfReclaim || (pool.reservedBytes() == 0 && pool.peakBytes() != 0)) {
    return pool.capacity();
  }
  return std::max<int64_t>(0, pool.capacity() - memoryPoolReservedCapacity_);
}

int64_t SharedArbitrator::reclaimableFreeCapacity(
    const MemoryPool& pool,
    bool isSelfReclaim) const {
  return std::min<int64_t>(
      pool.freeBytes(), maxReclaimableCapacity(pool, isSelfReclaim));
}

int64_t SharedArbitrator::reclaimableUsedCapacity(
    const MemoryPool& pool,
    bool isSelfReclaim) const {
  const auto maxReclaimableBytes = maxReclaimableCapacity(pool, isSelfReclaim);
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
  std::lock_guard<std::mutex> l(mutex_);
  ++numReserves_;
  const int64_t maxBytesToReserve =
      std::min<int64_t>(maxGrowCapacity(*pool), targetBytes);
  const int64_t minBytesToReserve = minGrowCapacity(*pool);
  uint64_t reservedBytes =
      decrementFreeCapacityLocked(maxBytesToReserve, minBytesToReserve);
  try {
    checkedGrow(pool, reservedBytes, 0);
  } catch (const VeloxRuntimeError&) {
    reservedBytes = 0;
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
  std::lock_guard<std::mutex> l(mutex_);
  ++numReleases_;
  const uint64_t freedBytes = shrinkPool(pool, targetBytes);
  incrementFreeCapacityLocked(freedBytes);
  return freedBytes;
}

uint64_t SharedArbitrator::shrinkCapacity(
    const std::vector<std::shared_ptr<MemoryPool>>& pools,
    uint64_t targetBytes,
    bool allowSpill,
    bool allowAbort) {
  incrementGlobalArbitrationCount();
  ArbitrationOperation op(targetBytes, pools);
  ScopedArbitration scopedArbitration(this, &op);
  if (targetBytes == 0) {
    targetBytes = capacity_;
  } else {
    targetBytes = std::max(memoryPoolTransferCapacity_, targetBytes);
  }
  std::lock_guard<std::shared_mutex> exclusiveLock(arbitrationLock_);
  getCandidateStats(&op);
  uint64_t freedBytes =
      reclaimFreeMemoryFromCandidates(&op, targetBytes, false);
  auto freeGuard = folly::makeGuard([&]() {
    // Returns the freed memory capacity back to the arbitrator.
    if (freedBytes > 0) {
      incrementFreeCapacity(freedBytes);
    }
  });
  if (freedBytes >= targetBytes) {
    return freedBytes;
  }
  RECORD_METRIC_VALUE(kMetricArbitratorSlowGlobalArbitrationCount);
  if (allowSpill) {
    freedBytes +=
        reclaimUsedMemoryFromCandidatesBySpill(&op, targetBytes - freedBytes);
    if (freedBytes >= targetBytes) {
      return freedBytes;
    }
    if (allowAbort) {
      // Candidate stats may change after spilling.
      getCandidateStats(&op);
    }
  }
  if (allowAbort) {
    freedBytes +=
        reclaimUsedMemoryFromCandidatesByAbort(&op, targetBytes - freedBytes);
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
  ArbitrationOperation op(pool, targetBytes, candidatePools);
  ScopedArbitration scopedArbitration(this, &op);

  bool needGlobalArbitration{false};
  if (!runLocalArbitration(&op, needGlobalArbitration)) {
    return false;
  }
  if (!needGlobalArbitration) {
    return true;
  }
  if (!globalArbitrationEnabled_) {
    return false;
  }
  return runGlobalArbitration(&op);
}

bool SharedArbitrator::runLocalArbitration(
    ArbitrationOperation* op,
    bool& needGlobalArbitration) {
  needGlobalArbitration = false;
  const std::chrono::steady_clock::time_point localArbitrationStartTime =
      std::chrono::steady_clock::now();
  std::shared_lock<std::shared_mutex> sharedLock(arbitrationLock_);
  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::runLocalArbitration", this);
  op->localArbitrationLockWaitTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - localArbitrationStartTime)
          .count();

  checkIfAborted(op);

  if (maybeGrowFromSelf(op)) {
    return true;
  }

  if (!ensureCapacity(op)) {
    updateArbitrationFailureStats();
    VELOX_MEM_LOG(ERROR) << "Can't grow " << op->requestRoot->name()
                         << " capacity to "
                         << succinctBytes(
                                op->requestRoot->capacity() + op->targetBytes)
                         << " which exceeds its max capacity "
                         << succinctBytes(op->requestRoot->maxCapacity())
                         << ", current capacity "
                         << succinctBytes(op->requestRoot->capacity())
                         << ", request " << succinctBytes(op->targetBytes);
    return false;
  }
  VELOX_CHECK(!op->requestRoot->aborted());

  if (maybeGrowFromSelf(op)) {
    return true;
  }

  uint64_t maxGrowTarget{0};
  uint64_t minGrowTarget{0};
  getGrowTargets(op, maxGrowTarget, minGrowTarget);

  uint64_t freedBytes = decrementFreeCapacity(maxGrowTarget, minGrowTarget);
  auto freeGuard = folly::makeGuard([&]() {
    // Returns the unused freed memory capacity back to the arbitrator.
    if (freedBytes > 0) {
      incrementFreeCapacity(freedBytes);
    }
  });
  if (freedBytes >= op->targetBytes) {
    checkedGrow(op->requestRoot, freedBytes, op->targetBytes);
    freedBytes = 0;
    return true;
  }
  VELOX_CHECK_LT(freedBytes, maxGrowTarget);

  getCandidateStats(op, true);
  freedBytes +=
      reclaimFreeMemoryFromCandidates(op, maxGrowTarget - freedBytes, true);
  if (freedBytes >= op->targetBytes) {
    const uint64_t bytesToGrow = std::min(maxGrowTarget, freedBytes);
    checkedGrow(op->requestRoot, bytesToGrow, op->targetBytes);
    freedBytes -= bytesToGrow;
    return true;
  }
  VELOX_CHECK_LT(freedBytes, maxGrowTarget);

  if (!globalArbitrationEnabled_) {
    freedBytes += reclaim(op->requestRoot, maxGrowTarget - freedBytes, true);
  }
  checkIfAborted(op);

  if (freedBytes >= op->targetBytes) {
    const uint64_t bytesToGrow = std::min(maxGrowTarget, freedBytes);
    checkedGrow(op->requestRoot, bytesToGrow, op->targetBytes);
    freedBytes -= bytesToGrow;
    return true;
  }

  needGlobalArbitration = true;
  return true;
}

bool SharedArbitrator::runGlobalArbitration(ArbitrationOperation* op) {
  incrementGlobalArbitrationCount();
  const std::chrono::steady_clock::time_point globalArbitrationStartTime =
      std::chrono::steady_clock::now();
  std::lock_guard<std::shared_mutex> exclusiveLock(arbitrationLock_);
  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::runGlobalArbitration", this);
  op->globalArbitrationLockWaitTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - globalArbitrationStartTime)
          .count();
  checkIfAborted(op);

  if (maybeGrowFromSelf(op)) {
    return true;
  }

  int32_t attempts = 0;
  for (;; ++attempts) {
    if (arbitrateMemory(op)) {
      return true;
    }
    if (attempts > 0) {
      break;
    }
    VELOX_CHECK(!op->requestRoot->aborted());
    if (!handleOOM(op)) {
      break;
    }
  }
  VELOX_MEM_LOG(ERROR)
      << "Failed to arbitrate sufficient memory for memory pool "
      << op->requestRoot->name() << ", request "
      << succinctBytes(op->targetBytes) << " after " << attempts
      << " attempts, Arbitrator state: " << toString();
  updateArbitrationFailureStats();
  return false;
}

void SharedArbitrator::getGrowTargets(
    ArbitrationOperation* op,
    uint64_t& maxGrowTarget,
    uint64_t& minGrowTarget) {
  maxGrowTarget = std::min(
      maxGrowCapacity(*op->requestRoot),
      std::max(memoryPoolTransferCapacity_, op->targetBytes));
  minGrowTarget = minGrowCapacity(*op->requestRoot);
}

void SharedArbitrator::checkIfAborted(ArbitrationOperation* op) {
  if (op->requestRoot->aborted()) {
    updateArbitrationFailureStats();
    VELOX_MEM_POOL_ABORTED("The requestor pool has been aborted");
  }
}

bool SharedArbitrator::maybeGrowFromSelf(ArbitrationOperation* op) {
  if (op->requestRoot->freeBytes() >= op->targetBytes) {
    if (growPool(op->requestRoot, 0, op->targetBytes)) {
      return true;
    }
  }
  return false;
}

bool SharedArbitrator::checkCapacityGrowth(ArbitrationOperation* op) const {
  return (maxGrowCapacity(*op->requestRoot) >= op->targetBytes) &&
      (capacityAfterGrowth(*op->requestRoot, op->targetBytes) <= capacity_);
}

bool SharedArbitrator::ensureCapacity(ArbitrationOperation* op) {
  if ((op->targetBytes > capacity_) ||
      (op->targetBytes > op->requestRoot->maxCapacity())) {
    return false;
  }
  if (checkCapacityGrowth(op)) {
    return true;
  }

  const uint64_t reclaimedBytes =
      reclaim(op->requestRoot, op->targetBytes, true);
  // NOTE: return the reclaimed bytes back to the arbitrator and let the memory
  // arbitration process to grow the requestor's memory capacity accordingly.
  incrementFreeCapacity(reclaimedBytes);
  // Check if the requestor has been aborted in reclaim operation above.
  if (op->requestRoot->aborted()) {
    updateArbitrationFailureStats();
    VELOX_MEM_POOL_ABORTED("The requestor pool has been aborted");
  }
  return checkCapacityGrowth(op);
}

bool SharedArbitrator::handleOOM(ArbitrationOperation* op) {
  MemoryPool* victim = findCandidateWithLargestCapacity(
                           op->requestRoot, op->targetBytes, op->candidates)
                           .pool;
  if (op->requestRoot == victim) {
    VELOX_MEM_LOG(ERROR)
        << "Requestor memory pool " << op->requestRoot->name()
        << " is selected as victim memory pool so fail the memory arbitration";
    return false;
  }
  VELOX_MEM_LOG(WARNING) << "Aborting victim memory pool " << victim->name()
                         << " to free up memory for requestor "
                         << op->requestRoot->name();
  try {
    if (victim == op->requestRoot) {
      VELOX_MEM_POOL_CAP_EXCEEDED(
          memoryPoolAbortMessage(victim, op->requestRoot, op->targetBytes));
    } else {
      VELOX_MEM_POOL_ABORTED(
          memoryPoolAbortMessage(victim, op->requestRoot, op->targetBytes));
    }
  } catch (VeloxRuntimeError&) {
    abort(victim, std::current_exception());
  }
  // Free up all the unused capacity from the aborted memory pool and gives back
  // to the arbitrator.
  incrementFreeCapacity(shrinkPool(victim, 0));
  return true;
}

void SharedArbitrator::checkedGrow(
    MemoryPool* pool,
    uint64_t growBytes,
    uint64_t reservationBytes) {
  const auto ret = growPool(pool, growBytes, reservationBytes);
  VELOX_CHECK(
      ret,
      "Failed to grow pool {} with {} and commit {} used reservation",
      pool->name(),
      succinctBytes(growBytes),
      succinctBytes(reservationBytes));
}

bool SharedArbitrator::arbitrateMemory(ArbitrationOperation* op) {
  VELOX_CHECK(!op->requestRoot->aborted());
  uint64_t maxGrowTarget{0};
  uint64_t minGrowTarget{0};
  getGrowTargets(op, maxGrowTarget, minGrowTarget);

  uint64_t freedBytes = decrementFreeCapacity(maxGrowTarget, minGrowTarget);
  auto freeGuard = folly::makeGuard([&]() {
    // Returns the unused freed memory capacity back to the arbitrator.
    if (freedBytes > 0) {
      incrementFreeCapacity(freedBytes);
    }
  });
  if (freedBytes >= op->targetBytes) {
    checkedGrow(op->requestRoot, freedBytes, op->targetBytes);
    freedBytes = 0;
    return true;
  }
  VELOX_CHECK_LT(freedBytes, maxGrowTarget);

  // Get refreshed stats before the global memory arbitration run.
  getCandidateStats(op);

  freedBytes +=
      reclaimFreeMemoryFromCandidates(op, maxGrowTarget - freedBytes, false);
  if (freedBytes >= op->targetBytes) {
    const uint64_t bytesToGrow = std::min(maxGrowTarget, freedBytes);
    checkedGrow(op->requestRoot, bytesToGrow, op->targetBytes);
    freedBytes -= bytesToGrow;
    return true;
  }
  VELOX_CHECK_LT(freedBytes, maxGrowTarget);

  RECORD_METRIC_VALUE(kMetricArbitratorSlowGlobalArbitrationCount);
  freedBytes +=
      reclaimUsedMemoryFromCandidatesBySpill(op, maxGrowTarget - freedBytes);
  checkIfAborted(op);

  if (freedBytes < op->targetBytes) {
    VELOX_MEM_LOG(WARNING)
        << "Failed to arbitrate sufficient memory for memory pool "
        << op->requestRoot->name() << ", request "
        << succinctBytes(op->targetBytes) << ", only "
        << succinctBytes(freedBytes)
        << " has been freed, Arbitrator state: " << toString();
    return false;
  }

  const uint64_t bytesToGrow = std::min(freedBytes, maxGrowTarget);
  checkedGrow(op->requestRoot, bytesToGrow, op->targetBytes);
  freedBytes -= bytesToGrow;
  return true;
}

uint64_t SharedArbitrator::reclaimFreeMemoryFromCandidates(
    ArbitrationOperation* op,
    uint64_t reclaimTargetBytes,
    bool isLocalArbitration) {
  // Sort candidate memory pools based on their reclaimable free capacity.
  sortCandidatesByReclaimableFreeCapacity(op->candidates);

  std::lock_guard<std::mutex> l(mutex_);
  uint64_t reclaimedBytes{0};
  for (const auto& candidate : op->candidates) {
    VELOX_CHECK_LT(reclaimedBytes, reclaimTargetBytes);
    if (candidate.freeBytes == 0) {
      break;
    }
    if (isLocalArbitration && (candidate.pool != op->requestRoot) &&
        isUnderArbitrationLocked(candidate.pool)) {
      // If the reclamation is for local arbitration and the candidate pool is
      // also under arbitration processing, then we can't reclaim from the
      // candidate pool as it might cause concurrent changes to the candidate
      // pool's capacity.
      continue;
    }
    const int64_t bytesToReclaim = std::min<int64_t>(
        reclaimTargetBytes - reclaimedBytes,
        reclaimableFreeCapacity(
            *candidate.pool, candidate.pool == op->requestRoot));
    if (bytesToReclaim <= 0) {
      continue;
    }
    reclaimedBytes += shrinkPool(candidate.pool, bytesToReclaim);
    if (reclaimedBytes >= reclaimTargetBytes) {
      break;
    }
  }
  reclaimedFreeBytes_ += reclaimedBytes;
  return reclaimedBytes;
}

uint64_t SharedArbitrator::reclaimUsedMemoryFromCandidatesBySpill(
    ArbitrationOperation* op,
    uint64_t reclaimTargetBytes) {
  // Sort candidate memory pools based on their reclaimable used capacity.
  sortCandidatesByReclaimableUsedCapacity(op->candidates);

  uint64_t reclaimedBytes{0};
  for (const auto& candidate : op->candidates) {
    VELOX_CHECK_LT(reclaimedBytes, reclaimTargetBytes);
    if (candidate.reclaimableBytes == 0) {
      break;
    }
    reclaimedBytes +=
        reclaim(candidate.pool, reclaimTargetBytes - reclaimedBytes, false);
    if ((reclaimedBytes >= reclaimTargetBytes) ||
        (op->requestRoot != nullptr && op->requestRoot->aborted())) {
      break;
    }
  }
  return reclaimedBytes;
}

uint64_t SharedArbitrator::reclaimUsedMemoryFromCandidatesByAbort(
    ArbitrationOperation* op,
    uint64_t reclaimTargetBytes) {
  sortCandidatesByUsage(op->candidates);

  uint64_t freedBytes{0};
  for (const auto& candidate : op->candidates) {
    VELOX_CHECK_LT(freedBytes, reclaimTargetBytes);
    if (candidate.pool->capacity() == 0) {
      break;
    }
    try {
      VELOX_MEM_POOL_ABORTED(fmt::format(
          "Memory pool aborted to reclaim used memory, current usage {}, "
          "memory pool details:\n{}\n{}",
          succinctBytes(candidate.reservedBytes),
          candidate.pool->toString(),
          candidate.pool->treeMemoryUsage()));
    } catch (VeloxRuntimeError&) {
      abort(candidate.pool, std::current_exception());
    }
    freedBytes += shrinkPool(candidate.pool, 0);
    if (freedBytes >= reclaimTargetBytes) {
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
      maxReclaimableCapacity(*pool, true));
  if (bytesToReclaim == 0) {
    return 0;
  }
  uint64_t reclaimDurationUs{0};
  uint64_t reclaimedUsedBytes{0};
  uint64_t reclaimedFreeBytes{0};
  MemoryReclaimer::Stats reclaimerStats;
  {
    MicrosecondTimer reclaimTimer(&reclaimDurationUs);
    try {
      reclaimedFreeBytes = shrinkPool(pool, bytesToReclaim);
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
      reclaimedUsedBytes = shrinkPool(pool, 0);
    }
    reclaimedUsedBytes += shrinkPool(pool, bytesToReclaim);
  }
  reclaimedUsedBytes_ += reclaimedUsedBytes;
  reclaimedFreeBytes_ += reclaimedFreeBytes;
  reclaimTimeUs_ += reclaimDurationUs;
  numNonReclaimableAttempts_ += reclaimerStats.numNonReclaimableAttempts;
  VELOX_MEM_LOG(INFO) << "Reclaimed from memory pool " << pool->name()
                      << " with target of " << succinctBytes(targetBytes)
                      << ", actually reclaimed "
                      << succinctBytes(reclaimedFreeBytes)
                      << " free memory and "
                      << succinctBytes(reclaimedUsedBytes)
                      << " used memory, spent "
                      << succinctMicros(reclaimDurationUs)
                      << ", isLocalArbitration: " << isLocalArbitration;
  return reclaimedUsedBytes + reclaimedFreeBytes;
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
  stats.numAborted = numAborted_;
  stats.numFailures = numFailures_;
  stats.queueTimeUs = waitTimeUs_;
  stats.arbitrationTimeUs = arbitrationTimeUs_;
  stats.numShrunkBytes = reclaimedFreeBytes_;
  stats.numReclaimedBytes = reclaimedUsedBytes_;
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
      "ARBITRATOR[{} CAPACITY[{}] PENDING[{}] {}]",
      kind_,
      succinctBytes(capacity_),
      numPending_,
      statsLocked().toString());
}

SharedArbitrator::ScopedArbitration::ScopedArbitration(
    SharedArbitrator* arbitrator,
    ArbitrationOperation* operation)
    : operation_(operation),
      arbitrator_(arbitrator),
      arbitrationCtx_(operation->requestPool),
      startTime_(std::chrono::steady_clock::now()) {
  VELOX_CHECK_NOT_NULL(arbitrator_);
  VELOX_CHECK_NOT_NULL(operation_);
  operation_->enterArbitration();
  if (arbitrator_->arbitrationStateCheckCb_ != nullptr &&
      operation_->requestPool != nullptr) {
    arbitrator_->arbitrationStateCheckCb_(*operation_->requestPool);
  }
  arbitrator_->startArbitration(operation_);
}

SharedArbitrator::ScopedArbitration::~ScopedArbitration() {
  operation_->leaveArbitration();
  arbitrator_->finishArbitration(operation_);

  // Report arbitration operation stats.
  const auto arbitrationTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - operation_->startTime)
          .count();
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricArbitratorArbitrationTimeMs, arbitrationTimeUs / 1'000);
  addThreadLocalRuntimeStat(
      kMemoryArbitrationWallNanos,
      RuntimeCounter(arbitrationTimeUs * 1'000, RuntimeCounter::Unit::kNanos));
  if (operation_->localArbitrationQueueTimeUs != 0) {
    addThreadLocalRuntimeStat(
        kLocalArbitrationQueueWallNanos,
        RuntimeCounter(
            operation_->localArbitrationQueueTimeUs * 1'000,
            RuntimeCounter::Unit::kNanos));
  }
  if (operation_->localArbitrationLockWaitTimeUs != 0) {
    addThreadLocalRuntimeStat(
        kLocalArbitrationLockWaitWallNanos,
        RuntimeCounter(
            operation_->localArbitrationLockWaitTimeUs * 1'000,
            RuntimeCounter::Unit::kNanos));
  }
  if (operation_->globalArbitrationLockWaitTimeUs != 0) {
    addThreadLocalRuntimeStat(
        kGlobalArbitrationLockWaitWallNanos,
        RuntimeCounter(
            operation_->globalArbitrationLockWaitTimeUs * 1'000,
            RuntimeCounter::Unit::kNanos));
  }
  arbitrator_->arbitrationTimeUs_ += arbitrationTimeUs;

  const uint64_t waitTimeUs = operation_->waitTimeUs();
  if (waitTimeUs != 0) {
    RECORD_HISTOGRAM_METRIC_VALUE(
        kMetricArbitratorWaitTimeMs, waitTimeUs / 1'000);
    arbitrator_->waitTimeUs_ += waitTimeUs;
  }
}

void SharedArbitrator::ArbitrationOperation::enterArbitration() {
  if (requestPool != nullptr) {
    requestPool->enterArbitration();
  }
}

void SharedArbitrator::ArbitrationOperation::leaveArbitration() {
  if (requestPool != nullptr) {
    requestPool->leaveArbitration();
  }
}

void SharedArbitrator::startArbitration(ArbitrationOperation* op) {
  updateArbitrationRequestStats();
  ContinueFuture waitPromise{ContinueFuture::makeEmpty()};
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++numPending_;
    if (op->requestPool != nullptr) {
      auto it = arbitrationQueues_.find(op->requestRoot);
      if (it != arbitrationQueues_.end()) {
        it->second->waitPromises.emplace_back(fmt::format(
            "Wait for arbitration {}/{}",
            op->requestPool->name(),
            op->requestRoot->name()));
        waitPromise = it->second->waitPromises.back().getSemiFuture();
      } else {
        arbitrationQueues_.emplace(
            op->requestRoot, std::make_unique<ArbitrationQueue>(op));
      }
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
    op->localArbitrationQueueTimeUs += waitTimeUs;
  }
}

void SharedArbitrator::finishArbitration(ArbitrationOperation* op) {
  ContinuePromise resumePromise{ContinuePromise::makeEmpty()};
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_GT(numPending_, 0);
    --numPending_;
    if (op->requestPool != nullptr) {
      auto it = arbitrationQueues_.find(op->requestRoot);
      VELOX_CHECK(
          it != arbitrationQueues_.end(),
          "{}/{} not found",
          op->requestPool->name(),
          op->requestRoot->name());
      auto* runningArbitration = it->second.get();
      if (runningArbitration->waitPromises.empty()) {
        arbitrationQueues_.erase(it);
      } else {
        resumePromise = std::move(runningArbitration->waitPromises.back());
        runningArbitration->waitPromises.pop_back();
      }
    }
  }
  if (resumePromise.valid()) {
    resumePromise.setValue();
  }
}

bool SharedArbitrator::isUnderArbitration(MemoryPool* pool) const {
  std::lock_guard<std::mutex> l(mutex_);
  return isUnderArbitrationLocked(pool);
}

bool SharedArbitrator::isUnderArbitrationLocked(MemoryPool* pool) const {
  return arbitrationQueues_.count(pool) != 0;
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
