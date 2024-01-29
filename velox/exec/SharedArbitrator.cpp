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

#include "velox/exec/SharedArbitrator.h"

#include "velox/common/base/Counters.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/Timer.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {

namespace {

// Returns the max capacity to grow of memory 'pool'. The calculation is based
// on a memory pool's max capacity and its current capacity.
uint64_t maxGrowBytes(const MemoryPool& pool) {
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
} // namespace

SharedArbitrator::SharedArbitrator(const MemoryArbitrator::Config& config)
    : MemoryArbitrator(config), freeCapacity_(capacity_) {
  RECORD_METRIC_VALUE(kMetricArbitratorFreeCapacityBytes, freeCapacity_);
  VELOX_CHECK_EQ(kind_, config.kind);
}

std::string SharedArbitrator::Candidate::toString() const {
  return fmt::format(
      "CANDIDATE[{} RECLAIMABLE[{}] RECLAIMABLE_BYTES[{}] FREE_BYTES[{}]]",
      pool->root()->name(),
      reclaimable,
      succinctBytes(reclaimableBytes),
      succinctBytes(freeBytes));
}

void SharedArbitrator::sortCandidatesByFreeCapacity(
    std::vector<Candidate>& candidates) const {
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
    std::vector<Candidate>& candidates) const {
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

const SharedArbitrator::Candidate&
SharedArbitrator::findCandidateWithLargestCapacity(
    MemoryPool* requestor,
    uint64_t targetBytes,
    const std::vector<Candidate>& candidates) const {
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

SharedArbitrator::~SharedArbitrator() {
  if (freeCapacity_ != capacity_) {
    const std::string errMsg = fmt::format(
        "\"There is unexpected free capacity not given back to arbitrator "
        "on destruction: freeCapacity_ != capacity_ ({} vs {})\\n{}\"",
        freeCapacity_,
        capacity_,
        toString());
    if (checkUsageLeak_) {
      VELOX_FAIL(errMsg);
    } else {
      LOG(ERROR) << errMsg;
    }
  }
}

uint64_t SharedArbitrator::growCapacity(
    MemoryPool* pool,
    uint64_t targetBytes) {
  const int64_t bytesToReserve =
      std::min<int64_t>(maxGrowBytes(*pool), targetBytes);
  uint64_t reserveBytes;
  uint64_t freeCapacity;
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++numReserves_;
    if (running_) {
      // NOTE: if there is a running memory arbitration, then we shall skip
      // reserving the free memory for the newly created memory pool but let it
      // grow its capacity on-demand later through the memory arbitration.
      return 0;
    }
    reserveBytes = decrementFreeCapacityLocked(bytesToReserve);
    pool->grow(reserveBytes);
    freeCapacity = freeCapacity_;
  }
  RECORD_METRIC_VALUE(kMetricArbitratorFreeCapacityBytes, freeCapacity);
  return reserveBytes;
}

uint64_t SharedArbitrator::shrinkCapacity(
    MemoryPool* pool,
    uint64_t targetBytes) {
  uint64_t freedBytes;
  uint64_t freeCapacity;
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++numReleases_;
    freedBytes = pool->shrink(targetBytes);
    incrementFreeCapacityLocked(freedBytes);
    freeCapacity = freeCapacity_;
  }
  RECORD_METRIC_VALUE(kMetricArbitratorFreeCapacityBytes, freeCapacity);
  return freedBytes;
}

std::vector<SharedArbitrator::Candidate> SharedArbitrator::getCandidateStats(
    const std::vector<std::shared_ptr<MemoryPool>>& pools) {
  std::vector<SharedArbitrator::Candidate> candidates;
  candidates.reserve(pools.size());
  for (const auto& pool : pools) {
    auto reclaimableBytesOpt = pool->reclaimableBytes();
    const uint64_t reclaimableBytes = reclaimableBytesOpt.value_or(0);
    candidates.push_back(
        {reclaimableBytesOpt.has_value(),
         reclaimableBytes,
         pool->freeBytes(),
         pool.get()});
  }
  return candidates;
}

bool SharedArbitrator::growCapacity(
    MemoryPool* pool,
    const std::vector<std::shared_ptr<MemoryPool>>& candidatePools,
    uint64_t targetBytes) {
  ScopedArbitration scopedArbitration(pool, this);
  MemoryPool* requestor = pool->root();
  if (FOLLY_UNLIKELY(requestor->aborted())) {
    RECORD_METRIC_VALUE(kMetricArbitratorFailuresCount);
    ++numFailures_;
    VELOX_MEM_POOL_ABORTED("The requestor has already been aborted");
  }

  if (FOLLY_UNLIKELY(!ensureCapacity(requestor, targetBytes))) {
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
  candidates.reserve(candidatePools.size());
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
  return (maxGrowBytes(pool) >= targetBytes) &&
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
  const uint64_t reclaimedBytes = reclaim(requestor, targetBytes);
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

bool SharedArbitrator::arbitrateMemory(
    MemoryPool* requestor,
    std::vector<Candidate>& candidates,
    uint64_t targetBytes) {
  VELOX_CHECK(!requestor->aborted());

  const uint64_t growTarget = std::min(
      maxGrowBytes(*requestor),
      std::max(memoryPoolTransferCapacity_, targetBytes));
  uint64_t freedBytes = decrementFreeCapacity(growTarget);
  if (freedBytes >= targetBytes) {
    requestor->grow(freedBytes);
    return true;
  }
  VELOX_CHECK_LT(freedBytes, growTarget);

  auto freeGuard = folly::makeGuard([&]() {
    // Returns the unused freed memory capacity back to the arbitrator.
    if (freedBytes > 0) {
      incrementFreeCapacity(freedBytes);
    }
  });

  freedBytes +=
      reclaimFreeMemoryFromCandidates(candidates, growTarget - freedBytes);
  if (freedBytes >= targetBytes) {
    const uint64_t bytesToGrow = std::min(growTarget, freedBytes);
    requestor->grow(bytesToGrow);
    freedBytes -= bytesToGrow;
    return true;
  }

  VELOX_CHECK_LT(freedBytes, growTarget);
  freedBytes += reclaimUsedMemoryFromCandidates(
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
  requestor->grow(bytesToGrow);
  freedBytes -= bytesToGrow;
  return true;
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
    MemoryPool* requestor,
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
        targetBytes - freedBytes, memoryPoolTransferCapacity_);
    VELOX_CHECK_GT(bytesToReclaim, 0);
    freedBytes += reclaim(candidate.pool, bytesToReclaim);
    if ((freedBytes >= targetBytes) || requestor->aborted()) {
      break;
    }
  }
  return freedBytes;
}

uint64_t SharedArbitrator::reclaim(
    MemoryPool* pool,
    uint64_t targetBytes) noexcept {
  uint64_t reclaimDurationUs{0};
  uint64_t reclaimedBytes{0};
  uint64_t freedBytes{0};
  MemoryReclaimer::Stats reclaimerStats;
  {
    MicrosecondTimer reclaimTimer(&reclaimDurationUs);
    const uint64_t oldCapacity = pool->capacity();
    try {
      freedBytes = pool->shrink(targetBytes);
      if (freedBytes < targetBytes) {
        pool->reclaim(
            targetBytes - freedBytes, memoryReclaimWaitMs_, reclaimerStats);
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
  numReclaimedBytes_ += reclaimedBytes - freedBytes;
  numShrunkBytes_ += freedBytes;
  reclaimTimeUs_ += reclaimDurationUs;
  numNonReclaimableAttempts_ += reclaimerStats.numNonReclaimableAttempts;
  VELOX_MEM_LOG_EVERY_MS(INFO, 1000)
      << "Reclaimed from memory pool " << pool->name() << " with target of "
      << succinctBytes(targetBytes) << ", actually reclaimed "
      << succinctBytes(freedBytes) << " free memory and "
      << succinctBytes(reclaimedBytes - freedBytes) << " used memory";
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

uint64_t SharedArbitrator::decrementFreeCapacity(uint64_t bytes) {
  uint64_t reserveBytes;
  uint64_t freeCapacity;
  {
    std::lock_guard<std::mutex> l(mutex_);
    reserveBytes = decrementFreeCapacityLocked(bytes);
    freeCapacity = freeCapacity_;
  }
  RECORD_METRIC_VALUE(kMetricArbitratorFreeCapacityBytes, freeCapacity);
  return reserveBytes;
}

uint64_t SharedArbitrator::decrementFreeCapacityLocked(uint64_t bytes) {
  const uint64_t targetBytes = std::min(freeCapacity_, bytes);
  VELOX_CHECK_LE(targetBytes, freeCapacity_);
  freeCapacity_ -= targetBytes;
  return targetBytes;
}

void SharedArbitrator::incrementFreeCapacity(uint64_t bytes) {
  uint64_t freeCapacity;
  {
    std::lock_guard<std::mutex> l(mutex_);
    incrementFreeCapacityLocked(bytes);
    freeCapacity = freeCapacity_;
  }
  RECORD_METRIC_VALUE(kMetricArbitratorFreeCapacityBytes, freeCapacity);
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
  stats.numSucceeded = numSucceeded_;
  stats.numAborted = numAborted_;
  stats.numFailures = numFailures_;
  stats.queueTimeUs = queueTimeUs_;
  stats.arbitrationTimeUs = arbitrationTimeUs_;
  stats.numShrunkBytes = numShrunkBytes_;
  stats.numReclaimedBytes = numReclaimedBytes_;
  stats.maxCapacityBytes = capacity_;
  stats.freeCapacityBytes = freeCapacity_;
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
    MemoryPool* requestor,
    SharedArbitrator* arbitrator)
    : requestor_(requestor),
      arbitrator_(arbitrator),
      startTime_(std::chrono::steady_clock::now()),
      arbitrationCtx_(*requestor_) {
  VELOX_CHECK_NOT_NULL(arbitrator_);
  arbitrator_->startArbitration(requestor);
  if (arbitrator_->arbitrationStateCheckCb_ != nullptr) {
    arbitrator_->arbitrationStateCheckCb_(*requestor);
  }
}

SharedArbitrator::ScopedArbitration::~ScopedArbitration() {
  requestor_->leaveArbitration();
  const auto arbitrationTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - startTime_)
          .count();
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricArbitratorArbitrationTimeMs, arbitrationTimeUs / 1'000);
  addThreadLocalRuntimeStat(
      "memoryArbitrationWallNanos",
      RuntimeCounter(arbitrationTimeUs * 1'000, RuntimeCounter::Unit::kNanos));
  arbitrator_->arbitrationTimeUs_ += arbitrationTimeUs;
  arbitrator_->finishArbitration();
}

void SharedArbitrator::startArbitration(MemoryPool* requestor) {
  requestor->enterArbitration();
  ContinueFuture waitPromise{ContinueFuture::makeEmpty()};
  {
    std::lock_guard<std::mutex> l(mutex_);
    RECORD_METRIC_VALUE(kMetricArbitratorRequestsCount);
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

  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::startArbitration", requestor);

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
} // namespace facebook::velox::exec
