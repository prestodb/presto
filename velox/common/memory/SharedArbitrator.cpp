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
#include "velox/common/config/Config.h"
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

template <typename T>
T getConfig(
    const std::unordered_map<std::string, std::string>& configs,
    const std::string_view& key,
    const T& defaultValue) {
  if (configs.count(std::string(key)) > 0) {
    try {
      return folly::to<T>(configs.at(std::string(key)));
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(
          "Failed while parsing SharedArbitrator configs: {}", e.what());
    }
  }
  return defaultValue;
}
} // namespace

int64_t SharedArbitrator::ExtraConfig::reservedCapacity(
    const std::unordered_map<std::string, std::string>& configs) {
  return config::toCapacity(
      getConfig<std::string>(
          configs, kReservedCapacity, std::string(kDefaultReservedCapacity)),
      config::CapacityUnit::BYTE);
}

uint64_t SharedArbitrator::ExtraConfig::memoryPoolInitialCapacity(
    const std::unordered_map<std::string, std::string>& configs) {
  return config::toCapacity(
      getConfig<std::string>(
          configs,
          kMemoryPoolInitialCapacity,
          std::string(kDefaultMemoryPoolInitialCapacity)),
      config::CapacityUnit::BYTE);
}

uint64_t SharedArbitrator::ExtraConfig::memoryPoolReservedCapacity(
    const std::unordered_map<std::string, std::string>& configs) {
  return config::toCapacity(
      getConfig<std::string>(
          configs,
          kMemoryPoolReservedCapacity,
          std::string(kDefaultMemoryPoolReservedCapacity)),
      config::CapacityUnit::BYTE);
}

uint64_t SharedArbitrator::ExtraConfig::memoryReclaimMaxWaitTimeMs(
    const std::unordered_map<std::string, std::string>& configs) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             config::toDuration(getConfig<std::string>(
                 configs,
                 kMemoryReclaimMaxWaitTime,
                 std::string(kDefaultMemoryReclaimMaxWaitTime))))
      .count();
}

uint64_t SharedArbitrator::ExtraConfig::memoryPoolMinFreeCapacity(
    const std::unordered_map<std::string, std::string>& configs) {
  return config::toCapacity(
      getConfig<std::string>(
          configs,
          kMemoryPoolMinFreeCapacity,
          std::string(kDefaultMemoryPoolMinFreeCapacity)),
      config::CapacityUnit::BYTE);
}

double SharedArbitrator::ExtraConfig::memoryPoolMinFreeCapacityPct(
    const std::unordered_map<std::string, std::string>& configs) {
  return getConfig<double>(
      configs,
      kMemoryPoolMinFreeCapacityPct,
      kDefaultMemoryPoolMinFreeCapacityPct);
}

bool SharedArbitrator::ExtraConfig::globalArbitrationEnabled(
    const std::unordered_map<std::string, std::string>& configs) {
  return getConfig<bool>(
      configs, kGlobalArbitrationEnabled, kDefaultGlobalArbitrationEnabled);
}

bool SharedArbitrator::ExtraConfig::checkUsageLeak(
    const std::unordered_map<std::string, std::string>& configs) {
  return getConfig<bool>(configs, kCheckUsageLeak, kDefaultCheckUsageLeak);
}

uint64_t SharedArbitrator::ExtraConfig::fastExponentialGrowthCapacityLimitBytes(
    const std::unordered_map<std::string, std::string>& configs) {
  return config::toCapacity(
      getConfig<std::string>(
          configs,
          kFastExponentialGrowthCapacityLimit,
          std::string(kDefaultFastExponentialGrowthCapacityLimit)),
      config::CapacityUnit::BYTE);
}

double SharedArbitrator::ExtraConfig::slowCapacityGrowPct(
    const std::unordered_map<std::string, std::string>& configs) {
  return getConfig<double>(
      configs, kSlowCapacityGrowPct, kDefaultSlowCapacityGrowPct);
}

SharedArbitrator::SharedArbitrator(const Config& config)
    : MemoryArbitrator(config),
      reservedCapacity_(ExtraConfig::reservedCapacity(config.extraConfigs)),
      memoryPoolInitialCapacity_(
          ExtraConfig::memoryPoolInitialCapacity(config.extraConfigs)),
      memoryPoolReservedCapacity_(
          ExtraConfig::memoryPoolReservedCapacity(config.extraConfigs)),
      memoryReclaimWaitMs_(
          ExtraConfig::memoryReclaimMaxWaitTimeMs(config.extraConfigs)),
      globalArbitrationEnabled_(
          ExtraConfig::globalArbitrationEnabled(config.extraConfigs)),
      checkUsageLeak_(ExtraConfig::checkUsageLeak(config.extraConfigs)),
      fastExponentialGrowthCapacityLimit_(
          ExtraConfig::fastExponentialGrowthCapacityLimitBytes(
              config.extraConfigs)),
      slowCapacityGrowPct_(
          ExtraConfig::slowCapacityGrowPct(config.extraConfigs)),
      memoryPoolMinFreeCapacity_(
          ExtraConfig::memoryPoolMinFreeCapacity(config.extraConfigs)),
      memoryPoolMinFreeCapacityPct_(
          ExtraConfig::memoryPoolMinFreeCapacityPct(config.extraConfigs)),
      freeReservedCapacity_(reservedCapacity_),
      freeNonReservedCapacity_(capacity_ - freeReservedCapacity_) {
  VELOX_CHECK_EQ(kind_, config.kind);
  VELOX_CHECK_LE(reservedCapacity_, capacity_);
  VELOX_CHECK_GE(slowCapacityGrowPct_, 0);
  VELOX_CHECK_GE(memoryPoolMinFreeCapacityPct_, 0);
  VELOX_CHECK_LE(memoryPoolMinFreeCapacityPct_, 1);
  VELOX_CHECK_EQ(
      fastExponentialGrowthCapacityLimit_ == 0,
      slowCapacityGrowPct_ == 0,
      "fastExponentialGrowthCapacityLimit_ {} and slowCapacityGrowPct_ {} "
      "both need to be set (non-zero) at the same time to enable growth capacity "
      "adjustment.",
      fastExponentialGrowthCapacityLimit_,
      slowCapacityGrowPct_);
  VELOX_CHECK_EQ(
      memoryPoolMinFreeCapacity_ == 0,
      memoryPoolMinFreeCapacityPct_ == 0,
      "memoryPoolMinFreeCapacity_ {} and memoryPoolMinFreeCapacityPct_ {} both "
      "need to be set (non-zero) at the same time to enable shrink capacity "
      "adjustment.",
      memoryPoolMinFreeCapacity_,
      memoryPoolMinFreeCapacityPct_);
}

std::string SharedArbitrator::Candidate::toString() const {
  return fmt::format(
      "CANDIDATE[{}] RECLAIMABLE_BYTES[{}] FREE_BYTES[{}]]",
      pool->name(),
      succinctBytes(reclaimableBytes),
      succinctBytes(freeBytes));
}

SharedArbitrator::~SharedArbitrator() {
  VELOX_CHECK(candidates_.empty());
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

void SharedArbitrator::addPool(const std::shared_ptr<MemoryPool>& pool) {
  VELOX_CHECK_EQ(pool->capacity(), 0);
  {
    std::unique_lock guard{poolLock_};
    VELOX_CHECK_EQ(candidates_.count(pool.get()), 0);
    candidates_.emplace(pool.get(), pool);
  }

  std::lock_guard<std::mutex> l(stateLock_);
  const uint64_t maxBytesToReserve =
      std::min(maxGrowCapacity(*pool), memoryPoolInitialCapacity_);
  const uint64_t minBytesToReserve = minGrowCapacity(*pool);
  const uint64_t reservedBytes =
      decrementFreeCapacityLocked(maxBytesToReserve, minBytesToReserve);
  try {
    checkedGrow(pool.get(), reservedBytes, 0);
  } catch (const VeloxRuntimeError&) {
    incrementFreeCapacityLocked(reservedBytes);
  }
}

void SharedArbitrator::removePool(MemoryPool* pool) {
  VELOX_CHECK_EQ(pool->reservedBytes(), 0);
  shrinkCapacity(pool);

  std::unique_lock guard{poolLock_};
  const auto ret = candidates_.erase(pool);
  VELOX_CHECK_EQ(ret, 1);
}

void SharedArbitrator::getCandidates(
    ArbitrationOperation* op,
    bool freeCapacityOnly) {
  op->candidates.clear();

  std::shared_lock guard{poolLock_};
  op->candidates.reserve(candidates_.size());
  for (const auto& candidate : candidates_) {
    const bool selfCandidate = op->requestPool == candidate.first;
    std::shared_ptr<MemoryPool> pool = candidate.second.lock();
    if (pool == nullptr) {
      VELOX_CHECK(!selfCandidate);
      continue;
    }
    op->candidates.push_back(
        {pool,
         freeCapacityOnly ? 0 : reclaimableUsedCapacity(*pool, selfCandidate),
         reclaimableFreeCapacity(*pool, selfCandidate),
         pool->reservedBytes()});
  }
  VELOX_CHECK(!op->candidates.empty());
}

void SharedArbitrator::sortCandidatesByReclaimableFreeCapacity(
    std::vector<Candidate>& candidates) {
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

void SharedArbitrator::sortCandidatesByReclaimableUsedCapacity(
    std::vector<Candidate>& candidates) {
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

void SharedArbitrator::sortCandidatesByUsage(
    std::vector<Candidate>& candidates) {
  std::sort(
      candidates.begin(),
      candidates.end(),
      [](const SharedArbitrator::Candidate& lhs,
         const SharedArbitrator::Candidate& rhs) {
        return lhs.reservedBytes > rhs.reservedBytes;
      });
}

const SharedArbitrator::Candidate&
SharedArbitrator::findCandidateWithLargestCapacity(
    MemoryPool* requestor,
    uint64_t targetBytes,
    const std::vector<SharedArbitrator::Candidate>& candidates) {
  VELOX_CHECK(!candidates.empty());
  int32_t candidateIdx{-1};
  uint64_t maxCapacity{0};
  for (int32_t i = 0; i < candidates.size(); ++i) {
    const bool isCandidate = candidates[i].pool.get() == requestor;
    // For capacity comparison, the requestor's capacity should include both its
    // current capacity and the capacity growth.
    const uint64_t capacity =
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
  // Checks if a query memory pool has likely finished processing. It is likely
  // this pool has finished when it has 0 current usage and non-0 past usage. If
  // there is a high chance this pool finished, then we don't have to respect
  // the memory pool reserved capacity limit check.
  //
  // NOTE: for query system like Prestissimo, it holds a finished query state in
  // minutes for query stats fetch request from the Presto coordinator.
  if (isSelfReclaim || (pool.reservedBytes() == 0 && pool.peakBytes() != 0)) {
    return pool.capacity();
  }
  return std::max<int64_t>(0, pool.capacity() - memoryPoolReservedCapacity_);
}

int64_t SharedArbitrator::reclaimableFreeCapacity(
    const MemoryPool& pool,
    bool isSelfReclaim) const {
  const auto freeBytes = pool.freeBytes();
  if (freeBytes == 0) {
    return 0;
  }
  return std::min<int64_t>(
      isSelfReclaim ? freeBytes : getCapacityShrinkTarget(pool, freeBytes),
      maxReclaimableCapacity(pool, isSelfReclaim));
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

uint64_t SharedArbitrator::decrementFreeCapacity(
    uint64_t maxBytesToReserve,
    uint64_t minBytesToReserve) {
  uint64_t reservedBytes{0};
  {
    std::lock_guard<std::mutex> l(stateLock_);
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

uint64_t SharedArbitrator::getCapacityShrinkTarget(
    const MemoryPool& pool,
    uint64_t requestBytes) const {
  VELOX_CHECK_NE(requestBytes, 0);
  auto targetBytes = requestBytes;
  if (memoryPoolMinFreeCapacity_ != 0) {
    const auto minFreeBytes = std::min(
        static_cast<uint64_t>(pool.capacity() * memoryPoolMinFreeCapacityPct_),
        memoryPoolMinFreeCapacity_);
    const auto maxShrinkBytes = std::max<int64_t>(
        0LL, pool.freeBytes() - static_cast<int64_t>(minFreeBytes));
    targetBytes = std::min(targetBytes, static_cast<uint64_t>(maxShrinkBytes));
  }
  return targetBytes;
}

uint64_t SharedArbitrator::shrinkCapacity(
    MemoryPool* pool,
    uint64_t requestBytes) {
  std::lock_guard<std::mutex> l(stateLock_);
  const uint64_t freedBytes = shrinkPool(
      pool,
      requestBytes == 0 ? 0 : getCapacityShrinkTarget(*pool, requestBytes));
  incrementFreeCapacityLocked(freedBytes);
  return freedBytes;
}

uint64_t SharedArbitrator::shrinkCapacity(
    uint64_t requestBytes,
    bool allowSpill,
    bool allowAbort) {
  incrementGlobalArbitrationCount();
  const uint64_t targetBytes = requestBytes == 0 ? capacity_ : requestBytes;
  ArbitrationOperation op(targetBytes);
  ScopedArbitration scopedArbitration(this, &op);

  std::lock_guard<std::shared_mutex> exclusiveLock(arbitrationLock_);
  getCandidates(&op);

  uint64_t reclaimedBytes{0};
  if (allowSpill) {
    uint64_t freedBytes{0};
    reclaimUsedMemoryFromCandidatesBySpill(&op, freedBytes);
    reclaimedBytes += freedBytes;
    if (freedBytes > 0) {
      incrementFreeCapacity(freedBytes);
    }
    if (reclaimedBytes >= op.requestBytes) {
      return reclaimedBytes;
    }
    if (allowAbort) {
      // Candidate stats may change after spilling.
      getCandidates(&op);
    }
  }

  if (allowAbort) {
    uint64_t freedBytes{0};
    reclaimUsedMemoryFromCandidatesByAbort(&op, freedBytes);
    reclaimedBytes += freedBytes;
    if (freedBytes > 0) {
      incrementFreeCapacity(freedBytes);
    }
  }
  return reclaimedBytes;
}

void SharedArbitrator::testingFreeCapacity(uint64_t capacity) {
  std::lock_guard<std::mutex> l(stateLock_);
  incrementFreeCapacityLocked(capacity);
}

uint64_t SharedArbitrator::testingNumRequests() const {
  return numRequests_;
}

uint64_t SharedArbitrator::getCapacityGrowthTarget(
    const MemoryPool& pool,
    uint64_t requestBytes) const {
  if (fastExponentialGrowthCapacityLimit_ == 0 && slowCapacityGrowPct_ == 0) {
    return requestBytes;
  }
  uint64_t targetBytes{0};
  const auto capacity = pool.capacity();
  if (capacity * 2 <= fastExponentialGrowthCapacityLimit_) {
    targetBytes = capacity;
  } else {
    targetBytes = capacity * slowCapacityGrowPct_;
  }
  return std::max(requestBytes, targetBytes);
}

bool SharedArbitrator::growCapacity(MemoryPool* pool, uint64_t requestBytes) {
  // NOTE: we shouldn't trigger the recursive memory capacity growth under
  // memory arbitration context.
  VELOX_CHECK(!underMemoryArbitration());

  ArbitrationOperation op(
      pool, requestBytes, getCapacityGrowthTarget(*pool, requestBytes));
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
    VELOX_MEM_LOG(ERROR) << "Can't grow " << op->requestPool->name()
                         << " capacity to "
                         << succinctBytes(
                                op->requestPool->capacity() + op->requestBytes)
                         << " which exceeds its max capacity "
                         << succinctBytes(op->requestPool->maxCapacity())
                         << ", current capacity "
                         << succinctBytes(op->requestPool->capacity())
                         << ", request " << succinctBytes(op->requestBytes);
    return false;
  }
  VELOX_CHECK(!op->requestPool->aborted());

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
  if (freedBytes >= op->requestBytes) {
    checkedGrow(op->requestPool, freedBytes, op->requestBytes);
    freedBytes = 0;
    return true;
  }
  VELOX_CHECK_LT(freedBytes, maxGrowTarget);

  getCandidates(op, /*freeCapacityOnly=*/true);
  freedBytes +=
      reclaimFreeMemoryFromCandidates(op, maxGrowTarget - freedBytes, true);
  if (freedBytes >= op->requestBytes) {
    const uint64_t bytesToGrow = std::min(maxGrowTarget, freedBytes);
    checkedGrow(op->requestPool, bytesToGrow, op->requestBytes);
    freedBytes -= bytesToGrow;
    return true;
  }
  VELOX_CHECK_LT(freedBytes, maxGrowTarget);

  if (!globalArbitrationEnabled_) {
    freedBytes += reclaim(op->requestPool, maxGrowTarget - freedBytes, true);
  }
  checkIfAborted(op);

  if (freedBytes >= op->requestBytes) {
    const uint64_t bytesToGrow = std::min(maxGrowTarget, freedBytes);
    checkedGrow(op->requestPool, bytesToGrow, op->requestBytes);
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
    VELOX_CHECK(!op->requestPool->aborted());
    if (!handleOOM(op)) {
      break;
    }
  }
  VELOX_MEM_LOG(ERROR)
      << "Failed to arbitrate sufficient memory for memory pool "
      << op->requestPool->name() << ", request "
      << succinctBytes(op->requestBytes) << " after " << attempts
      << " attempts, Arbitrator state: " << toString();
  updateArbitrationFailureStats();
  return false;
}

void SharedArbitrator::getGrowTargets(
    ArbitrationOperation* op,
    uint64_t& maxGrowTarget,
    uint64_t& minGrowTarget) {
  VELOX_CHECK(op->targetBytes.has_value());
  maxGrowTarget =
      std::min(maxGrowCapacity(*op->requestPool), op->targetBytes.value());
  minGrowTarget = minGrowCapacity(*op->requestPool);
}

void SharedArbitrator::checkIfAborted(ArbitrationOperation* op) {
  if (op->requestPool->aborted()) {
    updateArbitrationFailureStats();
    VELOX_MEM_POOL_ABORTED("The requestor pool has been aborted");
  }
}

bool SharedArbitrator::maybeGrowFromSelf(ArbitrationOperation* op) {
  if (op->requestPool->freeBytes() >= op->requestBytes) {
    if (growPool(op->requestPool, 0, op->requestBytes)) {
      return true;
    }
  }
  return false;
}

bool SharedArbitrator::checkCapacityGrowth(ArbitrationOperation* op) const {
  return (maxGrowCapacity(*op->requestPool) >= op->requestBytes) &&
      (capacityAfterGrowth(*op->requestPool, op->requestBytes) <= capacity_);
}

bool SharedArbitrator::ensureCapacity(ArbitrationOperation* op) {
  if ((op->requestBytes > capacity_) ||
      (op->requestBytes > op->requestPool->maxCapacity())) {
    return false;
  }
  if (checkCapacityGrowth(op)) {
    return true;
  }

  const uint64_t reclaimedBytes =
      reclaim(op->requestPool, op->requestBytes, true);
  // NOTE: return the reclaimed bytes back to the arbitrator and let the memory
  // arbitration process to grow the requestor's memory capacity accordingly.
  incrementFreeCapacity(reclaimedBytes);
  // Check if the requestor has been aborted in reclaim operation above.
  if (op->requestPool->aborted()) {
    updateArbitrationFailureStats();
    VELOX_MEM_POOL_ABORTED("The requestor pool has been aborted");
  }
  return checkCapacityGrowth(op);
}

bool SharedArbitrator::handleOOM(ArbitrationOperation* op) {
  MemoryPool* victim = findCandidateWithLargestCapacity(
                           op->requestPool, op->requestBytes, op->candidates)
                           .pool.get();
  if (op->requestPool == victim) {
    VELOX_MEM_LOG(ERROR)
        << "Requestor memory pool " << op->requestPool->name()
        << " is selected as victim memory pool so fail the memory arbitration";
    return false;
  }
  VELOX_MEM_LOG(WARNING) << "Aborting victim memory pool " << victim->name()
                         << " to free up memory for requestor "
                         << op->requestPool->name();
  try {
    if (victim == op->requestPool) {
      VELOX_MEM_POOL_CAP_EXCEEDED(
          memoryPoolAbortMessage(victim, op->requestPool, op->requestBytes));
    } else {
      VELOX_MEM_POOL_ABORTED(
          memoryPoolAbortMessage(victim, op->requestPool, op->requestBytes));
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
  VELOX_CHECK(!op->requestPool->aborted());
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
  if (freedBytes >= op->requestBytes) {
    checkedGrow(op->requestPool, freedBytes, op->requestBytes);
    freedBytes = 0;
    return true;
  }
  VELOX_CHECK_LT(freedBytes, maxGrowTarget);

  // Get refreshed stats before the global memory arbitration run.
  getCandidates(op);

  freedBytes +=
      reclaimFreeMemoryFromCandidates(op, maxGrowTarget - freedBytes, false);
  if (freedBytes >= op->requestBytes) {
    const uint64_t bytesToGrow = std::min(maxGrowTarget, freedBytes);
    checkedGrow(op->requestPool, bytesToGrow, op->requestBytes);
    freedBytes -= bytesToGrow;
    return true;
  }
  VELOX_CHECK_LT(freedBytes, maxGrowTarget);

  reclaimUsedMemoryFromCandidatesBySpill(op, freedBytes);
  checkIfAborted(op);

  if (freedBytes < op->requestBytes) {
    VELOX_MEM_LOG(WARNING)
        << "Failed to arbitrate sufficient memory for memory pool "
        << op->requestPool->name() << ", request "
        << succinctBytes(op->requestBytes) << ", only "
        << succinctBytes(freedBytes)
        << " has been freed, Arbitrator state: " << toString();
    return false;
  }

  const uint64_t bytesToGrow = std::min(freedBytes, maxGrowTarget);
  checkedGrow(op->requestPool, bytesToGrow, op->requestBytes);
  freedBytes -= bytesToGrow;
  return true;
}

uint64_t SharedArbitrator::reclaimFreeMemoryFromCandidates(
    ArbitrationOperation* op,
    uint64_t reclaimTargetBytes,
    bool isLocalArbitration) {
  // Sort candidate memory pools based on their reclaimable free capacity.
  sortCandidatesByReclaimableFreeCapacity(op->candidates);

  std::lock_guard<std::mutex> l(stateLock_);
  uint64_t reclaimedBytes{0};
  for (const auto& candidate : op->candidates) {
    VELOX_CHECK_LT(reclaimedBytes, reclaimTargetBytes);
    if (candidate.freeBytes == 0) {
      break;
    }
    if (isLocalArbitration && (candidate.pool.get() != op->requestPool) &&
        isUnderArbitrationLocked(candidate.pool.get())) {
      // If the reclamation is for local arbitration and the candidate pool is
      // also under arbitration processing, then we can't reclaim from the
      // candidate pool as it might cause concurrent changes to the candidate
      // pool's capacity.
      continue;
    }
    const int64_t bytesToReclaim = std::min<int64_t>(
        reclaimTargetBytes - reclaimedBytes,
        reclaimableFreeCapacity(
            *candidate.pool, candidate.pool.get() == op->requestPool));
    if (bytesToReclaim <= 0) {
      continue;
    }
    reclaimedBytes += shrinkPool(candidate.pool.get(), bytesToReclaim);
    if (reclaimedBytes >= reclaimTargetBytes) {
      break;
    }
  }
  reclaimedFreeBytes_ += reclaimedBytes;
  return reclaimedBytes;
}

void SharedArbitrator::reclaimUsedMemoryFromCandidatesBySpill(
    ArbitrationOperation* op,
    uint64_t& freedBytes) {
  // Sort candidate memory pools based on their reclaimable used capacity.
  sortCandidatesByReclaimableUsedCapacity(op->candidates);

  for (const auto& candidate : op->candidates) {
    VELOX_CHECK_LT(freedBytes, op->requestBytes);
    if (candidate.reclaimableBytes == 0) {
      break;
    }
    freedBytes +=
        reclaim(candidate.pool.get(), op->requestBytes - freedBytes, false);
    if ((freedBytes >= op->requestBytes) ||
        (op->requestPool != nullptr && op->requestPool->aborted())) {
      break;
    }
  }
}

void SharedArbitrator::reclaimUsedMemoryFromCandidatesByAbort(
    ArbitrationOperation* op,
    uint64_t& freedBytes) {
  sortCandidatesByUsage(op->candidates);

  for (const auto& candidate : op->candidates) {
    VELOX_CHECK_LT(freedBytes, op->requestBytes);
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
      abort(candidate.pool.get(), std::current_exception());
    }
    freedBytes += shrinkPool(candidate.pool.get(), 0);
    if (freedBytes >= op->requestBytes) {
      break;
    }
  }
}

uint64_t SharedArbitrator::reclaim(
    MemoryPool* pool,
    uint64_t targetBytes,
    bool isLocalArbitration) noexcept {
  int64_t bytesToReclaim =
      std::min<uint64_t>(targetBytes, maxReclaimableCapacity(*pool, true));
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
  std::lock_guard<std::mutex> l(stateLock_);
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
  std::lock_guard<std::mutex> l(stateLock_);
  return statsLocked();
}

MemoryArbitrator::Stats SharedArbitrator::statsLocked() const {
  Stats stats;
  stats.numRequests = numRequests_;
  stats.numRunning = numPending_;
  stats.numAborted = numAborted_;
  stats.numFailures = numFailures_;
  stats.reclaimedFreeBytes = reclaimedFreeBytes_;
  stats.reclaimedUsedBytes = reclaimedUsedBytes_;
  stats.maxCapacityBytes = capacity_;
  stats.freeCapacityBytes = freeNonReservedCapacity_ + freeReservedCapacity_;
  stats.freeReservedCapacityBytes = freeReservedCapacity_;
  stats.numNonReclaimableAttempts = numNonReclaimableAttempts_;
  return stats;
}

std::string SharedArbitrator::toString() const {
  std::lock_guard<std::mutex> l(stateLock_);
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
      arbitrationCtx_(
          operation->requestPool == nullptr
              ? std::make_unique<ScopedMemoryArbitrationContext>()
              : std::make_unique<ScopedMemoryArbitrationContext>(
                    operation->requestPool)),
      startTime_(std::chrono::steady_clock::now()) {
  VELOX_CHECK_NOT_NULL(arbitrator_);
  VELOX_CHECK_NOT_NULL(operation_);
  if (arbitrator_->arbitrationStateCheckCb_ != nullptr &&
      operation_->requestPool != nullptr) {
    arbitrator_->arbitrationStateCheckCb_(*operation_->requestPool);
  }
  arbitrator_->startArbitration(operation_);
}

SharedArbitrator::ScopedArbitration::~ScopedArbitration() {
  arbitrator_->finishArbitration(operation_);

  // Report arbitration operation stats.
  const auto arbitrationTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - operation_->startTime)
          .count();
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricArbitratorOpExecTimeMs, arbitrationTimeUs / 1'000);
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
}

void SharedArbitrator::startArbitration(ArbitrationOperation* op) {
  updateArbitrationRequestStats();
  ContinueFuture waitPromise{ContinueFuture::makeEmpty()};
  {
    std::lock_guard<std::mutex> l(stateLock_);
    ++numPending_;
    if (op->requestPool != nullptr) {
      auto it = arbitrationQueues_.find(op->requestPool);
      if (it != arbitrationQueues_.end()) {
        it->second->waitPromises.emplace_back(
            fmt::format("Wait for arbitration {}", op->requestPool->name()));
        waitPromise = it->second->waitPromises.back().getSemiFuture();
      } else {
        arbitrationQueues_.emplace(
            op->requestPool, std::make_unique<ArbitrationQueue>(op));
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
    std::lock_guard<std::mutex> l(stateLock_);
    VELOX_CHECK_GT(numPending_, 0);
    --numPending_;
    if (op->requestPool != nullptr) {
      auto it = arbitrationQueues_.find(op->requestPool);
      VELOX_CHECK(
          it != arbitrationQueues_.end(),
          "{} not found",
          op->requestPool->name());
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
