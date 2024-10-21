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
#include <folly/system/ThreadName.h>
#include <pthread.h>
#include <mutex>
#include "velox/common/base/AsyncSource.h"
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
#define RETURN_IF_TRUE(func) \
  {                          \
    const bool ret = func;   \
    if (ret) {               \
      return ret;            \
    }                        \
  }

#define CHECKED_GROW(pool, growBytes, reservationBytes) \
  try {                                                 \
    checkedGrow(pool, growBytes, reservationBytes);     \
  } catch (const VeloxRuntimeError& e) {                \
    freeCapacity(growBytes);                            \
    throw;                                              \
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

#define VELOX_MEM_ARBITRATION_TIMEOUT(errorMessage)                  \
  _VELOX_THROW(                                                      \
      ::facebook::velox::VeloxRuntimeError,                          \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(),  \
      ::facebook::velox::error_code::kMemArbitrationTimeout.c_str(), \
      /* isRetriable */ true,                                        \
      "{}",                                                          \
      errorMessage);
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

uint64_t SharedArbitrator::ExtraConfig::memoryPoolMinReclaimBytes(
    const std::unordered_map<std::string, std::string>& configs) {
  return config::toCapacity(
      getConfig<std::string>(
          configs,
          kMemoryPoolMinReclaimBytes,
          std::string(kDefaultMemoryPoolMinReclaimBytes)),
      config::CapacityUnit::BYTE);
}

uint64_t SharedArbitrator::ExtraConfig::memoryPoolAbortCapacityLimit(
    const std::unordered_map<std::string, std::string>& configs) {
  return config::toCapacity(
      getConfig<std::string>(
          configs,
          kMemoryPoolAbortCapacityLimit,
          std::string(kDefaultMemoryPoolAbortCapacityLimit)),
      config::CapacityUnit::BYTE);
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

double SharedArbitrator::ExtraConfig::memoryReclaimThreadsHwMultiplier(
    const std::unordered_map<std::string, std::string>& configs) {
  return getConfig<double>(
      configs,
      kMemoryReclaimThreadsHwMultiplier,
      kDefaultMemoryReclaimThreadsHwMultiplier);
}

uint32_t SharedArbitrator::ExtraConfig::globalArbitrationMemoryReclaimPct(
    const std::unordered_map<std::string, std::string>& configs) {
  return getConfig<uint32_t>(
      configs,
      kGlobalArbitrationMemoryReclaimPct,
      kDefaultGlobalMemoryArbitrationReclaimPct);
}

SharedArbitrator::SharedArbitrator(const Config& config)
    : MemoryArbitrator(config),
      reservedCapacity_(ExtraConfig::reservedCapacity(config.extraConfigs)),
      checkUsageLeak_(ExtraConfig::checkUsageLeak(config.extraConfigs)),
      maxArbitrationTimeMs_(
          ExtraConfig::memoryReclaimMaxWaitTimeMs(config.extraConfigs)),
      participantConfig_(
          ExtraConfig::memoryPoolInitialCapacity(config.extraConfigs),
          ExtraConfig::memoryPoolReservedCapacity(config.extraConfigs),
          ExtraConfig::fastExponentialGrowthCapacityLimitBytes(
              config.extraConfigs),
          ExtraConfig::slowCapacityGrowPct(config.extraConfigs),
          ExtraConfig::memoryPoolMinFreeCapacity(config.extraConfigs),
          ExtraConfig::memoryPoolMinFreeCapacityPct(config.extraConfigs),
          ExtraConfig::memoryPoolMinReclaimBytes(config.extraConfigs),
          ExtraConfig::memoryPoolAbortCapacityLimit(config.extraConfigs)),
      memoryReclaimThreadsHwMultiplier_(
          ExtraConfig::memoryReclaimThreadsHwMultiplier(config.extraConfigs)),
      globalArbitrationEnabled_(
          ExtraConfig::globalArbitrationEnabled(config.extraConfigs)),
      globalArbitrationMemoryReclaimPct_(
          ExtraConfig::globalArbitrationMemoryReclaimPct(config.extraConfigs)),
      freeReservedCapacity_(reservedCapacity_),
      freeNonReservedCapacity_(capacity_ - freeReservedCapacity_) {
  VELOX_CHECK_EQ(kind_, config.kind);
  VELOX_CHECK_LE(reservedCapacity_, capacity_);
  VELOX_CHECK_GT(
      maxArbitrationTimeMs_, 0, "maxArbitrationTimeMs can't be zero");

  VELOX_CHECK_LE(
      globalArbitrationMemoryReclaimPct_,
      100,
      "Invalid globalArbitrationMemoryReclaimPct");

  VELOX_CHECK_GT(
      memoryReclaimThreadsHwMultiplier_,
      0.0,
      "memoryReclaimThreadsHwMultiplier_ needs to be positive");

  const uint64_t numReclaimThreads = std::max<size_t>(
      1,
      std::thread::hardware_concurrency() * memoryReclaimThreadsHwMultiplier_);
  memoryReclaimExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
      numReclaimThreads,
      std::make_shared<folly::NamedThreadFactory>("MeomryReclaim"));
  VELOX_MEM_LOG(INFO) << "Start memory reclaim executor with "
                      << numReclaimThreads << " threads";

  setupGlobalArbitration();

  VELOX_MEM_LOG(INFO) << "Shared arbitrator created with "
                      << succinctBytes(capacity_) << " capacity, "
                      << succinctBytes(reservedCapacity_)
                      << " reserved capacity";
  if (globalArbitrationEnabled_) {
    VELOX_MEM_LOG(INFO) << "Arbitration config: max arbitration time "
                        << succinctMillis(maxArbitrationTimeMs_)
                        << ", global memory reclaim percentage "
                        << globalArbitrationMemoryReclaimPct_;
  }
  VELOX_MEM_LOG(INFO) << "Memory pool participant config: "
                      << participantConfig_.toString();
}

void SharedArbitrator::setupGlobalArbitration() {
  if (!globalArbitrationEnabled_) {
    return;
  }
  VELOX_CHECK_NULL(globalArbitrationController_);

  const uint64_t minAbortCapacity = 32 << 20;
  for (auto abortLimit = participantConfig_.abortCapacityLimit; abortLimit >=
       std::max<uint64_t>(minAbortCapacity,
                          folly::nextPowTwo(participantConfig_.minCapacity));
       abortLimit /= 2) {
    globalArbitrationAbortCapacityLimits_.push_back(abortLimit);
  }
  globalArbitrationAbortCapacityLimits_.push_back(0);

  VELOX_MEM_LOG(INFO) << "Global arbitration abort capacity limits: "
                      << folly::join(
                             ",", globalArbitrationAbortCapacityLimits_);

  globalArbitrationController_ = std::make_unique<std::thread>([&]() {
    folly::setThreadName("GlobalArbitrationController");
    globalArbitrationMain();
  });
}

void SharedArbitrator::shutdownGlobalArbitration() {
  if (!globalArbitrationEnabled_) {
    VELOX_CHECK_NULL(globalArbitrationController_);
    return;
  }

  VELOX_CHECK(!globalArbitrationAbortCapacityLimits_.empty());
  VELOX_CHECK_NOT_NULL(globalArbitrationController_);
  {
    std::lock_guard<std::mutex> l(stateLock_);
    // We only expect stop global arbitration once during velox runtime
    // shutdown.
    VELOX_CHECK(!globalArbitrationStop_);
    VELOX_CHECK(globalArbitrationWaiters_.empty());
    globalArbitrationStop_ = true;
  }

  VELOX_MEM_LOG(INFO) << "Stopping global arbitration controller";
  globalArbitrationThreadCv_.notify_one();
  globalArbitrationController_->join();
  globalArbitrationController_.reset();
  VELOX_MEM_LOG(INFO) << "Global arbitration controller stopped";
}

void SharedArbitrator::wakeupGlobalArbitrationThread() {
  VELOX_CHECK(globalArbitrationEnabled_);
  VELOX_CHECK_NOT_NULL(globalArbitrationController_);
  incrementGlobalArbitrationWaitCount();
  globalArbitrationThreadCv_.notify_one();
}

SharedArbitrator::~SharedArbitrator() {
  shutdownGlobalArbitration();

  VELOX_MEM_LOG(INFO) << "Stopping memory reclaim executor '"
                      << memoryReclaimExecutor_->getName() << "': threads: "
                      << memoryReclaimExecutor_->numActiveThreads() << "/"
                      << memoryReclaimExecutor_->numThreads()
                      << ", task queue: "
                      << memoryReclaimExecutor_->getTaskQueueSize();
  memoryReclaimExecutor_.reset();
  VELOX_MEM_LOG(INFO) << "Memory reclaim executor stopped";

  VELOX_CHECK_EQ(
      participants_.size(), 0, "Unexpected alive participants on destruction");

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

void SharedArbitrator::startArbitration(ArbitrationOperation* op) {
  updateArbitrationRequestStats();
  ++numRunning_;
  op->start();
}

void SharedArbitrator::finishArbitration(ArbitrationOperation* op) {
  VELOX_CHECK_GT(numRunning_, 0);
  --numRunning_;
  op->finish();

  const auto stats = op->stats();
  if (stats.executionTimeMs != 0) {
    RECORD_HISTOGRAM_METRIC_VALUE(
        kMetricArbitratorOpExecTimeMs, stats.executionTimeMs);
    addThreadLocalRuntimeStat(
        kMemoryArbitrationWallNanos,
        RuntimeCounter(
            stats.executionTimeMs * 1'000 * 1'000,
            RuntimeCounter::Unit::kNanos));
  }

  if (stats.localArbitrationWaitTimeMs != 0) {
    addThreadLocalRuntimeStat(
        kLocalArbitrationWaitWallNanos,
        RuntimeCounter(
            stats.localArbitrationWaitTimeMs * 1'000 * 1'000,
            RuntimeCounter::Unit::kNanos));
  }
  if (stats.localArbitrationExecTimeMs != 0) {
    addThreadLocalRuntimeStat(
        kLocalArbitrationExecutionWallNanos,
        RuntimeCounter(
            stats.localArbitrationExecTimeMs * 1'000 * 1'000,
            RuntimeCounter::Unit::kNanos));
  }
  if (stats.globalArbitrationWaitTimeMs != 0) {
    addThreadLocalRuntimeStat(
        kGlobalArbitrationWaitWallNanos,
        RuntimeCounter(
            stats.globalArbitrationWaitTimeMs * 1'000 * 1'000,
            RuntimeCounter::Unit::kNanos));
    RECORD_HISTOGRAM_METRIC_VALUE(
        kMetricArbitratorGlobalArbitrationWaitTimeMs,
        stats.globalArbitrationWaitTimeMs);
  }
}

void SharedArbitrator::addPool(const std::shared_ptr<MemoryPool>& pool) {
  VELOX_CHECK_EQ(pool->capacity(), 0);

  auto newParticipant = ArbitrationParticipant::create(
      nextParticipantId_++, pool, &participantConfig_);
  {
    std::unique_lock guard{participantLock_};
    VELOX_CHECK_EQ(
        participants_.count(pool->name()),
        0,
        "Memory pool {} already exists",
        pool->name());
    participants_.emplace(newParticipant->name(), newParticipant);
  }

  auto scopedParticipant = newParticipant->lock().value();
  std::vector<ContinuePromise> arbitrationWaiters;
  {
    std::lock_guard<std::mutex> l(stateLock_);
    const uint64_t minBytesToReserve = std::min(
        scopedParticipant->maxCapacity(), scopedParticipant->minCapacity());
    const uint64_t maxBytesToReserve = std::max(
        minBytesToReserve,
        std::min(
            scopedParticipant->maxCapacity(), participantConfig_.initCapacity));
    const uint64_t allocatedBytes = allocateCapacityLocked(
        scopedParticipant->id(), 0, maxBytesToReserve, minBytesToReserve);
    if (allocatedBytes > 0) {
      VELOX_CHECK_LE(allocatedBytes, maxBytesToReserve);
      try {
        checkedGrow(scopedParticipant, allocatedBytes, 0);
      } catch (const VeloxRuntimeError& e) {
        VELOX_MEM_LOG(ERROR)
            << "Failed to allocate initial capacity "
            << succinctBytes(allocatedBytes)
            << " for memory pool: " << scopedParticipant->name() << "\n"
            << e.what();
        freeCapacityLocked(allocatedBytes, arbitrationWaiters);
      }
    }
  }
  for (auto& waiter : arbitrationWaiters) {
    waiter.setValue();
  }
}

void SharedArbitrator::removePool(MemoryPool* pool) {
  VELOX_CHECK_EQ(pool->reservedBytes(), 0);
  const uint64_t freedBytes = shrinkPool(pool, 0);
  VELOX_CHECK_EQ(pool->capacity(), 0);
  freeCapacity(freedBytes);

  std::unique_lock guard{participantLock_};
  const auto ret = participants_.erase(pool->name());
  VELOX_CHECK_EQ(ret, 1);
}

std::vector<ArbitrationCandidate> SharedArbitrator::getCandidates(
    bool freeCapacityOnly) {
  std::vector<ArbitrationCandidate> candidates;
  std::shared_lock guard{participantLock_};
  candidates.reserve(participants_.size());
  for (const auto& entry : participants_) {
    auto candidate = entry.second->lock();
    if (!candidate.has_value()) {
      continue;
    }
    candidates.push_back({std::move(candidate.value()), freeCapacityOnly});
  }
  return candidates;
}

void SharedArbitrator::sortCandidatesByReclaimableFreeCapacity(
    std::vector<ArbitrationCandidate>& candidates) {
  std::sort(
      candidates.begin(),
      candidates.end(),
      [&](const ArbitrationCandidate& lhs, const ArbitrationCandidate& rhs) {
        return lhs.reclaimableFreeCapacity > rhs.reclaimableFreeCapacity;
      });
  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::sortCandidatesByReclaimableFreeCapacity",
      &candidates);
}

void SharedArbitrator::sortCandidatesByReclaimableUsedCapacity(
    std::vector<ArbitrationCandidate>& candidates) {
  std::sort(
      candidates.begin(),
      candidates.end(),
      [](const ArbitrationCandidate& lhs, const ArbitrationCandidate& rhs) {
        return lhs.reclaimableUsedCapacity > rhs.reclaimableUsedCapacity;
      });

  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::sortCandidatesByReclaimableUsedCapacity",
      &candidates);
}

std::optional<ArbitrationCandidate> SharedArbitrator::findAbortCandidate(
    bool force) {
  const auto candidates = getCandidates();
  if (candidates.empty()) {
    return std::nullopt;
  }

  for (uint64_t capacityLimit : globalArbitrationAbortCapacityLimits_) {
    int32_t candidateIdx{-1};
    for (int32_t i = 0; i < candidates.size(); ++i) {
      if (candidates[i].participant->aborted()) {
        continue;
      }
      if (candidates[i].currentCapacity < capacityLimit ||
          candidates[i].currentCapacity == 0) {
        continue;
      }
      if (candidateIdx == -1) {
        candidateIdx = i;
        continue;
      }
      // With the same capacity size bucket, we favor the old participant to let
      // long running query proceed first.
      if (candidates[candidateIdx].participant->id() <
          candidates[i].participant->id()) {
        candidateIdx = i;
      }
    }
    if (candidateIdx != -1) {
      return candidates[candidateIdx];
    }
  }

  if (!force) {
    VELOX_MEM_LOG(WARNING) << "Can't find an eligible abort victim";
    return std::nullopt;
  }

  // Can't find an eligible abort candidate and then return the youngest
  // candidate which has the largest participant id.
  int32_t candidateIdx{-1};
  for (auto i = 0; i < candidates.size(); ++i) {
    if (candidateIdx == -1) {
      candidateIdx = i;
    } else if (
        candidates[i].participant->id() >
        candidates[candidateIdx].participant->id()) {
      candidateIdx = i;
    }
  }
  VELOX_CHECK_NE(candidateIdx, -1);
  VELOX_MEM_LOG(WARNING)
      << "Can't find an eligible abort victim and force to abort the youngest participant "
      << candidates[candidateIdx].participant->name();
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

uint64_t SharedArbitrator::allocateCapacity(
    uint64_t participantId,
    uint64_t requestBytes,
    uint64_t maxAllocateBytes,
    uint64_t minAllocateBytes) {
  std::lock_guard<std::mutex> l(stateLock_);
  return allocateCapacityLocked(
      participantId, requestBytes, maxAllocateBytes, minAllocateBytes);
}

uint64_t SharedArbitrator::allocateCapacityLocked(
    uint64_t participantId,
    uint64_t requestBytes,
    uint64_t maxAllocateBytes,
    uint64_t minAllocateBytes) {
  VELOX_CHECK_LE(requestBytes, maxAllocateBytes);

  if (FOLLY_UNLIKELY(!globalArbitrationWaiters_.empty())) {
    if ((participantId > globalArbitrationWaiters_.begin()->first) &&
        (requestBytes > minAllocateBytes)) {
      return 0;
    }
    maxAllocateBytes = std::max(requestBytes, minAllocateBytes);
  }

  const uint64_t nonReservedBytes =
      std::min<uint64_t>(freeNonReservedCapacity_, maxAllocateBytes);
  if (nonReservedBytes >= maxAllocateBytes) {
    freeNonReservedCapacity_ -= nonReservedBytes;
    return nonReservedBytes;
  }

  uint64_t reservedBytes{0};
  if (nonReservedBytes < minAllocateBytes) {
    const uint64_t freeReservedCapacity = freeReservedCapacity_;
    reservedBytes =
        std::min(minAllocateBytes - nonReservedBytes, freeReservedCapacity);
  }
  if (FOLLY_UNLIKELY(nonReservedBytes + reservedBytes < requestBytes)) {
    return 0;
  }

  freeNonReservedCapacity_ -= nonReservedBytes;
  freeReservedCapacity_ -= reservedBytes;
  return nonReservedBytes + reservedBytes;
}

uint64_t SharedArbitrator::shrinkCapacity(
    MemoryPool* pool,
    uint64_t /*unused*/) {
  VELOX_CHECK(pool->isRoot());
  auto participant = getParticipant(pool->name());
  VELOX_CHECK(participant.has_value());
  return shrink(participant.value(), /*reclaimAll=*/true);
}

uint64_t SharedArbitrator::shrinkCapacity(
    uint64_t requestBytes,
    bool allowSpill,
    bool allowAbort) {
  const uint64_t targetBytes = requestBytes == 0 ? capacity_ : requestBytes;
  ScopedMemoryArbitrationContext abitrationCtx{};
  const uint64_t startTimeMs = getCurrentTimeMs();

  uint64_t totalReclaimedBytes{0};
  if (allowSpill) {
    totalReclaimedBytes += reclaimUsedMemoryBySpill(targetBytes);
  }

  if ((totalReclaimedBytes < targetBytes) && allowAbort) {
    for (;;) {
      const uint64_t reclaimedBytes = reclaimUsedMemoryByAbort(/*force=*/false);
      if (reclaimedBytes == 0) {
        break;
      }
      totalReclaimedBytes += reclaimedBytes;
      if (totalReclaimedBytes >= targetBytes) {
        break;
      }
    }
  }

  const uint64_t reclaimTimeMs = getCurrentTimeMs() - startTimeMs;
  VELOX_MEM_LOG(INFO) << "External shrink reclaimed "
                      << succinctBytes(totalReclaimedBytes) << ", spent "
                      << succinctMillis(reclaimTimeMs) << ", spill "
                      << (allowSpill ? "allowed" : "not allowed") << ", abort "
                      << (allowSpill ? "allowed" : "not allowed");
  updateGlobalArbitrationStats(reclaimTimeMs, totalReclaimedBytes);
  return totalReclaimedBytes;
}

ArbitrationOperation SharedArbitrator::createArbitrationOperation(
    MemoryPool* pool,
    uint64_t requestBytes) {
  VELOX_CHECK_NOT_NULL(pool);
  VELOX_CHECK(pool->isRoot());

  auto participant = getParticipant(pool->name());
  VELOX_CHECK(participant.has_value());
  return ArbitrationOperation(
      std::move(participant.value()), requestBytes, maxArbitrationTimeMs_);
}

bool SharedArbitrator::growCapacity(MemoryPool* pool, uint64_t requestBytes) {
  VELOX_CHECK(pool->isRoot());
  auto op = createArbitrationOperation(pool, requestBytes);
  ScopedArbitration scopedArbitration(this, &op);

  try {
    const bool ret = growCapacity(op);
    if (!ret) {
      updateArbitrationFailureStats();
    }
    return ret;
  } catch (const std::exception&) {
    updateArbitrationFailureStats();
    std::rethrow_exception(std::current_exception());
  }
}

bool SharedArbitrator::growCapacity(ArbitrationOperation& op) {
  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::growCapacity", this);
  checkIfAborted(op);
  checkIfTimeout(op);

  RETURN_IF_TRUE(maybeGrowFromSelf(op));

  if (!ensureCapacity(op)) {
    VELOX_MEM_LOG(ERROR) << "Can't grow " << op.participant()->name()
                         << " capacity with "
                         << succinctBytes(op.requestBytes())
                         << " which exceeds its max capacity "
                         << succinctBytes(op.participant()->maxCapacity())
                         << ", current capacity "
                         << succinctBytes(op.participant()->capacity());
    return false;
  }
  checkIfAborted(op);
  checkIfTimeout(op);

  RETURN_IF_TRUE(maybeGrowFromSelf(op));

  op.setGrowTargets();
  RETURN_IF_TRUE(growWithFreeCapacity(op));

  reclaimUnusedCapacity();
  RETURN_IF_TRUE(growWithFreeCapacity(op));

  if (!globalArbitrationEnabled_ &&
      op.participant()->reclaimableUsedCapacity() >=
          participantConfig_.minReclaimBytes) {
    // NOTE: if global memory arbitration is not enabled, we will try to reclaim
    // from the participant itself before failing this operation.
    reclaim(
        op.participant(),
        op.requestBytes(),
        op.timeoutMs(),
        /*localArbitration=*/true);
    checkIfAborted(op);
    RETURN_IF_TRUE(maybeGrowFromSelf(op));
    return growWithFreeCapacity(op);
  }
  return startAndWaitGlobalArbitration(op);
}

bool SharedArbitrator::startAndWaitGlobalArbitration(ArbitrationOperation& op) {
  VELOX_CHECK(globalArbitrationEnabled_);
  checkIfTimeout(op);

  std::unique_ptr<ArbitrationWait> arbitrationWait;
  ContinueFuture arbitrationWaitFuture{ContinueFuture::makeEmpty()};
  uint64_t allocatedBytes{0};
  {
    std::lock_guard<std::mutex> l(stateLock_);
    allocatedBytes = allocateCapacityLocked(
        op.participant()->id(),
        op.requestBytes(),
        op.maxGrowBytes(),
        op.minGrowBytes());
    if (allocatedBytes > 0) {
      VELOX_CHECK_GE(allocatedBytes, op.requestBytes());
    } else {
      arbitrationWait = std::make_unique<ArbitrationWait>(
          &op,
          ContinuePromise{fmt::format(
              "{} wait for memory arbitration with {} request bytes",
              op.participant()->name(),
              succinctBytes(op.requestBytes()))});
      arbitrationWaitFuture = arbitrationWait->resumePromise.getSemiFuture();
      globalArbitrationWaiters_.emplace(
          op.participant()->id(), arbitrationWait.get());
    }
  }

  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::startAndWaitGlobalArbitration",
      this);

  if (arbitrationWaitFuture.valid()) {
    VELOX_CHECK_NOT_NULL(arbitrationWait);
    op.recordGlobalArbitrationStartTime();
    wakeupGlobalArbitrationThread();

    const bool timeout = !std::move(arbitrationWaitFuture)
                              .wait(std::chrono::milliseconds(op.timeoutMs()));
    if (timeout) {
      VELOX_MEM_LOG(ERROR)
          << op.participant()->name()
          << " wait for memory arbitration timed out after running "
          << succinctMillis(op.executionTimeMs());
      removeGlobalArbitrationWaiter(op.participant()->id());
    }

    allocatedBytes = arbitrationWait->allocatedBytes;
    if (allocatedBytes == 0) {
      checkIfAborted(op);
      checkIfTimeout(op);
      return false;
    }
  }
  VELOX_CHECK_GE(allocatedBytes, op.requestBytes());
  CHECKED_GROW(op.participant(), allocatedBytes, op.requestBytes());
  return true;
}

void SharedArbitrator::updateGlobalArbitrationStats(
    uint64_t arbitrationTimeMs,
    uint64_t arbitrationBytes) {
  globalArbitrationTimeMs_ += arbitrationTimeMs;
  ++globalArbitrationRuns_;
  globalArbitrationBytes_ += arbitrationBytes;
  RECORD_METRIC_VALUE(kMetricArbitratorGlobalArbitrationCount);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricArbitratorGlobalArbitrationBytes, arbitrationBytes);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricArbitratorGlobalArbitrationTimeMs, arbitrationTimeMs);
}

void SharedArbitrator::globalArbitrationMain() {
  VELOX_MEM_LOG(INFO) << "Global arbitration controller started";
  while (true) {
    {
      std::unique_lock l(stateLock_);
      globalArbitrationThreadCv_.wait(l, [&] {
        return globalArbitrationStop_ || !globalArbitrationWaiters_.empty();
      });
      if (globalArbitrationStop_) {
        VELOX_CHECK(globalArbitrationWaiters_.empty());
        break;
      }
    }
    GlobalArbitrationSection section{this};
    runGlobalArbitration();
  }
  VELOX_MEM_LOG(INFO) << "Global arbitration controller stopped";
}

void SharedArbitrator::runGlobalArbitration() {
  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::runGlobalArbitration", this);

  const uint64_t startTimeMs = getCurrentTimeMs();
  uint64_t totalReclaimedBytes{0};
  bool reclaimByAbort{false};
  uint64_t reclaimedBytes{0};
  std::unordered_set<uint64_t> reclaimedParticipants;
  std::unordered_set<uint64_t> failedParticipants;
  bool allParticipantsReclaimed{false};

  size_t round{0};
  for (;; ++round) {
    uint64_t arbitrationTimeUs{0};
    {
      MicrosecondTimer timer(&arbitrationTimeUs);
      const uint64_t targetBytes = getGlobalArbitrationTarget();
      if (targetBytes == 0) {
        break;
      }

      // Check if we need to abort participant to reclaim used memory to
      // accelerate global arbitration.
      //
      // TODO: make the time based condition check configurable.
      reclaimByAbort =
          (getCurrentTimeMs() - startTimeMs) < maxArbitrationTimeMs_ / 2 &&
          (reclaimByAbort || (allParticipantsReclaimed && reclaimedBytes == 0));
      if (!reclaimByAbort) {
        reclaimedBytes = reclaimUsedMemoryBySpill(
            targetBytes,
            reclaimedParticipants,
            failedParticipants,
            allParticipantsReclaimed);
      } else {
        reclaimedBytes = reclaimUsedMemoryByAbort(/*force=*/true);
      }
      totalReclaimedBytes += reclaimedBytes;
      reclaimUnusedCapacity();
    }

    updateGlobalArbitrationStats(arbitrationTimeUs / 1'000, reclaimedBytes);
  }
  VELOX_MEM_LOG(INFO) << "Global arbitration reclaimed "
                      << succinctBytes(totalReclaimedBytes) << " "
                      << reclaimedParticipants.size() << " victims, spent "
                      << succinctMillis(getCurrentTimeMs() - startTimeMs)
                      << " with " << round << " rounds";
}

uint64_t SharedArbitrator::getGlobalArbitrationTarget() {
  uint64_t targetBytes{0};
  std::lock_guard<std::mutex> l(stateLock_);
  for (const auto& waiter : globalArbitrationWaiters_) {
    targetBytes += waiter.second->op->maxGrowBytes();
  }
  if (targetBytes == 0) {
    return 0;
  }
  return std::max<uint64_t>(
      capacity_ * globalArbitrationMemoryReclaimPct_ / 100, targetBytes);
}

void SharedArbitrator::getGrowTargets(
    ArbitrationOperation& op,
    uint64_t& maxGrowTarget,
    uint64_t& minGrowTarget) {
  op.participant()->getGrowTargets(
      op.requestBytes(), maxGrowTarget, minGrowTarget);
}

void SharedArbitrator::checkIfAborted(ArbitrationOperation& op) {
  if (op.participant()->aborted()) {
    VELOX_MEM_POOL_ABORTED(
        fmt::format("Memory pool {} aborted", op.participant()->name()));
  }
}

void SharedArbitrator::checkIfTimeout(ArbitrationOperation& op) {
  if (FOLLY_UNLIKELY(op.hasTimeout())) {
    VELOX_MEM_ARBITRATION_TIMEOUT(fmt::format(
        "Memory arbitration timed out on memory pool: {} after running {}",
        op.participant()->name(),
        succinctMillis(op.executionTimeMs())));
  }
}

bool SharedArbitrator::maybeGrowFromSelf(ArbitrationOperation& op) {
  return op.participant()->grow(0, op.requestBytes());
}

bool SharedArbitrator::growWithFreeCapacity(ArbitrationOperation& op) {
  const uint64_t allocatedBytes = allocateCapacity(
      op.participant()->id(),
      op.requestBytes(),
      op.maxGrowBytes(),
      op.minGrowBytes());
  if (allocatedBytes > 0) {
    VELOX_CHECK_GE(allocatedBytes, op.requestBytes());
    CHECKED_GROW(op.participant(), allocatedBytes, op.requestBytes());
    return true;
  }
  return false;
}

std::optional<ScopedArbitrationParticipant> SharedArbitrator::getParticipant(
    const std::string& name) const {
  std::shared_lock guard{participantLock_};
  auto it = participants_.find(name);
  VELOX_CHECK(it != participants_.end(), "Arbitration pool {} not found", name);
  return it->second->lock();
}

bool SharedArbitrator::checkCapacityGrowth(ArbitrationOperation& op) const {
  if (!op.participant()->checkCapacityGrowth(op.requestBytes())) {
    return false;
  }
  return (op.participant()->capacity() + op.requestBytes()) <= capacity_;
}

bool SharedArbitrator::ensureCapacity(ArbitrationOperation& op) {
  if ((op.requestBytes() > capacity_) ||
      (op.requestBytes() > op.participant()->maxCapacity())) {
    return false;
  }

  RETURN_IF_TRUE(checkCapacityGrowth(op));

  shrink(op.participant(), /*reclaimAll=*/true);

  RETURN_IF_TRUE(checkCapacityGrowth(op));

  reclaim(
      op.participant(),
      op.requestBytes(),
      op.timeoutMs(),
      /*localArbitration=*/true);
  // Checks if the requestor has been aborted in reclaim above.
  checkIfAborted(op);

  RETURN_IF_TRUE(checkCapacityGrowth(op));

  shrink(op.participant(), /*reclaimAll=*/true);
  return checkCapacityGrowth(op);
}

void SharedArbitrator::checkedGrow(
    const ScopedArbitrationParticipant& participant,
    uint64_t growBytes,
    uint64_t reservationBytes) {
  const auto ret = participant->grow(growBytes, reservationBytes);
  if (!ret) {
    VELOX_FAIL(
        "Failed to grow memory pool {} with {} and commit {} used reservation, memory pool stats:\n{}\n{}",
        participant->name(),
        succinctBytes(growBytes),
        succinctBytes(reservationBytes),
        participant->pool()->toString(),
        participant->pool()->treeMemoryUsage());
  }
}

uint64_t SharedArbitrator::reclaimUnusedCapacity() {
  std::vector<ArbitrationCandidate> candidates =
      getCandidates(/*freeCapacityOnly=*/true);
  uint64_t reclaimedBytes{0};
  SCOPE_EXIT {
    freeCapacity(reclaimedBytes);
  };
  for (const auto& candidate : candidates) {
    if (candidate.reclaimableFreeCapacity == 0) {
      continue;
    }
    reclaimedBytes += candidate.participant->shrink(/*reclaimAll=*/false);
  }
  reclaimedFreeBytes_ += reclaimedBytes;
  return reclaimedBytes;
}

uint64_t SharedArbitrator::reclaimUsedMemoryBySpill(uint64_t targetBytes) {
  std::unordered_set<uint64_t> unusedReclaimedParticipants;
  std::unordered_set<uint64_t> failedParticipants;
  bool unusedAllParticipantsReclaimed;
  return reclaimUsedMemoryBySpill(
      targetBytes,
      unusedReclaimedParticipants,
      failedParticipants,
      unusedAllParticipantsReclaimed);
}

uint64_t SharedArbitrator::reclaimUsedMemoryBySpill(
    uint64_t targetBytes,
    std::unordered_set<uint64_t>& reclaimedParticipants,
    std::unordered_set<uint64_t>& failedParticipants,
    bool& allParticipantsReclaimed) {
  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::reclaimUsedMemoryBySpill",
      this);

  allParticipantsReclaimed = true;
  const uint64_t prevReclaimedBytes = reclaimedUsedBytes_;
  auto candidates = getCandidates();
  sortCandidatesByReclaimableUsedCapacity(candidates);

  std::vector<ArbitrationCandidate> victims;
  victims.reserve(candidates.size());
  uint64_t bytesToReclaim{0};
  for (auto& candidate : candidates) {
    if (candidate.reclaimableUsedCapacity <
        participantConfig_.minReclaimBytes) {
      break;
    }
    if (failedParticipants.count(candidate.participant->id()) != 0) {
      VELOX_CHECK_EQ(
          reclaimedParticipants.count(candidate.participant->id()), 1);
      continue;
    }
    if (bytesToReclaim >= targetBytes) {
      if (reclaimedParticipants.count(candidate.participant->id()) == 0) {
        allParticipantsReclaimed = false;
      }
      continue;
    }
    bytesToReclaim += candidate.reclaimableUsedCapacity;
    reclaimedParticipants.insert(candidate.participant->id());
    victims.push_back(std::move(candidate));
  }
  if (victims.empty()) {
    FB_LOG_EVERY_MS(WARNING, 1'000)
        << "No spill victim participant found with global arbitration target: "
        << succinctBytes(targetBytes);
    return 0;
  }

  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricArbitratorGlobalArbitrationNumReclaimVictims, victims.size());

  struct ReclaimResult {
    uint64_t participantId{0};
    uint64_t reclaimedBytes{0};

    explicit ReclaimResult(uint64_t _participantId, uint64_t _reclaimedBytes)
        : participantId(_participantId), reclaimedBytes(_reclaimedBytes) {}
  };
  std::vector<std::shared_ptr<AsyncSource<ReclaimResult>>> reclaimTasks;
  for (auto& victim : victims) {
    reclaimTasks.push_back(
        memory::createAsyncMemoryReclaimTask<ReclaimResult>([this, victim]() {
          const auto participant = victim.participant;
          const uint64_t reclaimedBytes = reclaim(
              participant,
              victim.reclaimableUsedCapacity,
              maxArbitrationTimeMs_,
              /*localArbitration=*/false);
          return std::make_unique<ReclaimResult>(
              participant->id(), reclaimedBytes);
        }));
    if (reclaimTasks.size() > 1) {
      memoryReclaimExecutor_->add(
          [source = reclaimTasks.back()]() { source->prepare(); });
    }
  }

  // NOTE: reclaim task can never fail.
  uint64_t reclaimedBytes{0};
  for (auto& reclaimTask : reclaimTasks) {
    const auto reclaimResult = reclaimTask->move();
    if (reclaimResult->reclaimedBytes == 0) {
      RECORD_METRIC_VALUE(kMetricArbitratorGlobalArbitrationFailedVictimCount);
      VELOX_CHECK_EQ(failedParticipants.count(reclaimResult->participantId), 0);
      failedParticipants.insert(reclaimResult->participantId);
    }
    reclaimedBytes += reclaimResult->reclaimedBytes;
  }
  VELOX_CHECK_LE(prevReclaimedBytes, reclaimedUsedBytes_);
  // NOTE: there might be concurrent local spill or spill triggered by
  // external shrink.
  return std::max(reclaimedBytes, reclaimedUsedBytes_ - prevReclaimedBytes);
}

uint64_t SharedArbitrator::reclaimUsedMemoryByAbort(bool force) {
  TestValue::adjust(
      "facebook::velox::memory::SharedArbitrator::reclaimUsedMemoryByAbort",
      this);
  const auto victimOpt = findAbortCandidate(force);
  if (!victimOpt.has_value()) {
    return 0;
  }
  const auto& victim = victimOpt.value();
  try {
    VELOX_MEM_POOL_ABORTED(fmt::format(
        "Memory pool aborted to reclaim used memory, current capacity {}, "
        "memory pool stats:\n{}\n{}",
        succinctBytes(victim.participant->pool()->capacity()),
        victim.participant->pool()->toString(),
        victim.participant->pool()->treeMemoryUsage()));
  } catch (VeloxRuntimeError&) {
    return abort(victim.participant, std::current_exception());
  }
}

uint64_t SharedArbitrator::shrink(
    const ScopedArbitrationParticipant& participant,
    bool reclaimAll) {
  const uint64_t freedBytes = participant->shrink(reclaimAll);
  freeCapacity(freedBytes);
  reclaimedFreeBytes_ += freedBytes;
  return freedBytes;
}

uint64_t SharedArbitrator::reclaim(
    const ScopedArbitrationParticipant& participant,
    uint64_t targetBytes,
    uint64_t timeoutMs,
    bool localArbitration) noexcept {
  uint64_t reclaimTimeUs{0};
  uint64_t reclaimedBytes{0};
  MemoryReclaimer::Stats stats;
  {
    MicrosecondTimer reclaimTimer(&reclaimTimeUs);
    reclaimedBytes = participant->reclaim(targetBytes, timeoutMs, stats);
  }
  // NOTE: if memory reclaim fails, then the participant is also aborted. If
  // it happens, we shall first fail the arbitration operation from the
  // aborted participant before returning the freed capacity.
  if (participant->aborted()) {
    removeGlobalArbitrationWaiter(participant->id());
  }
  freeCapacity(reclaimedBytes);

  updateMemoryReclaimStats(
      reclaimedBytes, reclaimTimeUs / 1'000, localArbitration, stats);
  VELOX_MEM_LOG(INFO) << "Reclaimed from memory pool " << participant->name()
                      << " with target of " << succinctBytes(targetBytes)
                      << ", reclaimed " << succinctBytes(reclaimedBytes)
                      << ", spent " << succinctMicros(reclaimTimeUs)
                      << ", local arbitration: " << localArbitration
                      << " stats " << succinctBytes(stats.reclaimedBytes)
                      << " numNonReclaimableAttempts "
                      << stats.numNonReclaimableAttempts;
  if (reclaimedBytes == 0) {
    FB_LOG_EVERY_MS(WARNING, 1'000) << fmt::format(
        "Nothing reclaimed from memory pool {} with reclaim target {},  memory pool stats:\n{}\n{}",
        participant->name(),
        succinctBytes(targetBytes),
        participant->pool()->toString(),
        participant->pool()->treeMemoryUsage());
  }
  return reclaimedBytes;
}

void SharedArbitrator::updateMemoryReclaimStats(
    uint64_t reclaimedBytes,
    uint64_t reclaimTimeMs,
    bool localArbitration,
    const MemoryReclaimer::Stats& stats) {
  if (localArbitration) {
    incrementLocalArbitrationCount();
  }
  reclaimedUsedBytes_ += reclaimedBytes;
  numNonReclaimableAttempts_ += stats.numNonReclaimableAttempts;
  RECORD_METRIC_VALUE(kMetricQueryMemoryReclaimCount);
  RECORD_HISTOGRAM_METRIC_VALUE(kMetricQueryMemoryReclaimTimeMs, reclaimTimeMs);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricQueryMemoryReclaimedBytes, reclaimedBytes);
}

uint64_t SharedArbitrator::abort(
    const ScopedArbitrationParticipant& participant,
    const std::exception_ptr& error) {
  RECORD_METRIC_VALUE(kMetricArbitratorAbortedCount);
  ++numAborted_;
  const uint64_t freedBytes = participant->abort(error);
  // NOTE: no matter memory pool abort throws or not, it should have been
  // marked as aborted to prevent any new memory arbitration triggered from
  // the aborted memory pool.
  VELOX_CHECK(participant->aborted());
  reclaimedUsedBytes_ += freedBytes;
  removeGlobalArbitrationWaiter(participant->id());
  freeCapacity(freedBytes);
  return freedBytes;
}

void SharedArbitrator::freeCapacity(uint64_t bytes) {
  if (FOLLY_UNLIKELY(bytes == 0)) {
    return;
  }
  std::vector<ContinuePromise> resumes;
  {
    std::lock_guard<std::mutex> l(stateLock_);
    freeCapacityLocked(bytes, resumes);
  }
  for (auto& resume : resumes) {
    resume.setValue();
  }
}

void SharedArbitrator::freeCapacityLocked(
    uint64_t bytes,
    std::vector<ContinuePromise>& resumes) {
  freeReservedCapacityLocked(bytes);
  freeNonReservedCapacity_ += bytes;
  if (FOLLY_UNLIKELY(
          freeNonReservedCapacity_ + freeReservedCapacity_ > capacity_)) {
    VELOX_FAIL(
        "Free capacity {}/{} is larger than the max capacity {}, {}",
        succinctBytes(freeNonReservedCapacity_),
        succinctBytes(freeReservedCapacity_),
        succinctBytes(capacity_));
  }
  resumeGlobalArbitrationWaitersLocked(resumes);
}

void SharedArbitrator::resumeGlobalArbitrationWaitersLocked(
    std::vector<ContinuePromise>& resumes) {
  auto it = globalArbitrationWaiters_.begin();
  while (it != globalArbitrationWaiters_.end()) {
    auto* op = it->second->op;
    const uint64_t allocatedBytes = allocateCapacityLocked(
        op->participant()->id(),
        op->requestBytes(),
        op->maxGrowBytes(),
        op->minGrowBytes());
    if (allocatedBytes == 0) {
      break;
    }
    VELOX_CHECK_GE(allocatedBytes, op->requestBytes());
    VELOX_CHECK_EQ(it->second->allocatedBytes, 0);
    it->second->allocatedBytes = allocatedBytes;
    resumes.push_back(std::move(it->second->resumePromise));
    it = globalArbitrationWaiters_.erase(it);
  }
}

void SharedArbitrator::removeGlobalArbitrationWaiter(uint64_t id) {
  ContinuePromise resume = ContinuePromise::makeEmpty();
  {
    std::lock_guard<std::mutex> l(stateLock_);
    auto it = globalArbitrationWaiters_.find(id);
    if (it != globalArbitrationWaiters_.end()) {
      VELOX_CHECK_EQ(it->second->allocatedBytes, 0);
      resume = std::move(it->second->resumePromise);
      globalArbitrationWaiters_.erase(it);
    }
  }
  if (resume.valid()) {
    resume.setValue();
  }
}

void SharedArbitrator::freeReservedCapacityLocked(uint64_t& bytes) {
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
  stats.numRunning = numRunning_;
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
  return fmt::format(
      "ARBITRATOR[{} CAPACITY[{}] {}]",
      kind_,
      succinctBytes(capacity_),
      statsLocked().toString());
}

SharedArbitrator::ScopedArbitration::ScopedArbitration(
    SharedArbitrator* arbitrator,
    ArbitrationOperation* operation)
    : arbitrator_(arbitrator),
      operation_(operation),
      arbitrationCtx_(operation->participant()->pool()),
      startTime_(std::chrono::steady_clock::now()) {
  VELOX_CHECK_NOT_NULL(arbitrator_);
  VELOX_CHECK_NOT_NULL(operation_);
  if (arbitrator_->arbitrationStateCheckCb_ != nullptr) {
    arbitrator_->arbitrationStateCheckCb_(*operation_->participant()->pool());
  }
  arbitrator_->startArbitration(operation_);
}

SharedArbitrator::ScopedArbitration::~ScopedArbitration() {
  arbitrator_->finishArbitration(operation_);
}

SharedArbitrator::GlobalArbitrationSection::GlobalArbitrationSection(
    SharedArbitrator* arbitrator)
    : arbitrator_(arbitrator) {
  VELOX_CHECK_NOT_NULL(arbitrator_);
  VELOX_CHECK(!arbitrator_->globalArbitrationRunning_);
  arbitrator_->globalArbitrationRunning_ = true;
}

SharedArbitrator::GlobalArbitrationSection::~GlobalArbitrationSection() {
  VELOX_CHECK(arbitrator_->globalArbitrationRunning_);
  arbitrator_->globalArbitrationRunning_ = false;
  ;
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

void SharedArbitrator::incrementGlobalArbitrationWaitCount() {
  RECORD_METRIC_VALUE(kMetricArbitratorGlobalArbitrationWaitCount);
  addThreadLocalRuntimeStat(
      kGlobalArbitrationWaitCount,
      RuntimeCounter(1, RuntimeCounter::Unit::kNone));
}

void SharedArbitrator::incrementLocalArbitrationCount() {
  RECORD_METRIC_VALUE(kMetricArbitratorLocalArbitrationCount);
  addThreadLocalRuntimeStat(
      kLocalArbitrationCount, RuntimeCounter(1, RuntimeCounter::Unit::kNone));
}
} // namespace facebook::velox::memory
