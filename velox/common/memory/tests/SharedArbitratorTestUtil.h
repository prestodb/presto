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

#include "velox/common/memory/ArbitrationParticipant.h"
#include "velox/common/memory/SharedArbitrator.h"

namespace facebook::velox::memory::test {

class SharedArbitratorTestHelper {
 public:
  explicit SharedArbitratorTestHelper(SharedArbitrator* arbitrator)
      : arbitrator_(arbitrator) {}

  void freeCapacity(uint64_t targetBytes) {
    arbitrator_->freeCapacity(targetBytes);
  }

  size_t numParticipants() {
    std::lock_guard<std::mutex> l(arbitrator_->stateMutex_);
    return arbitrator_->participants_.size();
  }

  ScopedArbitrationParticipant getParticipant(const std::string& name) const {
    return arbitrator_->getParticipant(name).value();
  }

  size_t numGlobalArbitrationWaiters() const {
    std::lock_guard<std::mutex> l(arbitrator_->stateMutex_);
    return arbitrator_->globalArbitrationWaiters_.size();
  }

  bool globalArbitrationRunning() const {
    std::lock_guard<std::mutex> l(arbitrator_->stateMutex_);
    return arbitrator_->globalArbitrationRunning_;
  }

  void waitForGlobalArbitrationToFinish() const {
    while (globalArbitrationRunning()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
    }
  }

  uint64_t maxArbitrationTimeNs() const {
    return arbitrator_->maxArbitrationTimeNs_;
  }

  folly::CPUThreadPoolExecutor* memoryReclaimExecutor() const {
    return arbitrator_->memoryReclaimExecutor_.get();
  }

  std::thread* globalArbitrationController() const {
    return arbitrator_->globalArbitrationController_.get();
  }

  uint64_t globalArbitrationRuns() const {
    return arbitrator_->globalArbitrationRuns_;
  }

  bool hasShutdown() const {
    std::lock_guard<std::mutex> l(arbitrator_->stateMutex_);
    return arbitrator_->hasShutdownLocked();
  }

 private:
  SharedArbitrator* const arbitrator_;
};

class ArbitrationParticipantTestHelper {
 public:
  explicit ArbitrationParticipantTestHelper(ArbitrationParticipant* participant)
      : participant_(participant) {}

  size_t numOps() const {
    std::lock_guard<std::mutex> l(participant_->stateLock_);
    return !!(participant_->runningOp_ != nullptr) +
        participant_->waitOps_.size();
  }

  ArbitrationOperation* runningOp() const {
    std::lock_guard<std::mutex> l(participant_->stateLock_);
    return participant_->runningOp_;
  }

  std::vector<ArbitrationOperation*> waitingOps() const {
    std::vector<ArbitrationOperation*> ops;
    std::lock_guard<std::mutex> l(participant_->stateLock_);
    ops.reserve(participant_->waitOps_.size());
    for (const auto& waitOp : participant_->waitOps_) {
      ops.push_back(waitOp.op);
    }
    return ops;
  }

 private:
  ArbitrationParticipant* const participant_;
};

struct ArbitrationTestStructs {
  ArbitrationParticipant::Config config;
  std::shared_ptr<ArbitrationParticipant> participant{nullptr};
  std::shared_ptr<ArbitrationOperation> operation{nullptr};

  static ArbitrationTestStructs createArbitrationTestStructs(
      const std::shared_ptr<MemoryPool>& pool,
      uint64_t initCapacity = 1024,
      uint64_t minCapacity = 128,
      uint64_t fastExponentialGrowthCapacityLimit = 0,
      double slowCapacityGrowRatio = 0,
      uint64_t minFreeCapacity = 0,
      double minFreeCapacityRatio = 0,
      uint64_t minReclaimBytes = 128,
      double minReclaimPct = 0,
      uint64_t abortCapacityLimit = 512,
      uint64_t requestBytes = 128,
      uint64_t maxArbitrationTimeNs = 1'000'000'000'000UL /* 1'000s */) {
    ArbitrationTestStructs ret{
        .config = ArbitrationParticipant::Config(
            initCapacity,
            minCapacity,
            fastExponentialGrowthCapacityLimit,
            slowCapacityGrowRatio,
            minFreeCapacity,
            minFreeCapacityRatio,
            minReclaimBytes,
            minReclaimPct,
            abortCapacityLimit)};
    ret.participant = ArbitrationParticipant::create(
        folly::Random::rand64(), pool, &ret.config);
    ret.operation = std::make_shared<ArbitrationOperation>(
        ScopedArbitrationParticipant(ret.participant, pool),
        requestBytes,
        maxArbitrationTimeNs);
    return ret;
  }
};

} // namespace facebook::velox::memory::test
