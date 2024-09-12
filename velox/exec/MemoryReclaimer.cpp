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

#include "velox/exec/MemoryReclaimer.h"

#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {
std::unique_ptr<memory::MemoryReclaimer> MemoryReclaimer::create() {
  return std::unique_ptr<memory::MemoryReclaimer>(new MemoryReclaimer());
}

void MemoryReclaimer::enterArbitration() {
  DriverThreadContext* driverThreadCtx = driverThreadContext();
  if (FOLLY_UNLIKELY(driverThreadCtx == nullptr)) {
    // Skips the driver suspension handling if this memory arbitration
    // request is not issued from a driver thread.
    return;
  }

  Driver* const driver = driverThreadCtx->driverCtx.driver;
  if (driver->task()->enterSuspended(driver->state()) != StopReason::kNone) {
    // There is no need for arbitration if the associated task has already
    // terminated.
    VELOX_FAIL("Terminate detected when entering suspension");
  }
}

void MemoryReclaimer::leaveArbitration() noexcept {
  DriverThreadContext* driverThreadCtx = driverThreadContext();
  if (FOLLY_UNLIKELY(driverThreadCtx == nullptr)) {
    // Skips the driver suspension handling if this memory arbitration
    // request is not issued from a driver thread.
    return;
  }
  Driver* const driver = driverThreadCtx->driverCtx.driver;
  driver->task()->leaveSuspended(driver->state());
}

void MemoryReclaimer::abort(
    memory::MemoryPool* pool,
    const std::exception_ptr& error) {
  if (pool->kind() == memory::MemoryPool::Kind::kLeaf) {
    return;
  }
  pool->visitChildren([&](memory::MemoryPool* child) {
    auto* reclaimer = child->reclaimer();
    if (reclaimer != nullptr) {
      reclaimer->abort(child, error);
    }
    return true;
  });
}

/*static*/ std::unique_ptr<memory::MemoryReclaimer>
ParallelMemoryReclaimer::create(folly::Executor* executor) {
  return std::unique_ptr<memory::MemoryReclaimer>(
      new ParallelMemoryReclaimer(executor));
}

ParallelMemoryReclaimer::ParallelMemoryReclaimer(folly::Executor* executor)
    : executor_(executor) {}

uint64_t ParallelMemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    Stats& stats) {
  if (executor_ == nullptr) {
    return memory::MemoryReclaimer::reclaim(
        pool, targetBytes, maxWaitMs, stats);
  }

  // Sort the child pools based on their reserved memory and reclaim from the
  // child pool with most reservation first.
  struct Candidate {
    std::shared_ptr<memory::MemoryPool> pool;
    int64_t reclaimableBytes;
  };
  std::vector<Candidate> candidates;
  {
    std::shared_lock guard{pool->poolMutex_};
    candidates.reserve(pool->children_.size());
    for (auto& entry : pool->children_) {
      auto child = entry.second.lock();
      if (child != nullptr) {
        const int64_t reclaimableBytes = child->reclaimableBytes().value_or(0);
        candidates.push_back(Candidate{std::move(child), reclaimableBytes});
      }
    }
  }
  struct ReclaimResult {
    const uint64_t reclaimedBytes{0};
    const Stats stats;
    const std::exception_ptr error{nullptr};

    explicit ReclaimResult(std::exception_ptr _error)
        : reclaimedBytes(0), error(std::move(_error)) {}

    ReclaimResult(uint64_t _reclaimedBytes, Stats&& _stats)
        : reclaimedBytes(_reclaimedBytes),
          stats(std::move(_stats)),
          error(nullptr) {}
  };

  std::vector<std::shared_ptr<AsyncSource<ReclaimResult>>> reclaimTasks;
  for (const auto& candidate : candidates) {
    if (candidate.reclaimableBytes == 0) {
      continue;
    }
    reclaimTasks.push_back(memory::createAsyncMemoryReclaimTask<ReclaimResult>(
        [&, reclaimPool = candidate.pool]() {
          try {
            Stats reclaimStats;
            const auto bytes =
                reclaimPool->reclaim(targetBytes, maxWaitMs, reclaimStats);
            return std::make_unique<ReclaimResult>(
                bytes, std::move(reclaimStats));
          } catch (const std::exception& e) {
            VELOX_MEM_LOG(ERROR) << "Reclaim from memory pool " << pool->name()
                                 << " failed: " << e.what();
            // The exception is captured and thrown by the caller.
            return std::make_unique<ReclaimResult>(std::current_exception());
          }
        }));
    if (reclaimTasks.size() > 1) {
      executor_->add([source = reclaimTasks.back()]() { source->prepare(); });
    }
  }

  auto syncGuard = folly::makeGuard([&]() {
    for (auto& reclaimTask : reclaimTasks) {
      // We consume the result for the pending tasks. This is a cleanup in the
      // guard and must not throw. The first error is already captured before
      // this runs.
      try {
        reclaimTask->move();
      } catch (const std::exception&) {
      }
    }
  });

  uint64_t reclaimedBytes{0};
  for (auto& reclaimTask : reclaimTasks) {
    const auto result = reclaimTask->move();
    if (result->error) {
      std::rethrow_exception(result->error);
    }
    stats += result->stats;
    reclaimedBytes += result->reclaimedBytes;
  }
  return reclaimedBytes;
}

void memoryArbitrationStateCheck(memory::MemoryPool& pool) {
  const auto* driverThreadCtx = driverThreadContext();
  if (driverThreadCtx != nullptr) {
    Driver* driver = driverThreadCtx->driverCtx.driver;
    if (!driver->state().suspended()) {
      VELOX_FAIL(
          "Driver thread is not suspended under memory arbitration processing: {}, request memory pool: {}",
          driver->toString(),
          pool.name());
    }
  }
}
} // namespace facebook::velox::exec
