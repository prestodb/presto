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

#include "velox/common/memory/MemoryArbitrator.h"

#include <utility>

#include "velox/common/memory/Memory.h"
#include "velox/common/memory/SharedArbitrator.h"

namespace facebook::velox::memory {

namespace {
class FactoryRegistry {
 public:
  void registerFactory(
      const std::string& kind,
      MemoryArbitrator::Factory factory) {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_USER_CHECK(
        map_.find(kind) == map_.end(),
        "Arbitrator factory for kind {} already registered",
        kind)
    map_[kind] = std::move(factory);
  }

  MemoryArbitrator::Factory& getFactory(const std::string& kind) {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_USER_CHECK(
        map_.find(kind) != map_.end(),
        "Arbitrator factory for kind {} not registered",
        kind)
    return map_[kind];
  }

  bool unregisterFactory(const std::string& kind) {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_USER_CHECK(
        map_.find(kind) != map_.end(),
        "Arbitrator factory for kind {} not registered",
        kind)
    return map_.erase(kind);
  }

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, MemoryArbitrator::Factory> map_;
};

FactoryRegistry& arbitratorFactories() {
  static FactoryRegistry registry;
  return registry;
}

// Used to enforce the fixed query memory isolation across running queries.
// When a memory pool exceeds the fixed capacity limit, the query just
// fails with memory capacity exceeded error without arbitration. This is
// used to match the current memory isolation behavior adopted by
// Prestissimo.
//
// TODO: deprecate this legacy policy with kShared policy for Prestissimo
// later.
class NoopArbitrator : public MemoryArbitrator {
 public:
  explicit NoopArbitrator(const Config& config) : MemoryArbitrator(config) {
    VELOX_CHECK(config.kind.empty());
  }

  std::string kind() override {
    return "NOOP";
  }

  // Noop arbitrator has no memory capacity limit so no operation needed for
  // memory pool capacity reserve.
  void reserveMemory(MemoryPool* pool, uint64_t /*unused*/) override {
    pool->grow(pool->maxCapacity());
  }

  // Noop arbitrator has no memory capacity limit so no operation needed for
  // memory pool capacity release.
  void releaseMemory(MemoryPool* /*unused*/) override {
    // No-op
  }

  // Noop arbitrator has no memory capacity limit so no operation needed for
  // memory pool capacity grow.
  bool growMemory(
      MemoryPool* /*unused*/,
      const std::vector<std::shared_ptr<MemoryPool>>& /*unused*/,
      uint64_t /*unused*/) override {
    return false;
  }

  // Noop arbitrator has no memory capacity limit so no operation needed for
  // memory pool capacity shrink.
  uint64_t shrinkMemory(
      const std::vector<std::shared_ptr<MemoryPool>>& /*unused*/,
      uint64_t /*unused*/) override {
    return 0;
  }

  Stats stats() const override {
    Stats stats;
    stats.maxCapacityBytes = kMaxMemory;
    return stats;
  }

  std::string toString() const override {
    return "NOOP ARBITRATOR";
  }
};

} // namespace

std::unique_ptr<MemoryArbitrator> MemoryArbitrator::create(
    const Config& config) {
  if (config.kind.empty()) {
    // if kind is not set, return noop arbitrator.
    return std::make_unique<NoopArbitrator>(config);
  }
  auto& factory = arbitratorFactories().getFactory(config.kind);
  return factory(config);
}

void MemoryArbitrator::registerFactory(
    const std::string& kind,
    MemoryArbitrator::Factory factory) {
  arbitratorFactories().registerFactory(kind, std::move(factory));
}

void MemoryArbitrator::unregisterFactory(const std::string& kind) {
  arbitratorFactories().unregisterFactory(kind);
}

void MemoryArbitrator::registerAllFactories() {
  SharedArbitrator::registerFactory();
}

void MemoryArbitrator::unregisterAllFactories() {
  SharedArbitrator::unregisterFactory();
}

std::unique_ptr<MemoryReclaimer> MemoryReclaimer::create() {
  return std::unique_ptr<MemoryReclaimer>(new MemoryReclaimer());
}

bool MemoryReclaimer::reclaimableBytes(
    const MemoryPool& pool,
    uint64_t& reclaimableBytes) const {
  reclaimableBytes = 0;
  if (pool.kind() == MemoryPool::Kind::kLeaf) {
    return false;
  }
  bool reclaimable{false};
  pool.visitChildren([&](MemoryPool* pool) {
    uint64_t poolReclaimableBytes{0};
    reclaimable |= pool->reclaimableBytes(poolReclaimableBytes);
    reclaimableBytes += poolReclaimableBytes;
    return true;
  });
  VELOX_CHECK(reclaimable || reclaimableBytes == 0);
  return reclaimable;
}

uint64_t MemoryReclaimer::reclaim(MemoryPool* pool, uint64_t targetBytes) {
  if (pool->kind() == MemoryPool::Kind::kLeaf) {
    return 0;
  }
  // TODO: add to sort the child memory pools based on the reclaimable bytes
  // before memory reclamation.
  uint64_t reclaimedBytes{0};
  pool->visitChildren([&targetBytes, &reclaimedBytes](MemoryPool* child) {
    const auto bytes = child->reclaim(targetBytes);
    reclaimedBytes += bytes;
    if (targetBytes != 0) {
      if (bytes >= targetBytes) {
        return false;
      }
      targetBytes -= bytes;
    }
    return true;
  });
  return reclaimedBytes;
}

void MemoryReclaimer::abort(MemoryPool* pool, const std::exception_ptr& error) {
  if (pool->kind() == MemoryPool::Kind::kLeaf) {
    VELOX_UNSUPPORTED(
        "Don't support to abort a leaf memory pool {}", pool->name());
  }
  pool->visitChildren([&](MemoryPool* child) {
    // NOTE: we issue abort request through the child pool's reclaimer directly
    // instead of the child pool as the latter always forwards the abort to its
    // root first.
    auto* reclaimer = child->reclaimer();
    if (reclaimer != nullptr) {
      reclaimer->abort(child, error);
    }
    return true;
  });
}

std::string MemoryArbitrator::Stats::toString() const {
  return fmt::format(
      "STATS[numRequests {} numAborted {} numFailures {} queueTime {} arbitrationTime {} shrunkMemory {} reclaimedMemory {} maxCapacity {} freeCapacity {}]",
      numRequests,
      numAborted,
      numFailures,
      succinctMicros(queueTimeUs),
      succinctMicros(arbitrationTimeUs),
      succinctBytes(numShrunkBytes),
      succinctBytes(numReclaimedBytes),
      succinctBytes(maxCapacityBytes),
      succinctBytes(freeCapacityBytes));
}
} // namespace facebook::velox::memory
