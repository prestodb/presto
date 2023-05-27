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

#include "velox/common/memory/Memory.h"
#include "velox/common/memory/SharedArbitrator.h"

namespace facebook::velox::memory {
std::string MemoryArbitrator::kindString(Kind kind) {
  switch (kind) {
    case Kind::kNoOp:
      return "NOOP";
    case Kind::kShared:
      return "SHARED";
    default:
      return fmt::format("UNKNOWN: {}", static_cast<int>(kind));
  }
}

std::ostream& operator<<(
    std::ostream& out,
    const MemoryArbitrator::Kind& kind) {
  out << MemoryArbitrator::kindString(kind);
  return out;
}

std::unique_ptr<MemoryArbitrator> MemoryArbitrator::create(
    const Config& config) {
  switch (config.kind) {
    case Kind::kNoOp:
      return nullptr;
    case Kind::kShared:
      return std::make_unique<SharedArbitrator>(config);
    default:
      VELOX_UNREACHABLE(kindString(config.kind));
  }
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

void MemoryReclaimer::abort(MemoryPool* pool) {
  if (pool->kind() == MemoryPool::Kind::kLeaf) {
    VELOX_UNSUPPORTED(
        "Don't support to abort a leaf memory pool {}", pool->name());
  }
  pool->visitChildren([&](MemoryPool* child) {
    // NOTE: we issue abort request through the child pool's reclaimer directly
    // instead of the child pool as the latter always forwards the abort to its
    // root first.
    auto* reclaimer = child->reclaimer();
    VELOX_CHECK_NOT_NULL(reclaimer);
    reclaimer->abort(child);
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
