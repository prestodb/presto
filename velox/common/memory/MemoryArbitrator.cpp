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

namespace facebook::velox::memory {
std::string MemoryArbitrator::kindString(Kind kind) {
  switch (kind) {
    case Kind::kFixed:
      return "FIXED";
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
    case Kind::kFixed:
      FOLLY_FALLTHROUGH;
    case Kind::kShared:
      VELOX_UNSUPPORTED(
          "{} arbitrator type not supported yet", kindString(config.kind));
    default:
      VELOX_UNREACHABLE(kindString(config.kind));
  }
}

std::shared_ptr<MemoryReclaimer> MemoryReclaimer::create() {
  return std::shared_ptr<MemoryReclaimer>(new MemoryReclaimer());
}

bool MemoryReclaimer::canReclaim(const MemoryPool& pool) const {
  if (pool.kind() == MemoryPool::Kind::kLeaf) {
    return false;
  }
  bool canReclaim = false;
  pool.visitChildren([&](MemoryPool* pool) {
    if (pool->canReclaim()) {
      canReclaim = true;
      return false;
    }
    return true;
  });
  return canReclaim;
}

uint64_t MemoryReclaimer::reclaimableBytes(const MemoryPool& pool) const {
  if (pool.kind() == MemoryPool::Kind::kLeaf) {
    return 0;
  }
  uint64_t reclaimableBytes{0};
  pool.visitChildren([&](MemoryPool* pool) {
    reclaimableBytes += pool->reclaimableBytes();
    return true;
  });
  return reclaimableBytes;
}

uint64_t MemoryReclaimer::reclaim(MemoryPool* pool, uint64_t targetBytes) {
  if (pool->kind() == MemoryPool::Kind::kLeaf) {
    return 0;
  }
  // TODO: add to sort the child memory pools based on the reclaimable bytes
  // before memory reclamation.
  uint64_t reclaimedBytes{0};
  pool->visitChildren([&](MemoryPool* pool) {
    const auto bytes = pool->reclaim(targetBytes);
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

std::string MemoryArbitrator::Stats::toString() const {
  return fmt::format(
      "STATS[numRequests {} numFailures {} numQueuedRequests {} queueTime {} arbitrationTime {} shrunkMemory {} reclaimedMemory {}]",
      numRequests,
      numFailures,
      numQueuedRequests,
      succinctMicros(queueTimeUs),
      succinctMicros(arbitrationTimeUs),
      succinctBytes(numShrunkBytes),
      succinctBytes(numReclaimedBytes));
}
} // namespace facebook::velox::memory
