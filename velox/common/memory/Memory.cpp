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

#include "velox/common/memory/Memory.h"

DECLARE_bool(velox_memory_leak_check_enabled);

namespace facebook::velox::memory {
namespace {
#define VELOX_MEM_MANAGER_CAP_EXCEEDED(cap)                         \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemCapExceeded.c_str(),       \
      /* isRetriable */ true,                                       \
      "Exceeded memory manager cap of {} MB",                       \
      (cap) / 1024 / 1024);

constexpr folly::StringPiece kDefaultRootName{"__default_root__"};
constexpr folly::StringPiece kDefaultLeafName("__default_leaf__");
} // namespace

MemoryManager::MemoryManager(const Options& options)
    : allocator_{options.allocator->shared_from_this()},
      memoryQuota_{options.capacity},
      alignment_(std::max(MemoryAllocator::kMinAlignment, options.alignment)),
      poolDestructionCb_([&](MemoryPool* pool) { dropPool(pool); }),
      defaultRoot_{std::make_shared<MemoryPoolImpl>(
          this,
          kDefaultRootName.str(),
          MemoryPool::Kind::kAggregate,
          nullptr,
          nullptr,
          // NOTE: the default root memory pool has no quota limit, and it is
          // used for system usage in production such as disk spilling.
          MemoryPool::Options{alignment_, kMaxMemory})},
      deprecatedDefaultLeafPool_(defaultRoot_->addChild(
          kDefaultLeafName.str(),
          MemoryPool::Kind::kLeaf)) {
  VELOX_CHECK_NOT_NULL(allocator_);
  VELOX_USER_CHECK_GE(memoryQuota_, 0);
  MemoryAllocator::alignmentCheck(0, alignment_);
}

MemoryManager::~MemoryManager() {
  VELOX_CHECK_EQ(
      numPools(),
      0,
      "There are {} unexpected alive memory pools allocated by user on memory manager destruction:\n{}",
      numPools(),
      toString());

  if (FLAGS_velox_memory_leak_check_enabled) {
    const auto currentBytes = getTotalBytes();
    VELOX_CHECK_EQ(
        currentBytes,
        0,
        "Leaked total memory of {}",
        succinctBytes(currentBytes));
  }
}

int64_t MemoryManager::getMemoryQuota() const {
  return memoryQuota_;
}

uint16_t MemoryManager::alignment() const {
  return alignment_;
}

std::shared_ptr<MemoryPool> MemoryManager::getPool(
    const std::string& name,
    MemoryPool::Kind kind,
    int64_t maxBytes,
    bool trackUsage) {
  std::string poolName = name;
  if (poolName.empty()) {
    static std::atomic<int64_t> poolId{0};
    poolName =
        fmt::format("default_{}_{}", MemoryPool::kindString(kind), poolId++);
  }
  if (kind == MemoryPool::Kind::kLeaf) {
    return defaultRoot_->addChild(poolName, kind);
  }

  MemoryPool::Options options;
  options.alignment = alignment_;
  options.capacity = maxBytes;
  options.trackUsage = trackUsage;
  auto pool = std::make_shared<MemoryPoolImpl>(
      this,
      poolName,
      MemoryPool::Kind::kAggregate,
      nullptr,
      poolDestructionCb_,
      options);
  folly::SharedMutex::WriteHolder guard{mutex_};
  pools_.push_back(pool.get());
  return pool;
}

void MemoryManager::dropPool(MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(pool);
  folly::SharedMutex::WriteHolder guard{mutex_};
  auto it = pools_.begin();
  while (it != pools_.end()) {
    if (*it == pool) {
      pools_.erase(it);
      return;
    }
    ++it;
  }
  VELOX_UNREACHABLE("Memory pool is not found");
}

MemoryPool& MemoryManager::deprecatedGetPool() {
  return *deprecatedDefaultLeafPool_;
}

int64_t MemoryManager::getTotalBytes() const {
  return totalBytes_.load(std::memory_order_relaxed);
}

bool MemoryManager::reserve(int64_t size) {
  return totalBytes_.fetch_add(size, std::memory_order_relaxed) + size <=
      memoryQuota_;
}

void MemoryManager::release(int64_t size) {
  totalBytes_.fetch_sub(size, std::memory_order_relaxed);
}

size_t MemoryManager::numPools() const {
  // Don't count 'deprecatedDefaultLeafPool_' which is a child of
  // 'defaultRoot_'.
  size_t numPools = defaultRoot_->getChildCount() - 1;
  VELOX_CHECK_GE(numPools, 0);
  {
    folly::SharedMutex::ReadHolder guard{mutex_};
    numPools += pools_.size();
  }
  return numPools;
}

MemoryAllocator& MemoryManager::getAllocator() {
  return *allocator_;
}

std::string MemoryManager::toString() const {
  std::stringstream out;
  out << "Memory Manager[limit " << succinctBytes(memoryQuota_) << " alignment "
      << succinctBytes(alignment_) << " usedBytes "
      << succinctBytes(totalBytes_) << " number of pools " << numPools()
      << "\n";
  out << "List of root pools:\n";
  out << "\t" << defaultRoot_->name() << "\n";
  folly::SharedMutex::ReadHolder guard{mutex_};
  for (const auto* pool : pools_) {
    out << "\t" << pool->name() << "\n";
  }
  out << "]";
  return out.str();
}

IMemoryManager& getProcessDefaultMemoryManager() {
  return MemoryManager::getInstance();
}

std::shared_ptr<MemoryPool> getDefaultMemoryPool(const std::string& name) {
  auto& memoryManager = getProcessDefaultMemoryManager();
  return memoryManager.getPool(name, MemoryPool::Kind::kLeaf);
}

} // namespace facebook::velox::memory
