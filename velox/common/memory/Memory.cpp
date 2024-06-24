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

#include <atomic>

#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/MmapAllocator.h"

DECLARE_int32(velox_memory_num_shared_leaf_pools);

namespace facebook::velox::memory {
namespace {
constexpr std::string_view kSysRootName{"__sys_root__"};
constexpr std::string_view kSysSharedLeafNamePrefix{"__sys_shared_leaf__"};

struct SingletonState {
  ~SingletonState() {
    delete instance.load(std::memory_order_acquire);
  }

  std::atomic<MemoryManager*> instance{nullptr};
  std::mutex mutex;
};

SingletonState& singletonState() {
  static SingletonState state;
  return state;
}

std::shared_ptr<MemoryAllocator> createAllocator(
    const MemoryManagerOptions& options) {
  if (options.useMmapAllocator) {
    MmapAllocator::Options mmapOptions;
    mmapOptions.capacity = options.allocatorCapacity;
    mmapOptions.largestSizeClass = options.largestSizeClassPages;
    mmapOptions.useMmapArena = options.useMmapArena;
    mmapOptions.mmapArenaCapacityRatio = options.mmapArenaCapacityRatio;
    return std::make_shared<MmapAllocator>(mmapOptions);
  } else {
    return std::make_shared<MallocAllocator>(
        options.allocatorCapacity,
        options.allocationSizeThresholdWithReservation);
  }
}

std::unique_ptr<MemoryArbitrator> createArbitrator(
    const MemoryManagerOptions& options) {
  // TODO: consider to reserve a small amount of memory to compensate for the
  // non-reclaimable cache memory which are pinned by query accesses if enabled.
  return MemoryArbitrator::create(
      {.kind = options.arbitratorKind,
       .capacity =
           std::min(options.arbitratorCapacity, options.allocatorCapacity),
       .reservedCapacity = options.arbitratorReservedCapacity,
       .memoryPoolReservedCapacity = options.memoryPoolReservedCapacity,
       .memoryPoolTransferCapacity = options.memoryPoolTransferCapacity,
       .memoryReclaimWaitMs = options.memoryReclaimWaitMs,
       .globalArbitrationEnabled = options.globalArbitrationEnabled,
       .arbitrationStateCheckCb = options.arbitrationStateCheckCb,
       .checkUsageLeak = options.checkUsageLeak});
}

std::vector<std::shared_ptr<MemoryPool>> createSharedLeafMemoryPools(
    MemoryPool& sysPool) {
  VELOX_CHECK_EQ(sysPool.name(), kSysRootName);
  std::vector<std::shared_ptr<MemoryPool>> leafPools;
  const size_t numSharedPools =
      std::max(1, FLAGS_velox_memory_num_shared_leaf_pools);
  leafPools.reserve(numSharedPools);
  for (size_t i = 0; i < numSharedPools; ++i) {
    leafPools.emplace_back(
        sysPool.addLeafChild(fmt::format("{}{}", kSysSharedLeafNamePrefix, i)));
  }
  return leafPools;
}
} // namespace

MemoryManager::MemoryManager(const MemoryManagerOptions& options)
    : allocator_{createAllocator(options)},
      poolInitCapacity_(options.memoryPoolInitCapacity),
      arbitrator_(createArbitrator(options)),
      alignment_(std::max(MemoryAllocator::kMinAlignment, options.alignment)),
      checkUsageLeak_(options.checkUsageLeak),
      debugEnabled_(options.debugEnabled),
      coreOnAllocationFailureEnabled_(options.coreOnAllocationFailureEnabled),
      poolDestructionCb_([&](MemoryPool* pool) { dropPool(pool); }),
      poolGrowCb_([&](MemoryPool* pool, uint64_t targetBytes) {
        return growPool(pool, targetBytes);
      }),
      sysRoot_{std::make_shared<MemoryPoolImpl>(
          this,
          std::string(kSysRootName),
          MemoryPool::Kind::kAggregate,
          nullptr,
          nullptr,
          nullptr,
          nullptr,
          // NOTE: the default root memory pool has no capacity limit, and it is
          // used for system usage in production such as disk spilling.
          MemoryPool::Options{
              .alignment = alignment_,
              .maxCapacity = kMaxMemory,
              .trackUsage = options.trackDefaultUsage,
              .debugEnabled = options.debugEnabled,
              .coreOnAllocationFailureEnabled =
                  options.coreOnAllocationFailureEnabled})},
      spillPool_{addLeafPool("__sys_spilling__")},
      sharedLeafPools_(createSharedLeafMemoryPools(*sysRoot_)) {
  VELOX_CHECK_NOT_NULL(allocator_);
  VELOX_CHECK_NOT_NULL(arbitrator_);
  VELOX_USER_CHECK_GE(capacity(), 0);
  VELOX_CHECK_GE(allocator_->capacity(), arbitrator_->capacity());
  MemoryAllocator::alignmentCheck(0, alignment_);
  const bool ret = sysRoot_->grow(sysRoot_->maxCapacity(), 0);
  VELOX_CHECK(
      ret,
      "Failed to set max capacity {} for {}",
      succinctBytes(sysRoot_->maxCapacity()),
      sysRoot_->name());
  VELOX_CHECK_EQ(
      sharedLeafPools_.size(),
      std::max(1, FLAGS_velox_memory_num_shared_leaf_pools));
}

MemoryManager::~MemoryManager() {
  if (pools_.size() != 0) {
    const auto errMsg = fmt::format(
        "pools_.size() != 0 ({} vs {}). There are unexpected alive memory "
        "pools allocated by user on memory manager destruction:\n{}",
        pools_.size(),
        0,
        toString(true));
    if (checkUsageLeak_) {
      VELOX_FAIL(errMsg);
    } else {
      LOG(ERROR) << errMsg;
    }
  }
}

// static
MemoryManager& MemoryManager::deprecatedGetInstance(
    const MemoryManagerOptions& options) {
  auto& state = singletonState();
  if (auto* instance = state.instance.load(std::memory_order_acquire)) {
    return *instance;
  }

  std::lock_guard<std::mutex> l(state.mutex);
  auto* instance = state.instance.load(std::memory_order_acquire);
  if (instance != nullptr) {
    return *instance;
  }
  instance = new MemoryManager(options);
  state.instance.store(instance, std::memory_order_release);
  return *instance;
}

// static
void MemoryManager::initialize(const MemoryManagerOptions& options) {
  auto& state = singletonState();
  std::lock_guard<std::mutex> l(state.mutex);
  auto* instance = state.instance.load(std::memory_order_acquire);
  VELOX_CHECK_NULL(
      instance,
      "The memory manager has already been set: {}",
      instance->toString());
  instance = new MemoryManager(options);
  state.instance.store(instance, std::memory_order_release);
}

// static.
MemoryManager* MemoryManager::getInstance() {
  auto* instance = singletonState().instance.load(std::memory_order_acquire);
  VELOX_CHECK_NOT_NULL(instance, "The memory manager is not set");
  return instance;
}

// static.
MemoryManager& MemoryManager::testingSetInstance(
    const MemoryManagerOptions& options) {
  auto& state = singletonState();
  std::lock_guard<std::mutex> l(state.mutex);
  auto* instance = new MemoryManager(options);
  delete state.instance.exchange(instance, std::memory_order_acq_rel);
  return *instance;
}

int64_t MemoryManager::capacity() const {
  return allocator_->capacity();
}

uint16_t MemoryManager::alignment() const {
  return alignment_;
}

std::shared_ptr<MemoryPool> MemoryManager::addRootPool(
    const std::string& name,
    int64_t maxCapacity,
    std::unique_ptr<MemoryReclaimer> reclaimer) {
  std::string poolName = name;
  if (poolName.empty()) {
    static std::atomic<int64_t> poolId{0};
    poolName = fmt::format("default_root_{}", poolId++);
  }

  MemoryPool::Options options;
  options.alignment = alignment_;
  options.maxCapacity = maxCapacity;
  options.trackUsage = true;
  options.debugEnabled = debugEnabled_;
  options.coreOnAllocationFailureEnabled = coreOnAllocationFailureEnabled_;

  std::unique_lock guard{mutex_};
  if (pools_.find(poolName) != pools_.end()) {
    VELOX_FAIL("Duplicate root pool name found: {}", poolName);
  }
  auto pool = std::make_shared<MemoryPoolImpl>(
      this,
      poolName,
      MemoryPool::Kind::kAggregate,
      nullptr,
      std::move(reclaimer),
      poolGrowCb_,
      poolDestructionCb_,
      options);
  pools_.emplace(poolName, pool);
  VELOX_CHECK_EQ(pool->capacity(), 0);
  arbitrator_->growCapacity(
      pool.get(), std::min<uint64_t>(poolInitCapacity_, maxCapacity));
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricMemoryPoolInitialCapacityBytes, pool->capacity());
  return pool;
}

std::shared_ptr<MemoryPool> MemoryManager::addLeafPool(
    const std::string& name,
    bool threadSafe) {
  std::string poolName = name;
  if (poolName.empty()) {
    static std::atomic<int64_t> poolId{0};
    poolName = fmt::format("default_leaf_{}", poolId++);
  }
  return sysRoot_->addLeafChild(poolName, threadSafe, nullptr);
}

bool MemoryManager::growPool(MemoryPool* pool, uint64_t incrementBytes) {
  VELOX_CHECK_NOT_NULL(pool);
  VELOX_CHECK_NE(pool->capacity(), kMaxMemory);
  return arbitrator_->growCapacity(pool, getAlivePools(), incrementBytes);
}

uint64_t MemoryManager::shrinkPools(
    uint64_t targetBytes,
    bool allowSpill,
    bool allowAbort) {
  return arbitrator_->shrinkCapacity(
      getAlivePools(), targetBytes, allowSpill, allowAbort);
}

void MemoryManager::dropPool(MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(pool);
  std::unique_lock guard{mutex_};
  auto it = pools_.find(pool->name());
  if (it == pools_.end()) {
    VELOX_FAIL("The dropped memory pool {} not found", pool->name());
  }
  pools_.erase(it);
  VELOX_DCHECK_EQ(pool->reservedBytes(), 0);
  arbitrator_->shrinkCapacity(pool, 0);
}

MemoryPool& MemoryManager::deprecatedSharedLeafPool() {
  const auto idx = std::hash<std::thread::id>{}(std::this_thread::get_id());
  return *sharedLeafPools_.at(idx % sharedLeafPools_.size());
}

int64_t MemoryManager::getTotalBytes() const {
  return allocator_->totalUsedBytes();
}

size_t MemoryManager::numPools() const {
  size_t numPools = sysRoot_->getChildCount();
  {
    std::shared_lock guard{mutex_};
    numPools += pools_.size() - sharedLeafPools_.size();
  }
  return numPools;
}

MemoryAllocator* MemoryManager::allocator() {
  return allocator_.get();
}

MemoryArbitrator* MemoryManager::arbitrator() {
  return arbitrator_.get();
}

std::string MemoryManager::toString(bool detail) const {
  const int64_t allocatorCapacity = capacity();
  std::stringstream out;
  out << "Memory Manager[capacity "
      << (allocatorCapacity == kMaxMemory ? "UNLIMITED"
                                          : succinctBytes(allocatorCapacity))
      << " alignment " << succinctBytes(alignment_) << " usedBytes "
      << succinctBytes(getTotalBytes()) << " number of pools " << numPools()
      << "\n";
  out << "List of root pools:\n";
  if (detail) {
    out << sysRoot_->treeMemoryUsage(false);
  } else {
    out << "\t" << sysRoot_->name() << "\n";
  }
  std::vector<std::shared_ptr<MemoryPool>> pools = getAlivePools();
  for (const auto& pool : pools) {
    if (detail) {
      out << pool->treeMemoryUsage(false);
    } else {
      out << "\t" << pool->name() << "\n";
    }
    out << "\trefcount " << pool.use_count() << "\n";
  }
  out << allocator_->toString() << "\n";
  out << arbitrator_->toString();
  out << "]";
  return out.str();
}

std::vector<std::shared_ptr<MemoryPool>> MemoryManager::getAlivePools() const {
  std::vector<std::shared_ptr<MemoryPool>> pools;
  std::shared_lock guard{mutex_};
  pools.reserve(pools_.size());
  for (const auto& entry : pools_) {
    auto pool = entry.second.lock();
    if (pool != nullptr) {
      pools.push_back(std::move(pool));
    }
  }
  return pools;
}

void initializeMemoryManager(const MemoryManagerOptions& options) {
  MemoryManager::initialize(options);
}

MemoryManager* memoryManager() {
  return MemoryManager::getInstance();
}

MemoryManager& deprecatedDefaultMemoryManager() {
  return MemoryManager::deprecatedGetInstance();
}

std::shared_ptr<MemoryPool> deprecatedAddDefaultLeafMemoryPool(
    const std::string& name,
    bool threadSafe) {
  auto& memoryManager = deprecatedDefaultMemoryManager();
  return memoryManager.addLeafPool(name, threadSafe);
}

MemoryPool& deprecatedSharedLeafPool() {
  return deprecatedDefaultMemoryManager().deprecatedSharedLeafPool();
}

memory::MemoryPool* spillMemoryPool() {
  return memory::MemoryManager::getInstance()->spillPool();
}

bool isSpillMemoryPool(memory::MemoryPool* pool) {
  return pool == spillMemoryPool();
}
} // namespace facebook::velox::memory
