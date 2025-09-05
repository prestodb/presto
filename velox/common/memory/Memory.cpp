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
    const MemoryManager::Options& options) {
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
    const MemoryManager::Options& options) {
  // TODO: consider to reserve a small amount of memory to compensate for the
  //  non-reclaimable cache memory which are pinned by query accesses if
  //  enabled.

  return MemoryArbitrator::create(
      {.kind = options.arbitratorKind,
       .capacity =
           std::min(options.arbitratorCapacity, options.allocatorCapacity),
       .arbitrationStateCheckCb = options.arbitrationStateCheckCb,
       .extraConfigs = options.extraArbitratorConfigs});
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

// Used by sys root memory pool for use case that expect a memory reclaimer to
// set like QueryCtx.
class SysMemoryReclaimer : public memory::MemoryReclaimer {
 public:
  static std::unique_ptr<memory::MemoryReclaimer> create() {
    return std::unique_ptr<memory::MemoryReclaimer>(new SysMemoryReclaimer());
  }

  uint64_t reclaim(
      memory::MemoryPool* pool,
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      memory::MemoryReclaimer::Stats& stats) override {
    return 0;
  }

  void enterArbitration() override {}

  void leaveArbitration() noexcept override {}

  int32_t priority() const override {
    return 0;
  }

  bool reclaimableBytes(const MemoryPool& pool, uint64_t& reclaimableBytes)
      const override {
    return false;
  }

  /// Invoked by the memory arbitrator to abort memory 'pool' and the associated
  /// query execution when encounters non-recoverable memory reclaim error or
  /// fails to reclaim enough free capacity. The abort is a synchronous
  /// operation and we expect most of used memory to be freed after the abort
  /// completes. 'error' should be passed in as the direct cause of the
  /// abortion. It will be propagated all the way to task level for accurate
  /// error exposure.
  void abort(MemoryPool* pool, const std::exception_ptr& error) override {
    VELOX_UNSUPPORTED("SysMemoryReclaimer::abort is not supported");
  }

 private:
  SysMemoryReclaimer() : MemoryReclaimer{0} {};
};
} // namespace

MemoryManager::MemoryManager(const MemoryManager::Options& options)
    : allocator_{createAllocator(options)},
      arbitrator_(createArbitrator(options)),
      alignment_(std::max(MemoryAllocator::kMinAlignment, options.alignment)),
      checkUsageLeak_(options.checkUsageLeak),
      coreOnAllocationFailureEnabled_(options.coreOnAllocationFailureEnabled),
      disableMemoryPoolTracking_(options.disableMemoryPoolTracking),
      getPreferredSize_(options.getPreferredSize),
      poolDestructionCb_([&](MemoryPool* pool) { dropPool(pool); }),
      sysRoot_{std::make_shared<MemoryPoolImpl>(
          this,
          std::string(kSysRootName),
          MemoryPool::Kind::kAggregate,
          nullptr,
          nullptr,
          // NOTE: the default root memory pool has no capacity limit, and it is
          // used for system usage in production such as disk spilling.
          MemoryPool::Options{
              .alignment = alignment_,
              .maxCapacity = kMaxMemory,
              .trackUsage = options.trackDefaultUsage,
              .coreOnAllocationFailureEnabled =
                  options.coreOnAllocationFailureEnabled,
              .getPreferredSize = getPreferredSize_})},
      spillPool_{addLeafPool("__sys_spilling__")},
      cachePool_{addLeafPool("__sys_caching__")},
      tracePool_{addLeafPool("__sys_tracing__")},
      sharedLeafPools_(createSharedLeafMemoryPools(*sysRoot_)) {
  sysRoot_->setReclaimer(SysMemoryReclaimer::create());
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
  arbitrator_->shutdown();

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
    const MemoryManager::Options& options) {
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
void MemoryManager::initialize(const MemoryManager::Options& options) {
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
bool MemoryManager::testInstance() {
  auto* instance = singletonState().instance.load(std::memory_order_acquire);
  return instance != nullptr;
}

// static.
MemoryManager& MemoryManager::testingSetInstance(
    const MemoryManager::Options& options) {
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

std::shared_ptr<MemoryPoolImpl> MemoryManager::createRootPool(
    std::string poolName,
    std::unique_ptr<MemoryReclaimer>& reclaimer,
    MemoryPool::Options& options) {
  auto pool = std::make_shared<MemoryPoolImpl>(
      this,
      poolName,
      MemoryPool::Kind::kAggregate,
      nullptr,
      std::move(reclaimer),
      options);
  VELOX_CHECK_EQ(pool->capacity(), 0);
  arbitrator_->addPool(pool);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricMemoryPoolInitialCapacityBytes, pool->capacity());
  return pool;
}

std::shared_ptr<MemoryPool> MemoryManager::addRootPool(
    const std::string& name,
    int64_t maxCapacity,
    std::unique_ptr<MemoryReclaimer> reclaimer,
    const std::optional<MemoryPool::DebugOptions>& poolDebugOpts) {
  std::string poolName = name;
  if (poolName.empty()) {
    static std::atomic<int64_t> poolId{0};
    poolName = fmt::format("default_root_{}", poolId++);
  }

  MemoryPool::Options options;
  options.alignment = alignment_;
  options.maxCapacity = maxCapacity;
  options.trackUsage = true;
  options.coreOnAllocationFailureEnabled = coreOnAllocationFailureEnabled_;
  options.getPreferredSize = getPreferredSize_;
  options.debugOptions = poolDebugOpts;

  auto pool = createRootPool(poolName, reclaimer, options);
  if (!disableMemoryPoolTracking_) {
    try {
      std::unique_lock guard{mutex_};
      if (pools_.find(poolName) != pools_.end()) {
        VELOX_FAIL("Duplicate root pool name found: {}", poolName);
      }
      pools_.emplace(poolName, pool);
    } catch (const VeloxRuntimeError&) {
      arbitrator_->removePool(pool.get());
      throw;
    }
  }
  // NOTE: we need to set destruction callback at the end to avoid potential
  // deadlock or failure because of duplicate memory pool name or unexpected
  // failure to add memory pool to the arbitrator.
  pool->setDestructionCallback(poolDestructionCb_);
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

uint64_t MemoryManager::shrinkPools(
    uint64_t targetBytes,
    bool allowSpill,
    bool allowAbort) {
  return arbitrator_->shrinkCapacity(targetBytes, allowSpill, allowAbort);
}

void MemoryManager::dropPool(MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(pool);
  VELOX_DCHECK_EQ(pool->reservedBytes(), 0);
  arbitrator_->removePool(pool);
  if (disableMemoryPoolTracking_) {
    return;
  }
  std::unique_lock guard{mutex_};
  auto it = pools_.find(pool->name());
  if (it == pools_.end()) {
    VELOX_FAIL("The dropped memory pool {} not found", pool->name());
  }
  pools_.erase(it);
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

void initializeMemoryManager(const MemoryManager::Options& options) {
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

MemoryPool& deprecatedRootPool() {
  return deprecatedDefaultMemoryManager().deprecatedSysRootPool();
}

memory::MemoryPool* spillMemoryPool() {
  return memory::MemoryManager::getInstance()->spillPool();
}

bool isSpillMemoryPool(memory::MemoryPool* pool) {
  return pool == spillMemoryPool();
}

memory::MemoryPool* traceMemoryPool() {
  return memory::MemoryManager::getInstance()->tracePool();
}
} // namespace facebook::velox::memory
