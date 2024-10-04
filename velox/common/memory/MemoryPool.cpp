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

#include "velox/common/memory/MemoryPool.h"

#include <signal.h>
#include <set>

#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"

#include <re2/re2.h>

DEFINE_bool(
    velox_memory_pool_capacity_transfer_across_tasks,
    false,
    "Whether allow to memory capacity transfer between memory pools from different tasks, which might happen in use case like Spark-Gluten");

DECLARE_bool(velox_suppress_memory_capacity_exceeding_error_message);

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::memory {
namespace {
// Check if memory operation is allowed and increment the named stats.
#define CHECK_AND_INC_MEM_OP_STATS(stats)                             \
  do {                                                                \
    if (FOLLY_UNLIKELY(kind_ != Kind::kLeaf)) {                       \
      VELOX_FAIL(                                                     \
          "Memory operation is only allowed on leaf memory pool: {}", \
          toString());                                                \
    }                                                                 \
    ++num##stats##_;                                                  \
  } while (0)

// Check if memory operation is allowed and increment the named stats.
#define INC_MEM_OP_STATS(stats) ++num##stats##_;

// Check if a memory pool management operation is allowed.
#define CHECK_POOL_MANAGEMENT_OP(opName)                                             \
  do {                                                                               \
    if (FOLLY_UNLIKELY(kind_ != Kind::kAggregate)) {                                 \
      VELOX_FAIL(                                                                    \
          "Memory pool {} operation is only allowed on aggregation memory pool: {}", \
          #opName,                                                                   \
          toString());                                                               \
    }                                                                                \
  } while (0)

// Collect the memory usage from memory pool for memory capacity exceeded error
// message generation.
struct MemoryUsage {
  std::string name;
  uint64_t currentUsage;
  uint64_t reservedUsage;
  uint64_t peakUsage;

  bool operator>(const MemoryUsage& other) const {
    return std::tie(reservedUsage, currentUsage, peakUsage, name) >
        std::tie(
               other.reservedUsage,
               other.currentUsage,
               other.peakUsage,
               other.name);
  }

  std::string toString() const {
    return fmt::format(
        "{} usage {} reserved {} peak {}",
        name,
        succinctBytes(currentUsage),
        succinctBytes(reservedUsage),
        succinctBytes(peakUsage));
  }
};

struct MemoryUsageComp {
  bool operator()(const MemoryUsage& lhs, const MemoryUsage& rhs) const {
    return lhs > rhs;
  }
};
using MemoryUsageHeap =
    std::priority_queue<MemoryUsage, std::vector<MemoryUsage>, MemoryUsageComp>;

static constexpr size_t kCapMessageIndentSize = 4;

std::vector<MemoryUsage> sortMemoryUsages(MemoryUsageHeap& heap) {
  std::vector<MemoryUsage> usages;
  usages.reserve(heap.size());
  while (!heap.empty()) {
    usages.push_back(heap.top());
    heap.pop();
  }
  std::reverse(usages.begin(), usages.end());
  return usages;
}

// Invoked by visitChildren() to traverse the memory pool structure to build the
// memory capacity exceeded exception error message.
void treeMemoryUsageVisitor(
    MemoryPool* pool,
    size_t indent,
    MemoryUsageHeap& topLeafMemUsages,
    bool skipEmptyPool,
    std::stringstream& out) {
  const MemoryPool::Stats stats = pool->stats();
  // Avoid logging empty pools if 'skipEmptyPool' is true.
  if (stats.empty() && skipEmptyPool) {
    return;
  }
  const MemoryUsage usage{
      .name = pool->name(),
      .currentUsage = stats.usedBytes,
      .reservedUsage = stats.reservedBytes,
      .peakUsage = stats.peakBytes,
  };
  out << std::string(indent, ' ') << usage.toString() << "\n";

  if (pool->kind() == MemoryPool::Kind::kLeaf) {
    if (stats.empty()) {
      return;
    }
    static const size_t kTopNLeafMessages = 10;
    topLeafMemUsages.push(usage);
    if (topLeafMemUsages.size() > kTopNLeafMessages) {
      topLeafMemUsages.pop();
    }
    return;
  }
  pool->visitChildren([&, indent = indent + kCapMessageIndentSize](
                          MemoryPool* pool) {
    treeMemoryUsageVisitor(pool, indent, topLeafMemUsages, skipEmptyPool, out);
    return true;
  });
}

std::string capacityToString(int64_t capacity) {
  return capacity == kMaxMemory ? "UNLIMITED" : succinctBytes(capacity);
}

#define DEBUG_RECORD_ALLOC(...)        \
  if (FOLLY_UNLIKELY(debugEnabled_)) { \
    recordAllocDbg(__VA_ARGS__);       \
  }
#define DEBUG_RECORD_FREE(...)         \
  if (FOLLY_UNLIKELY(debugEnabled_)) { \
    recordFreeDbg(__VA_ARGS__);        \
  }
#define DEBUG_LEAK_CHECK()             \
  if (FOLLY_UNLIKELY(debugEnabled_)) { \
    leakCheckDbg();                    \
  }
} // namespace

std::string MemoryPool::Stats::toString() const {
  return fmt::format(
      "usedBytes:{} reservedBytes:{} peakBytes:{} cumulativeBytes:{} numAllocs:{} numFrees:{} numReserves:{} numReleases:{} numShrinks:{} numReclaims:{} numCollisions:{} numCapacityGrowths:{}",
      succinctBytes(usedBytes),
      succinctBytes(reservedBytes),
      succinctBytes(peakBytes),
      succinctBytes(cumulativeBytes),
      numAllocs,
      numFrees,
      numReserves,
      numReleases,
      numShrinks,
      numReclaims,
      numCollisions,
      numCapacityGrowths);
}

bool MemoryPool::Stats::operator==(const MemoryPool::Stats& other) const {
  return std::tie(
             usedBytes,
             reservedBytes,
             peakBytes,
             cumulativeBytes,
             numAllocs,
             numFrees,
             numReserves,
             numReleases,
             numCollisions,
             numCapacityGrowths) ==
      std::tie(
             other.usedBytes,
             other.reservedBytes,
             other.peakBytes,
             other.cumulativeBytes,
             other.numAllocs,
             other.numFrees,
             other.numReserves,
             other.numReleases,
             other.numCollisions,
             other.numCapacityGrowths);
}

std::ostream& operator<<(std::ostream& os, const MemoryPool::Stats& stats) {
  return os << stats.toString();
}

MemoryPool::MemoryPool(
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryPool> parent,
    const Options& options)
    : name_(name),
      kind_(kind),
      alignment_(options.alignment),
      parent_(std::move(parent)),
      maxCapacity_(parent_ == nullptr ? options.maxCapacity : kMaxMemory),
      trackUsage_(options.trackUsage),
      threadSafe_(options.threadSafe),
      debugEnabled_(options.debugEnabled),
      coreOnAllocationFailureEnabled_(options.coreOnAllocationFailureEnabled) {
  VELOX_CHECK(!isRoot() || !isLeaf());
  VELOX_CHECK_GT(
      maxCapacity_, 0, "Memory pool {} max capacity can't be zero", name_);
  MemoryAllocator::alignmentCheck(0, alignment_);
}

MemoryPool::~MemoryPool() {
  VELOX_CHECK(children_.empty());
}

// static
std::string MemoryPool::kindString(Kind kind) {
  switch (kind) {
    case Kind::kLeaf:
      return "LEAF";
    case Kind::kAggregate:
      return "AGGREGATE";
    default:
      return fmt::format("UNKNOWN_{}", static_cast<int>(kind));
  }
}

std::ostream& operator<<(std::ostream& out, MemoryPool::Kind kind) {
  return out << MemoryPool::kindString(kind);
}

const std::string& MemoryPool::name() const {
  return name_;
}

MemoryPool::Kind MemoryPool::kind() const {
  return kind_;
}

MemoryPool* MemoryPool::parent() const {
  return parent_.get();
}

MemoryPool* MemoryPool::root() const {
  const MemoryPool* pool = this;
  while (pool->parent_ != nullptr) {
    pool = pool->parent_.get();
  }
  return const_cast<MemoryPool*>(pool);
}

uint64_t MemoryPool::getChildCount() const {
  std::shared_lock guard{poolMutex_};
  return children_.size();
}

void MemoryPool::visitChildren(
    const std::function<bool(MemoryPool*)>& visitor) const {
  std::vector<std::shared_ptr<MemoryPool>> children;
  {
    std::shared_lock guard{poolMutex_};
    children.reserve(children_.size());
    for (auto& entry : children_) {
      auto child = entry.second.lock();
      if (child != nullptr) {
        children.push_back(std::move(child));
      }
    }
  }

  // NOTE: we should call 'visitor' on child pool object out of 'poolMutex_' to
  // avoid potential recursive locking issues. Firstly, the user provided
  // 'visitor' might try to acquire this memory pool lock again. Secondly, the
  // shared child pool reference created from the weak pointer might be the last
  // reference if some other threads drop all the external references during
  // this time window. Then drop of this last shared reference after 'visitor'
  // call will trigger child memory pool destruction in that case. The child
  // memory pool destructor will remove its weak pointer reference from the
  // parent pool which needs to acquire this memory pool lock again.
  for (auto& child : children) {
    if (!visitor(child.get())) {
      return;
    }
  }
}

std::shared_ptr<MemoryPool> MemoryPool::addLeafChild(
    const std::string& name,
    bool threadSafe,
    std::unique_ptr<MemoryReclaimer> _reclaimer) {
  CHECK_POOL_MANAGEMENT_OP(addLeafChild);
  // NOTE: we shall only set reclaimer in a child pool if its parent has also
  // set. Otherwise it should be mis-configured.
  VELOX_CHECK(
      reclaimer() != nullptr || _reclaimer == nullptr,
      "Child memory pool {} shall only set memory reclaimer if its parent {} has also set",
      name,
      name_);

  std::unique_lock guard{poolMutex_};
  VELOX_CHECK_EQ(
      children_.count(name),
      0,
      "Leaf child memory pool {} already exists in {}",
      name,
      name_);
  auto child = genChild(
      shared_from_this(),
      name,
      MemoryPool::Kind::kLeaf,
      threadSafe,
      std::move(_reclaimer));
  children_.emplace(name, child);
  return child;
}

std::shared_ptr<MemoryPool> MemoryPool::addAggregateChild(
    const std::string& name,
    std::unique_ptr<MemoryReclaimer> _reclaimer) {
  CHECK_POOL_MANAGEMENT_OP(addAggregateChild);
  // NOTE: we shall only set reclaimer in a child pool if its parent has also
  // set. Otherwise it should be mis-configured.
  VELOX_CHECK(
      reclaimer() != nullptr || _reclaimer == nullptr,
      "Child memory pool {} shall only set memory reclaimer if its parent {} has also set",
      name,
      name_);

  std::unique_lock guard{poolMutex_};
  VELOX_CHECK_EQ(
      children_.count(name),
      0,
      "Child memory pool {} already exists in {}",
      name,
      name_);
  auto child = genChild(
      shared_from_this(),
      name,
      MemoryPool::Kind::kAggregate,
      true,
      std::move(_reclaimer));
  children_.emplace(name, child);
  return child;
}

void MemoryPool::dropChild(const MemoryPool* child) {
  CHECK_POOL_MANAGEMENT_OP(dropChild);
  std::unique_lock guard{poolMutex_};
  const auto ret = children_.erase(child->name());
  VELOX_CHECK_EQ(
      ret,
      1,
      "Child memory pool {} doesn't exist in {}",
      child->name(),
      toString());
}

bool MemoryPool::aborted() const {
  if (parent_ != nullptr) {
    return parent_->aborted();
  }
  return aborted_;
}

std::exception_ptr MemoryPool::abortError() const {
  if (parent_ != nullptr) {
    return parent_->abortError();
  }
  return abortError_;
}

size_t MemoryPool::preferredSize(size_t size) {
  if (size < 8) {
    return 8;
  }
  int32_t bits = 63 - bits::countLeadingZeros<uint64_t>(size);
  size_t lower = 1ULL << bits;
  // Size is a power of 2.
  if (lower == size) {
    return size;
  }
  // If size is below 1.5 * previous power of two, return 1.5 *
  // the previous power of two, else the next power of 2.
  if (lower + (lower / 2) >= size) {
    return lower + (lower / 2);
  }
  return lower * 2;
}

MemoryPoolImpl::MemoryPoolImpl(
    MemoryManager* memoryManager,
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryPool> parent,
    std::unique_ptr<MemoryReclaimer> reclaimer,
    const Options& options)
    : MemoryPool{name, kind, parent, options},
      manager_{memoryManager},
      allocator_{manager_->allocator()},
      arbitrator_{manager_->arbitrator()},
      debugPoolNameRegex_(debugEnabled_ ? *(debugPoolNameRegex().rlock()) : ""),
      reclaimer_(std::move(reclaimer)),
      // The memory manager sets the capacity through grow() according to the
      // actually used memory arbitration policy.
      capacity_(parent_ != nullptr ? kMaxMemory : 0) {
  VELOX_CHECK(options.threadSafe || isLeaf());
}

MemoryPoolImpl::~MemoryPoolImpl() {
  DEBUG_LEAK_CHECK();
  if (parent_ != nullptr) {
    toImpl(parent_)->dropChild(this);
  }

  if (isLeaf()) {
    if (usedReservationBytes_ > 0) {
      VELOX_MEM_LOG(ERROR) << "Memory leak (Used memory): " << toString();
      RECORD_METRIC_VALUE(
          kMetricMemoryPoolUsageLeakBytes, usedReservationBytes_);
    }

    if (minReservationBytes_ > 0) {
      VELOX_MEM_LOG(ERROR) << "Memory leak (Reserved Memory): " << toString();
      RECORD_METRIC_VALUE(
          kMetricMemoryPoolReservationLeakBytes, minReservationBytes_);
    }
  }
  VELOX_DCHECK(
      (usedReservationBytes_ == 0) && (reservationBytes_ == 0) &&
          (minReservationBytes_ == 0),
      "Bad memory usage track state: {}",
      toString());

  if (isRoot()) {
    RECORD_HISTOGRAM_METRIC_VALUE(
        kMetricMemoryPoolCapacityGrowCount, numCapacityGrowths_);
  }

  if (destructionCb_ != nullptr) {
    destructionCb_(this);
  }
}

MemoryPool::Stats MemoryPoolImpl::stats() const {
  std::lock_guard<std::mutex> l(mutex_);
  return statsLocked();
}

MemoryPool::Stats MemoryPoolImpl::statsLocked() const {
  Stats stats;
  stats.usedBytes = usedBytes();
  stats.reservedBytes = reservationBytes_;
  stats.peakBytes = peakBytes_;
  stats.cumulativeBytes = cumulativeBytes_;
  stats.numAllocs = numAllocs_;
  stats.numFrees = numFrees_;
  stats.numReserves = numReserves_;
  stats.numReleases = numReleases_;
  stats.numCollisions = numCollisions_;
  stats.numCapacityGrowths = numCapacityGrowths_;
  return stats;
}

void* MemoryPoolImpl::allocate(int64_t size) {
  CHECK_AND_INC_MEM_OP_STATS(Allocs);
  const auto alignedSize = sizeAlign(size);
  reserve(alignedSize);
  void* buffer = allocator_->allocateBytes(alignedSize, alignment_);
  if (FOLLY_UNLIKELY(buffer == nullptr)) {
    release(alignedSize);
    handleAllocationFailure(fmt::format(
        "{} failed with {} from {} {}",
        __FUNCTION__,
        succinctBytes(size),
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  DEBUG_RECORD_ALLOC(buffer, size);
  return buffer;
}

void* MemoryPoolImpl::allocateZeroFilled(int64_t numEntries, int64_t sizeEach) {
  CHECK_AND_INC_MEM_OP_STATS(Allocs);
  const auto size = sizeEach * numEntries;
  const auto alignedSize = sizeAlign(size);
  reserve(alignedSize);
  void* buffer = allocator_->allocateZeroFilled(alignedSize);
  if (FOLLY_UNLIKELY(buffer == nullptr)) {
    release(alignedSize);
    handleAllocationFailure(fmt::format(
        "{} failed with {} entries and {} each from {} {}",
        __FUNCTION__,
        numEntries,
        succinctBytes(sizeEach),
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  DEBUG_RECORD_ALLOC(buffer, size);
  return buffer;
}

void* MemoryPoolImpl::reallocate(void* p, int64_t size, int64_t newSize) {
  CHECK_AND_INC_MEM_OP_STATS(Allocs);
  const auto alignedNewSize = sizeAlign(newSize);
  reserve(alignedNewSize);

  void* newP = allocator_->allocateBytes(alignedNewSize, alignment_);
  if (FOLLY_UNLIKELY(newP == nullptr)) {
    release(alignedNewSize);
    handleAllocationFailure(fmt::format(
        "{} failed with new {} and old {} from {} {}",
        __FUNCTION__,
        succinctBytes(newSize),
        succinctBytes(size),
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  DEBUG_RECORD_ALLOC(newP, newSize);
  if (p != nullptr) {
    ::memcpy(newP, p, std::min(size, newSize));
    free(p, size);
  }
  return newP;
}

void MemoryPoolImpl::free(void* p, int64_t size) {
  CHECK_AND_INC_MEM_OP_STATS(Frees);
  const auto alignedSize = sizeAlign(size);
  DEBUG_RECORD_FREE(p, size);
  allocator_->freeBytes(p, alignedSize);
  release(alignedSize);
}

void MemoryPoolImpl::allocateNonContiguous(
    MachinePageCount numPages,
    Allocation& out,
    MachinePageCount minSizeClass) {
  CHECK_AND_INC_MEM_OP_STATS(Allocs);
  if (!out.empty()) {
    INC_MEM_OP_STATS(Frees);
  }
  VELOX_CHECK_GT(numPages, 0);
  TestValue::adjust(
      "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
      this);
  DEBUG_RECORD_FREE(out);
  if (!allocator_->allocateNonContiguous(
          numPages,
          out,
          [this](uint64_t allocBytes, bool preAllocate) {
            if (preAllocate) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          },
          minSizeClass)) {
    VELOX_CHECK(out.empty());
    handleAllocationFailure(fmt::format(
        "{} failed with {} pages from {} {}",
        __FUNCTION__,
        numPages,
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  DEBUG_RECORD_ALLOC(out);
  VELOX_CHECK(!out.empty());
  VELOX_CHECK_NULL(out.pool());
  out.setPool(this);
}

void MemoryPoolImpl::freeNonContiguous(Allocation& allocation) {
  CHECK_AND_INC_MEM_OP_STATS(Frees);
  DEBUG_RECORD_FREE(allocation);
  const int64_t freedBytes = allocator_->freeNonContiguous(allocation);
  VELOX_CHECK(allocation.empty());
  release(freedBytes);
}

MachinePageCount MemoryPoolImpl::largestSizeClass() const {
  return allocator_->largestSizeClass();
}

const std::vector<MachinePageCount>& MemoryPoolImpl::sizeClasses() const {
  return allocator_->sizeClasses();
}

void MemoryPoolImpl::allocateContiguous(
    MachinePageCount numPages,
    ContiguousAllocation& out,
    MachinePageCount maxPages) {
  CHECK_AND_INC_MEM_OP_STATS(Allocs);
  if (!out.empty()) {
    INC_MEM_OP_STATS(Frees);
  }
  VELOX_CHECK_GT(numPages, 0);
  DEBUG_RECORD_FREE(out);
  if (!allocator_->allocateContiguous(
          numPages,
          nullptr,
          out,
          [this](uint64_t allocBytes, bool preAlloc) {
            if (preAlloc) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          },
          maxPages)) {
    VELOX_CHECK(out.empty());
    handleAllocationFailure(fmt::format(
        "{} failed with {} pages from {} {}",
        __FUNCTION__,
        numPages,
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  DEBUG_RECORD_ALLOC(out);
  VELOX_CHECK(!out.empty());
  VELOX_CHECK_NULL(out.pool());
  out.setPool(this);
}

void MemoryPoolImpl::freeContiguous(ContiguousAllocation& allocation) {
  CHECK_AND_INC_MEM_OP_STATS(Frees);
  const int64_t bytesToFree = allocation.size();
  DEBUG_RECORD_FREE(allocation);
  allocator_->freeContiguous(allocation);
  VELOX_CHECK(allocation.empty());
  release(bytesToFree);
}

void MemoryPoolImpl::growContiguous(
    MachinePageCount increment,
    ContiguousAllocation& allocation) {
  if (!allocator_->growContiguous(
          increment, allocation, [this](uint64_t allocBytes, bool preAlloc) {
            if (preAlloc) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          })) {
    handleAllocationFailure(fmt::format(
        "{} failed with {} pages from {} {}",
        __FUNCTION__,
        increment,
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  if (FOLLY_UNLIKELY(debugEnabled_)) {
    recordGrowDbg(allocation.data(), allocation.size());
  }
}

int64_t MemoryPoolImpl::capacity() const {
  if (parent_ != nullptr) {
    return parent_->capacity();
  }
  std::lock_guard<std::mutex> l(mutex_);
  return capacity_;
}

int64_t MemoryPoolImpl::usedBytes() const {
  if (isLeaf()) {
    return usedReservationBytes_;
  }
  if (reservedBytes() == 0) {
    return 0;
  }
  int64_t usedBytes{0};
  visitChildren([&](MemoryPool* pool) {
    usedBytes += pool->usedBytes();
    return true;
  });
  return usedBytes;
}

int64_t MemoryPoolImpl::releasableReservation() const {
  if (isLeaf()) {
    std::lock_guard<std::mutex> l(mutex_);
    return std::max<int64_t>(
        0, reservationBytes_ - quantizedSize(usedReservationBytes_));
  }
  if (reservedBytes() == 0) {
    return 0;
  }
  int64_t releasableBytes{0};
  visitChildren([&](MemoryPool* pool) {
    releasableBytes += pool->releasableReservation();
    return true;
  });
  return releasableBytes;
}

std::shared_ptr<MemoryPool> MemoryPoolImpl::genChild(
    std::shared_ptr<MemoryPool> parent,
    const std::string& name,
    Kind kind,
    bool threadSafe,
    std::unique_ptr<MemoryReclaimer> reclaimer) {
  return std::make_shared<MemoryPoolImpl>(
      manager_,
      name,
      kind,
      parent,
      std::move(reclaimer),
      Options{
          .alignment = alignment_,
          .trackUsage = trackUsage_,
          .threadSafe = threadSafe,
          .debugEnabled = debugEnabled_,
          .coreOnAllocationFailureEnabled = coreOnAllocationFailureEnabled_});
}

bool MemoryPoolImpl::maybeReserve(uint64_t increment) {
  CHECK_AND_INC_MEM_OP_STATS(Reserves);
  TestValue::adjust(
      "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve", this);
  // TODO: make this a configurable memory pool option.
  constexpr int32_t kGrowthQuantum = 8 << 20;
  const auto reservationToAdd = bits::roundUp(increment, kGrowthQuantum);
  try {
    reserve(reservationToAdd, true);
  } catch (const std::exception&) {
    if (aborted()) {
      // NOTE: we shall throw to stop the query execution if the root memory
      // pool has been aborted. It is also unsafe to proceed as the memory abort
      // code path might have already freed up the memory resource of this
      // operator while it is under memory arbitration.
      std::rethrow_exception(std::current_exception());
    }
    return false;
  }
  return true;
}

void MemoryPoolImpl::reserve(uint64_t size, bool reserveOnly) {
  if (FOLLY_LIKELY(trackUsage_)) {
    if (FOLLY_LIKELY(threadSafe_)) {
      reserveThreadSafe(size, reserveOnly);
    } else {
      reserveNonThreadSafe(size, reserveOnly);
    }
  }
  if (reserveOnly) {
    return;
  }
}

void MemoryPoolImpl::reserveThreadSafe(uint64_t size, bool reserveOnly) {
  VELOX_CHECK(isLeaf());

  int32_t numAttempts = 0;
  int64_t increment = 0;
  for (;; ++numAttempts) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      increment = reservationSizeLocked(size);
      if (increment == 0) {
        if (reserveOnly) {
          minReservationBytes_ = tsanAtomicValue(reservationBytes_);
        } else {
          usedReservationBytes_ += size;
          cumulativeBytes_ += size;
          maybeUpdatePeakBytesLocked(usedReservationBytes_);
        }
        sanityCheckLocked();
        break;
      }
    }
    TestValue::adjust(
        "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe", this);
    try {
      incrementReservationThreadSafe(this, increment);
    } catch (const std::exception&) {
      // When race with concurrent memory reservation free, we might end up with
      // unused reservation but no used reservation if a retry memory
      // reservation attempt run into memory capacity exceeded error.
      releaseThreadSafe(0, false);
      std::rethrow_exception(std::current_exception());
    }
  }

  // NOTE: in case of concurrent reserve and release requests, we might see
  // potential conflicts as the quantized memory release might free up extra
  // reservation bytes so reserve might go extra round to reserve more bytes.
  // This should happen rarely in production as the leaf tracker updates are
  // mostly single thread executed.
  if (numAttempts > 1) {
    numCollisions_ += numAttempts - 1;
  }
}

bool MemoryPoolImpl::incrementReservationThreadSafe(
    MemoryPool* requestor,
    uint64_t size) {
  VELOX_CHECK(threadSafe_);
  VELOX_CHECK_GT(size, 0);

  // Propagate the increment to the root memory pool to check the capacity limit
  // first. If it exceeds the capacity and can't grow, the root memory pool will
  // throw an exception to fail the request.
  if (parent_ != nullptr) {
    if (!toImpl(parent_)->incrementReservationThreadSafe(requestor, size)) {
      return false;
    }
  }

  if (maybeIncrementReservation(size)) {
    return true;
  }

  VELOX_CHECK_NULL(parent_);

  if (growCapacity(requestor, size)) {
    TestValue::adjust(
        "facebook::velox::memory::MemoryPoolImpl::incrementReservationThreadSafe::AfterGrowCallback",
        this);
    // NOTE: if memory arbitration succeeds, it should have already committed
    // the reservation 'size' in the root memory pool.
    return true;
  }
  VELOX_MEM_POOL_CAP_EXCEEDED(fmt::format(
      "Exceeded memory pool capacity after attempt to grow capacity "
      "through arbitration. Requestor pool name '{}', request size {}, memory "
      "pool capacity {}, memory pool max capacity {}, memory manager capacity "
      "{}, current usage {}\n{}",
      requestor->name(),
      succinctBytes(size),
      capacityToString(capacity()),
      capacityToString(maxCapacity_),
      capacityToString(manager_->capacity()),
      succinctBytes(requestor->usedBytes()),
      treeMemoryUsage()));
}

bool MemoryPoolImpl::growCapacity(MemoryPool* requestor, uint64_t size) {
  VELOX_CHECK(requestor->isLeaf());
  ++numCapacityGrowths_;

  bool success{false};
  {
    MemoryPoolArbitrationSection arbitrationSection(requestor);
    success = arbitrator_->growCapacity(this, size);
  }
  // The memory pool might have been aborted during the time it leaves the
  // arbitration no matter the arbitration succeed or not.
  if (FOLLY_UNLIKELY(aborted())) {
    if (success) {
      // Release the reservation committed by the memory arbitration on success.
      decrementReservation(size);
    }
    VELOX_CHECK_NOT_NULL(abortError());
    std::rethrow_exception(abortError());
  }
  return success;
}

bool MemoryPoolImpl::maybeIncrementReservation(uint64_t size) {
  std::lock_guard<std::mutex> l(mutex_);
  if (isRoot()) {
    checkIfAborted();

    // NOTE: we allow memory pool to overuse its memory during the memory
    // arbitration process. The memory arbitration process itself needs to
    // ensure the memory pool usage of the memory pool is within the capacity
    // limit after the arbitration operation completes.
    if (FOLLY_UNLIKELY(
            (reservationBytes_ + size > capacity_) &&
            !underMemoryArbitration())) {
      return false;
    }
  }
  incrementReservationLocked(size);
  return true;
}

void MemoryPoolImpl::incrementReservationLocked(uint64_t bytes) {
  reservationBytes_ += bytes;
  if (!isLeaf()) {
    cumulativeBytes_ += bytes;
    maybeUpdatePeakBytesLocked(reservationBytes_);
  }
}

void MemoryPoolImpl::release() {
  CHECK_AND_INC_MEM_OP_STATS(Releases);
  release(0, true);
}

void MemoryPoolImpl::release(uint64_t size, bool releaseOnly) {
  if (FOLLY_LIKELY(trackUsage_)) {
    if (FOLLY_LIKELY(threadSafe_)) {
      releaseThreadSafe(size, releaseOnly);
    } else {
      releaseNonThreadSafe(size, releaseOnly);
    }
  }
}

void MemoryPoolImpl::releaseThreadSafe(uint64_t size, bool releaseOnly) {
  VELOX_CHECK(isLeaf());
  VELOX_DCHECK_NOT_NULL(parent_);

  int64_t freeable = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    int64_t newQuantized;
    if (FOLLY_UNLIKELY(releaseOnly)) {
      VELOX_DCHECK_EQ(size, 0);
      if (minReservationBytes_ == 0) {
        return;
      }
      newQuantized = quantizedSize(usedReservationBytes_);
      minReservationBytes_ = 0;
    } else {
      usedReservationBytes_ -= size;
      const int64_t newCap =
          std::max(minReservationBytes_, usedReservationBytes_);
      newQuantized = quantizedSize(newCap);
    }
    freeable = reservationBytes_ - newQuantized;
    if (freeable > 0) {
      reservationBytes_ = newQuantized;
    }
    sanityCheckLocked();
  }
  if (freeable > 0) {
    toImpl(parent_)->decrementReservation(freeable);
  }
}

void MemoryPoolImpl::decrementReservation(uint64_t size) noexcept {
  VELOX_CHECK_GT(size, 0);

  if (parent_ != nullptr) {
    toImpl(parent_)->decrementReservation(size);
  }
  std::lock_guard<std::mutex> l(mutex_);
  reservationBytes_ -= size;
  sanityCheckLocked();
}

std::string MemoryPoolImpl::treeMemoryUsage(bool skipEmptyPool) const {
  if (parent_ != nullptr) {
    return parent_->treeMemoryUsage(skipEmptyPool);
  }
  if (FLAGS_velox_suppress_memory_capacity_exceeding_error_message) {
    return "";
  }
  std::stringstream out;
  {
    std::lock_guard<std::mutex> l(mutex_);
    const Stats stats = statsLocked();
    const MemoryUsage usage{
        .name = name(),
        .currentUsage = stats.usedBytes,
        .reservedUsage = stats.reservedBytes,
        .peakUsage = stats.peakBytes};
    out << usage.toString() << "\n";
  }

  MemoryUsageHeap topLeafMemUsages;
  visitChildren([&, indent = kCapMessageIndentSize](MemoryPool* pool) {
    treeMemoryUsageVisitor(pool, indent, topLeafMemUsages, skipEmptyPool, out);
    return true;
  });

  if (!topLeafMemUsages.empty()) {
    out << "\nTop " << topLeafMemUsages.size() << " leaf memory pool usages:\n";
    std::vector<MemoryUsage> usages = sortMemoryUsages(topLeafMemUsages);
    for (const auto& usage : usages) {
      out << std::string(kCapMessageIndentSize, ' ') << usage.toString()
          << "\n";
    }
  }
  return out.str();
}

uint64_t MemoryPoolImpl::freeBytes() const {
  if (parent_ != nullptr) {
    return parent_->freeBytes();
  }
  std::lock_guard<std::mutex> l(mutex_);
  if (capacity_ == kMaxMemory) {
    return 0;
  }
  if (capacity_ < reservationBytes_) {
    // NOTE: the memory reservation could be temporarily larger than its
    // capacity if this memory pool is under memory arbitration processing.
    return 0;
  }
  return capacity_ - reservationBytes_;
}

void MemoryPoolImpl::setReclaimer(std::unique_ptr<MemoryReclaimer> reclaimer) {
  VELOX_CHECK_NOT_NULL(reclaimer);
  if (parent_ != nullptr) {
    VELOX_CHECK_NOT_NULL(
        parent_->reclaimer(),
        "Child memory pool {} shall only set reclaimer if its parent {} has also set",
        name_,
        parent_->name());
  }
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK_NULL(reclaimer_);
  reclaimer_ = std::move(reclaimer);
}

MemoryReclaimer* MemoryPoolImpl::reclaimer() const {
  tsan_lock_guard<std::mutex> l(mutex_);
  return reclaimer_.get();
}

std::optional<uint64_t> MemoryPoolImpl::reclaimableBytes() const {
  if (reclaimer() == nullptr) {
    return std::nullopt;
  }

  uint64_t reclaimableBytes = 0;
  if (!reclaimer()->reclaimableBytes(*this, reclaimableBytes)) {
    return std::nullopt;
  }

  return reclaimableBytes;
}

uint64_t MemoryPoolImpl::reclaim(
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    memory::MemoryReclaimer::Stats& stats) {
  if (reclaimer() == nullptr) {
    return 0;
  }
  return reclaimer()->reclaim(this, targetBytes, maxWaitMs, stats);
}

void MemoryPoolImpl::enterArbitration() {
  if (reclaimer() != nullptr) {
    reclaimer()->enterArbitration();
  }
}

void MemoryPoolImpl::leaveArbitration() noexcept {
  if (reclaimer() != nullptr) {
    reclaimer()->leaveArbitration();
  }
}

uint64_t MemoryPoolImpl::shrink(uint64_t targetBytes) {
  if (parent_ != nullptr) {
    return toImpl(parent_)->shrink(targetBytes);
  }
  std::lock_guard<std::mutex> l(mutex_);
  // We don't expect to shrink a memory pool without capacity limit.
  VELOX_CHECK_NE(capacity_, kMaxMemory);
  uint64_t freeBytes = std::max<uint64_t>(0, capacity_ - reservationBytes_);
  if (targetBytes != 0) {
    freeBytes = std::min(targetBytes, freeBytes);
  }
  capacity_ -= freeBytes;
  return freeBytes;
}

bool MemoryPoolImpl::grow(uint64_t growBytes, uint64_t reservationBytes) {
  if (parent_ != nullptr) {
    return toImpl(parent_)->grow(growBytes, reservationBytes);
  }
  // TODO: add to prevent from growing beyond the max capacity and the
  // corresponding support in memory arbitrator.
  std::lock_guard<std::mutex> l(mutex_);
  // We don't expect to grow a memory pool without capacity limit.
  VELOX_CHECK_NE(capacity_, kMaxMemory, "Can't grow with unlimited capacity");
  if (capacity_ + growBytes > maxCapacity_) {
    return false;
  }
  if (reservationBytes_ + reservationBytes > capacity_ + growBytes) {
    return false;
  }

  capacity_ += growBytes;
  VELOX_CHECK_GE(capacity_, growBytes);
  if (reservationBytes > 0) {
    incrementReservationLocked(reservationBytes);
    VELOX_CHECK_LE(reservationBytes, reservationBytes_);
  }
  return true;
}

void MemoryPoolImpl::abort(const std::exception_ptr& error) {
  VELOX_CHECK_NOT_NULL(error);
  if (parent_ != nullptr) {
    parent_->abort(error);
    return;
  }
  if (reclaimer() == nullptr) {
    VELOX_FAIL("Can't abort the memory pool {} without reclaimer", name_);
  }
  setAbortError(error);
  reclaimer()->abort(this, error);
}

void MemoryPoolImpl::setAbortError(const std::exception_ptr& error) {
  VELOX_CHECK(
      !aborted_,
      "Trying to set another abort error on an already aborted pool.");
  abortError_ = error;
  aborted_ = true;
}

void MemoryPoolImpl::checkIfAborted() const {
  if (FOLLY_UNLIKELY(aborted())) {
    VELOX_CHECK_NOT_NULL(abortError());
    std::rethrow_exception(abortError());
  }
}

void MemoryPoolImpl::setDestructionCallback(
    const DestructionCallback& callback) {
  VELOX_CHECK_NOT_NULL(callback);
  VELOX_CHECK(
      isRoot(),
      "Only root memory pool allows to set destruction callbacks: {}",
      name_);
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK_NULL(destructionCb_);
  destructionCb_ = callback;
}

void MemoryPoolImpl::testingSetCapacity(int64_t bytes) {
  if (parent_ != nullptr) {
    return toImpl(parent_)->testingSetCapacity(bytes);
  }
  std::lock_guard<std::mutex> l(mutex_);
  capacity_ = bytes;
}

void MemoryPoolImpl::testingSetReservation(int64_t bytes) {
  if (parent_ != nullptr) {
    return toImpl(parent_)->testingSetReservation(bytes);
  }
  std::lock_guard<std::mutex> l(mutex_);
  reservationBytes_ = bytes;
}

bool MemoryPoolImpl::needRecordDbg(bool /* isAlloc */) {
  if (!debugPoolNameRegex_.empty()) {
    return RE2::FullMatch(name_, debugPoolNameRegex_);
  }
  // TODO(jtan6): Add sample based condition support.
  return true;
}

void MemoryPoolImpl::recordAllocDbg(const void* addr, uint64_t size) {
  VELOX_CHECK(debugEnabled_);
  if (!needRecordDbg(true)) {
    return;
  }
  std::lock_guard<std::mutex> l(debugAllocMutex_);
  debugAllocRecords_.emplace(
      reinterpret_cast<uint64_t>(addr),
      AllocationRecord{size, process::StackTrace()});
}

void MemoryPoolImpl::recordAllocDbg(const Allocation& allocation) {
  VELOX_CHECK(debugEnabled_);
  if (!needRecordDbg(true) || allocation.empty()) {
    return;
  }
  recordAllocDbg(allocation.runAt(0).data(), allocation.byteSize());
}

void MemoryPoolImpl::recordAllocDbg(const ContiguousAllocation& allocation) {
  VELOX_CHECK(debugEnabled_);
  if (!needRecordDbg(true) || allocation.empty()) {
    return;
  }
  recordAllocDbg(allocation.data(), allocation.size());
}

void MemoryPoolImpl::recordFreeDbg(const void* addr, uint64_t size) {
  VELOX_CHECK(debugEnabled_);
  if (!needRecordDbg(false) || addr == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> l(debugAllocMutex_);
  uint64_t addrUint64 = reinterpret_cast<uint64_t>(addr);
  auto allocResult = debugAllocRecords_.find(addrUint64);
  if (allocResult == debugAllocRecords_.end()) {
    VELOX_FAIL("Freeing of un-allocated memory. Free address {}.", addrUint64);
  }
  const auto allocRecord = allocResult->second;
  if (allocRecord.size != size) {
    const auto freeStackTrace = process::StackTrace().toString();
    VELOX_FAIL(fmt::format(
        "[MemoryPool] Trying to free {} bytes on an allocation of {} bytes.\n"
        "======== Allocation Stack ========\n"
        "{}\n"
        "============ Free Stack ==========\n"
        "{}\n",
        size,
        allocRecord.size,
        allocRecord.callStack.toString(),
        freeStackTrace));
  }
  debugAllocRecords_.erase(addrUint64);
}

void MemoryPoolImpl::recordFreeDbg(const Allocation& allocation) {
  VELOX_CHECK(debugEnabled_);
  if (!needRecordDbg(false) || allocation.empty()) {
    return;
  }
  recordFreeDbg(allocation.runAt(0).data(), allocation.byteSize());
}

void MemoryPoolImpl::recordFreeDbg(const ContiguousAllocation& allocation) {
  VELOX_CHECK(debugEnabled_);
  if (!needRecordDbg(false) || allocation.empty()) {
    return;
  }
  recordFreeDbg(allocation.data(), allocation.size());
}

void MemoryPoolImpl::recordGrowDbg(const void* addr, uint64_t newSize) {
  VELOX_CHECK(debugEnabled_);
  if (!needRecordDbg(false) || addr == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> l(debugAllocMutex_);
  uint64_t addrUint64 = reinterpret_cast<uint64_t>(addr);
  auto allocResult = debugAllocRecords_.find(addrUint64);
  if (allocResult == debugAllocRecords_.end()) {
    VELOX_FAIL("Growing of un-allocated memory. Free address {}.", addrUint64);
  }
  allocResult->second.size = newSize;
}

void MemoryPoolImpl::leakCheckDbg() {
  VELOX_CHECK(debugEnabled_);
  if (debugAllocRecords_.empty()) {
    return;
  }
  std::stringbuf buf;
  std::ostream oss(&buf);
  oss << "Detected total of " << debugAllocRecords_.size()
      << " leaked allocations:\n";
  struct AllocationStats {
    uint64_t size{0};
    uint64_t numAllocations{0};
  };
  std::unordered_map<std::string, AllocationStats> sizeAggregatedRecords;
  for (const auto& itr : debugAllocRecords_) {
    const auto& allocationRecord = itr.second;
    const auto stackStr = allocationRecord.callStack.toString();
    if (sizeAggregatedRecords.count(stackStr) == 0) {
      sizeAggregatedRecords[stackStr] = AllocationStats();
    }
    sizeAggregatedRecords[stackStr].size += allocationRecord.size;
    ++sizeAggregatedRecords[stackStr].numAllocations;
  }
  std::vector<std::pair<std::string, AllocationStats>> sortedRecords(
      sizeAggregatedRecords.begin(), sizeAggregatedRecords.end());
  std::sort(
      sortedRecords.begin(),
      sortedRecords.end(),
      [](const std::pair<std::string, AllocationStats>& a,
         std::pair<std::string, AllocationStats>& b) {
        return a.second.size > b.second.size;
      });
  for (const auto& pair : sortedRecords) {
    oss << "======== Leaked memory from " << pair.second.numAllocations
        << " total allocations of " << succinctBytes(pair.second.size)
        << " total size ========\n"
        << pair.first << "\n";
  }
  VELOX_FAIL(buf.str());
}

void MemoryPoolImpl::handleAllocationFailure(
    const std::string& failureMessage) {
  if (coreOnAllocationFailureEnabled_) {
    VELOX_MEM_LOG(ERROR) << failureMessage;
    // SIGBUS is one of the standard signals in Linux that triggers a core dump
    // Normally it is raised by the operating system when a misaligned memory
    // access occurs. On x86 and aarch64 misaligned access is allowed by default
    // hence this signal should never occur naturally. Raising a signal other
    // than SIGABRT makes it easier to distinguish an allocation failure from
    // any other crash
    raise(SIGBUS);
  }

  VELOX_MEM_ALLOC_ERROR(failureMessage);
}
} // namespace facebook::velox::memory
