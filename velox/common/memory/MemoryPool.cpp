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

#include <set>

#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"

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
  uint64_t peakUsage;

  bool operator>(const MemoryUsage& other) const {
    return std::tie(currentUsage, peakUsage, name) >
        std::tie(other.currentUsage, other.peakUsage, other.name);
  }

  std::string toString() const {
    return fmt::format(
        "{} usage {} peak {}",
        name,
        succinctBytes(currentUsage),
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
void capExceedingMessageVisitor(
    MemoryPool* pool,
    size_t indent,
    MemoryUsageHeap& topLeafMemUsages,
    std::stringstream& out) {
  const MemoryPool::Stats stats = pool->stats();
  // Avoid logging empty pools.
  if (stats.empty()) {
    return;
  }
  const MemoryUsage usage{
      .name = pool->name(),
      .currentUsage = stats.currentBytes,
      .peakUsage = stats.peakBytes,
  };
  out << std::string(indent, ' ') << usage.toString() << "\n";

  if (pool->kind() == MemoryPool::Kind::kLeaf) {
    static const size_t kTopNLeafMessages = 10;
    topLeafMemUsages.push(usage);
    if (topLeafMemUsages.size() > kTopNLeafMessages) {
      topLeafMemUsages.pop();
    }
    return;
  }
  pool->visitChildren(
      [&, indent = indent + kCapMessageIndentSize](MemoryPool* pool) {
        capExceedingMessageVisitor(pool, indent, topLeafMemUsages, out);
        return true;
      });
}

std::string capacityToString(int64_t capacity) {
  return capacity == kMaxMemory ? "UNLIMITED" : succinctBytes(capacity);
}
} // namespace

std::string MemoryPool::Stats::toString() const {
  return fmt::format(
      "currentBytes:{} peakBytes:{} cumulativeBytes:{} numAllocs:{} numFrees:{} numReserves:{} numReleases:{} numShrinks:{} numReclaims:{} numCollisions:{}",
      succinctBytes(currentBytes),
      succinctBytes(peakBytes),
      succinctBytes(cumulativeBytes),
      numAllocs,
      numFrees,
      numReserves,
      numReleases,
      numShrinks,
      numReclaims,
      numCollisions);
}

bool MemoryPool::Stats::operator==(const MemoryPool::Stats& other) const {
  return std::tie(
             currentBytes,
             peakBytes,
             cumulativeBytes,
             numAllocs,
             numFrees,
             numReserves,
             numReleases,
             numCollisions) ==
      std::tie(
             other.currentBytes,
             other.peakBytes,
             other.cumulativeBytes,
             other.numAllocs,
             other.numFrees,
             other.numReserves,
             other.numReleases,
             other.numCollisions);
}

std::ostream& operator<<(std::ostream& os, const MemoryPool::Stats& stats) {
  return os << stats.toString();
}

MemoryPool::MemoryPool(
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryPool> parent,
    std::unique_ptr<MemoryReclaimer> reclaimer,
    const Options& options)
    : name_(name),
      kind_(kind),
      alignment_(options.alignment),
      parent_(std::move(parent)),

      trackUsage_(options.trackUsage),
      threadSafe_(options.threadSafe),
      checkUsageLeak_(options.checkUsageLeak),
      reclaimer_(std::move(reclaimer)) {
  VELOX_CHECK(!isRoot() || !isLeaf());
  // NOTE: we shall only set reclaimer in a child pool if its parent has also
  // set. Otherwise. it should be mis-configured.
  VELOX_CHECK(
      parent_ == nullptr || parent_->reclaimer() != nullptr ||
          reclaimer_ == nullptr,
      "Child memory pool {} shall only set memory reclaimer if its parent {} has also set",
      name_,
      parent_->name());
  MemoryAllocator::alignmentCheck(0, alignment_);
}

MemoryPool::~MemoryPool() {
  VELOX_CHECK(children_.empty());
  if (parent_ != nullptr) {
    parent_->dropChild(this);
  }
}

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

MemoryPool* MemoryPool::root() {
  MemoryPool* root = this;
  while (root->parent_ != nullptr) {
    root = root->parent_.get();
  }
  return root;
}

uint64_t MemoryPool::getChildCount() const {
  folly::SharedMutex::ReadHolder guard{poolMutex_};
  return children_.size();
}

void MemoryPool::visitChildren(
    const std::function<bool(MemoryPool*)>& visitor) const {
  std::vector<std::shared_ptr<MemoryPool>> children;
  {
    folly::SharedMutex::ReadHolder guard{poolMutex_};
    children.reserve(children_.size());
    for (auto& entry : children_) {
      auto child = entry.second.lock();
      if (child != nullptr) {
        children.push_back(std::move(child));
      }
    }
  }

  // NOTE: we should call 'visitor' on child pool object out of
  // 'poolMutex_' to avoid potential recursive locking issues. Firstly, the
  // user provided 'visitor' might try to acquire this memory pool lock again.
  // Secondly, the shared child pool reference created from the weak pointer
  // might be the last reference if some other threads drop all the external
  // references during this time window. Then drop of this last shared reference
  // after 'visitor' call will trigger child memory pool destruction in that
  // case. The child memory pool destructor will remove its weak pointer
  // reference from the parent pool which needs to acquire this memory pool lock
  // again.
  for (auto& child : children) {
    if (!visitor(child.get())) {
      return;
    }
  }
}

std::shared_ptr<MemoryPool> MemoryPool::addLeafChild(
    const std::string& name,
    bool threadSafe,
    std::unique_ptr<MemoryReclaimer> reclaimer) {
  CHECK_POOL_MANAGEMENT_OP(addLeafChild);

  folly::SharedMutex::WriteHolder guard{poolMutex_};
  VELOX_CHECK_EQ(
      children_.count(name),
      0,
      "Leaf child memory pool {} already exists in {}",
      name,
      toString());
  auto child = genChild(
      shared_from_this(),
      name,
      MemoryPool::Kind::kLeaf,
      threadSafe,
      std::move(reclaimer));
  children_.emplace(name, child);
  return child;
}

std::shared_ptr<MemoryPool> MemoryPool::addAggregateChild(
    const std::string& name,
    std::unique_ptr<MemoryReclaimer> reclaimer) {
  CHECK_POOL_MANAGEMENT_OP(addAggregateChild);

  folly::SharedMutex::WriteHolder guard{poolMutex_};
  VELOX_CHECK_EQ(
      children_.count(name),
      0,
      "Child memory pool {} already exists in {}",
      name,
      toString());
  auto child = genChild(
      shared_from_this(),
      name,
      MemoryPool::Kind::kAggregate,
      true,
      std::move(reclaimer));
  children_.emplace(name, child);
  return child;
}

void MemoryPool::dropChild(const MemoryPool* child) {
  CHECK_POOL_MANAGEMENT_OP(dropChild);
  folly::SharedMutex::WriteHolder guard{poolMutex_};
  const auto ret = children_.erase(child->name());
  VELOX_CHECK_EQ(
      ret,
      1,
      "Child memory pool {} doesn't exist in {}",
      child->name(),
      toString());
}

size_t MemoryPool::preferredSize(size_t size) {
  if (size < 8) {
    return 8;
  }
  int32_t bits = 63 - bits::countLeadingZeros(size);
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

void MemoryPool::setReclaimer(std::unique_ptr<MemoryReclaimer> reclaimer) {
  VELOX_CHECK_NOT_NULL(reclaimer);
  if (parent_ != nullptr) {
    VELOX_CHECK_NOT_NULL(
        parent_->reclaimer(),
        "Child memory pool {} shall only set reclaimer if its parent {} has also set",
        name_,
        parent_->name());
  }
  folly::SharedMutex::WriteHolder guard{poolMutex_};
  VELOX_CHECK_NULL(reclaimer_);
  reclaimer_ = std::move(reclaimer);
}

MemoryReclaimer* MemoryPool::reclaimer() const {
  return reclaimer_.get();
}

bool MemoryPool::reclaimableBytes(uint64_t& reclaimableBytes) const {
  reclaimableBytes = 0;
  if (reclaimer_ == nullptr) {
    return false;
  }
  return reclaimer_->reclaimableBytes(*this, reclaimableBytes);
}

uint64_t MemoryPool::reclaim(uint64_t targetBytes) {
  if (reclaimer_ == nullptr) {
    return 0;
  }
  return reclaimer_->reclaim(this, targetBytes);
}

void MemoryPool::enterArbitration() {
  if (reclaimer_ != nullptr) {
    reclaimer_->enterArbitration();
  }
}

void MemoryPool::leaveArbitration() noexcept {
  if (reclaimer_ != nullptr) {
    reclaimer_->leaveArbitration();
  }
}

MemoryPoolImpl::MemoryPoolImpl(
    MemoryManager* memoryManager,
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryPool> parent,
    std::unique_ptr<MemoryReclaimer> reclaimer,
    DestructionCallback destructionCb,
    const Options& options)
    : MemoryPool{name, kind, parent, std::move(reclaimer), options},
      manager_{memoryManager},
      allocator_{&manager_->allocator()},
      destructionCb_(std::move(destructionCb)),
      capacity_(parent_ == nullptr ? options.capacity : kMaxMemory) {
  VELOX_CHECK(options.threadSafe || isLeaf());
}

MemoryPoolImpl::~MemoryPoolImpl() {
  if (checkUsageLeak_) {
    VELOX_CHECK(
        (usedReservationBytes_ == 0) && (reservationBytes_ == 0) &&
            (minReservationBytes_ == 0),
        "Bad memory usage track state: {}",
        toString());
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
  stats.currentBytes = currentBytesLocked();
  stats.peakBytes = peakBytes_;
  stats.cumulativeBytes = cumulativeBytes_;
  stats.numAllocs = numAllocs_;
  stats.numFrees = numFrees_;
  stats.numReserves = numReserves_;
  stats.numReleases = numReleases_;
  stats.numCollisions = numCollisions_;
  return stats;
}

void* MemoryPoolImpl::allocate(int64_t size) {
  CHECK_AND_INC_MEM_OP_STATS(Allocs);
  const auto alignedSize = sizeAlign(size);
  reserve(alignedSize);
  void* buffer = allocator_->allocateBytes(alignedSize, alignment_);
  if (FOLLY_UNLIKELY(buffer == nullptr)) {
    release(alignedSize);
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "{} failed with {} bytes from {}", __FUNCTION__, size, toString()));
  }
  return buffer;
}

void* MemoryPoolImpl::allocateZeroFilled(int64_t numEntries, int64_t sizeEach) {
  CHECK_AND_INC_MEM_OP_STATS(Allocs);
  const auto alignedSize = sizeAlign(sizeEach * numEntries);
  reserve(alignedSize);
  void* buffer = allocator_->allocateZeroFilled(alignedSize);
  if (FOLLY_UNLIKELY(buffer == nullptr)) {
    release(alignedSize);
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "{} failed with {} entries and {} bytes each from {}",
        __FUNCTION__,
        numEntries,
        sizeEach,
        toString()));
  }
  return buffer;
}

void* MemoryPoolImpl::reallocate(void* p, int64_t size, int64_t newSize) {
  CHECK_AND_INC_MEM_OP_STATS(Allocs);
  const auto alignedSize = sizeAlign(size);
  const auto alignedNewSize = sizeAlign(newSize);
  reserve(alignedNewSize);

  void* newP = allocator_->allocateBytes(alignedNewSize, alignment_);
  if (FOLLY_UNLIKELY(newP == nullptr)) {
    release(alignedNewSize);
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "{} failed with {} new bytes and {} old bytes from {}",
        __FUNCTION__,
        newSize,
        size,
        toString()));
  }
  if (p != nullptr) {
    ::memcpy(newP, p, std::min(size, newSize));
    free(p, alignedSize);
  }

  return newP;
}

void MemoryPoolImpl::free(void* p, int64_t size) {
  CHECK_AND_INC_MEM_OP_STATS(Frees);
  const auto alignedSize = sizeAlign(size);
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
  if (!allocator_->allocateNonContiguous(
          numPages,
          out,
          [this](int64_t allocBytes, bool preAllocate) {
            if (preAllocate) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          },
          minSizeClass)) {
    VELOX_CHECK(out.empty());
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "{} failed with {} pages from {}", __FUNCTION__, numPages, toString()));
  }
  VELOX_CHECK(!out.empty());
  VELOX_CHECK_NULL(out.pool());
  out.setPool(this);
}

void MemoryPoolImpl::freeNonContiguous(Allocation& allocation) {
  CHECK_AND_INC_MEM_OP_STATS(Frees);
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
    ContiguousAllocation& out) {
  CHECK_AND_INC_MEM_OP_STATS(Allocs);
  if (!out.empty()) {
    INC_MEM_OP_STATS(Frees);
  }
  VELOX_CHECK_GT(numPages, 0);

  if (!allocator_->allocateContiguous(
          numPages, nullptr, out, [this](int64_t allocBytes, bool preAlloc) {
            if (preAlloc) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          })) {
    VELOX_CHECK(out.empty());
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "{} failed with {} pages from {}", __FUNCTION__, numPages, toString()));
  }
  VELOX_CHECK(!out.empty());
  VELOX_CHECK_NULL(out.pool());
  out.setPool(this);
}

void MemoryPoolImpl::freeContiguous(ContiguousAllocation& allocation) {
  CHECK_AND_INC_MEM_OP_STATS(Frees);
  const int64_t bytesToFree = allocation.size();
  allocator_->freeContiguous(allocation);
  VELOX_CHECK(allocation.empty());
  release(bytesToFree);
}

int64_t MemoryPoolImpl::capacity() const {
  if (parent_ != nullptr) {
    return parent_->capacity();
  }
  std::lock_guard<std::mutex> l(mutex_);
  return capacity_;
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
      nullptr,
      Options{
          .alignment = alignment_,
          .trackUsage = trackUsage_,
          .threadSafe = threadSafe,
          .checkUsageLeak = checkUsageLeak_});
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
  } catch (const std::exception& e) {
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
  if (FOLLY_UNLIKELY(!manager_->reserve(size))) {
    // NOTE: If we can make the reserve and release a single transaction we
    // would have more accurate aggregates in intermediate states. However, this
    // is low-pri because we can only have inflated aggregates, and be on the
    // more conservative side.
    release(size);
    VELOX_MEM_POOL_CAP_EXCEEDED(toImpl(root())->capExceedingMessage(
        this,
        fmt::format(
            "Exceeded memory manager cap of {} when requesting {}, memory pool cap is {}",
            capacityToString(manager_->capacity()),
            succinctBytes(size),
            capacityToString(capacity()))));
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
    } catch (const std::exception& e) {
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

  {
    std::lock_guard<std::mutex> l(mutex_);
    if (maybeIncrementReservationLocked(size)) {
      return true;
    }
  }
  VELOX_CHECK_NULL(parent_);

  if (manager_->growPool(requestor, size)) {
    TestValue::adjust(
        "facebook::velox::memory::MemoryPoolImpl::incrementReservationThreadSafe::AfterGrowCallback",
        this);
    // NOTE: the memory reservation might still fail even if the memory grow
    // callback succeeds. The reason is that we don't hold the root tracker's
    // mutex lock while running the grow callback. Therefore, there is a
    // possibility in theory that a concurrent memory reservation request
    // might steal away the increased memory capacity after the grow callback
    // finishes and before we increase the reservation. If it happens, we can
    // simply fall back to retry the memory reservation from the leaf memory
    // pool which should happen rarely.
    return maybeIncrementReservation(size);
  }
  VELOX_MEM_POOL_CAP_EXCEEDED(capExceedingMessage(
      requestor,
      fmt::format(
          "Exceeded memory pool cap of {} when requesting {}, memory manager cap is {}",
          capacityToString(capacity_),
          succinctBytes(size),
          capacityToString(manager_->capacity()))));
}

bool MemoryPoolImpl::maybeIncrementReservation(uint64_t size) {
  std::lock_guard<std::mutex> l(mutex_);
  return maybeIncrementReservationLocked(size);
}

bool MemoryPoolImpl::maybeIncrementReservationLocked(uint64_t size) {
  if (isRoot()) {
    if (aborted()) {
      // Throw exception if this root memory pool has been aborted by the memory
      // arbitrator. This is to prevent it from triggering memory arbitration,
      // and we expect the associated query will also abort soon.
      VELOX_MEM_POOL_ABORTED(this);
    }
    if (reservationBytes_ + size > capacity_) {
      return false;
    }
  }
  reservationBytes_ += size;
  if (!isLeaf()) {
    cumulativeBytes_ += size;
    maybeUpdatePeakBytesLocked(reservationBytes_);
  }
  return true;
}

void MemoryPoolImpl::release() {
  CHECK_AND_INC_MEM_OP_STATS(Releases);
  release(0, true);
}

void MemoryPoolImpl::release(uint64_t size, bool releaseOnly) {
  if (!releaseOnly) {
    manager_->release(size);
  }
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

std::string MemoryPoolImpl::capExceedingMessage(
    MemoryPool* requestor,
    const std::string& errorMessage) {
  VELOX_CHECK_NULL(parent_);

  std::stringstream out;
  out << "\n" << errorMessage << "\n";
  if (FLAGS_velox_suppress_memory_capacity_exceeding_error_message) {
    return out.str();
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    const Stats stats = statsLocked();
    const MemoryUsage usage{
        .name = name(),
        .currentUsage = stats.currentBytes,
        .peakUsage = stats.peakBytes};
    out << usage.toString() << "\n";
  }

  MemoryUsageHeap topLeafMemUsages;
  visitChildren([&, indent = kCapMessageIndentSize](MemoryPool* pool) {
    capExceedingMessageVisitor(pool, indent, topLeafMemUsages, out);
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

  out << "\nFailed memory pool: " << requestor->name() << ": "
      << succinctBytes(requestor->currentBytes()) << "\n";
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
  VELOX_CHECK_GE(capacity_, reservationBytes_);
  return capacity_ - reservationBytes_;
}

uint64_t MemoryPoolImpl::shrink(uint64_t targetBytes) {
  if (parent_ != nullptr) {
    return parent_->shrink(targetBytes);
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

uint64_t MemoryPoolImpl::grow(uint64_t bytes) noexcept {
  if (parent_ != nullptr) {
    return parent_->grow(bytes);
  }
  std::lock_guard<std::mutex> l(mutex_);
  // We don't expect to grow a memory pool without capacity limit.
  VELOX_CHECK_NE(capacity_, kMaxMemory);
  capacity_ += bytes;
  VELOX_CHECK_GE(capacity_, bytes);
  return capacity_;
}

bool MemoryPoolImpl::aborted() const {
  if (parent_ != nullptr) {
    return parent_->aborted();
  }
  return aborted_;
}

void MemoryPoolImpl::abort() {
  if (parent_ != nullptr) {
    parent_->abort();
    return;
  }
  if (reclaimer_ == nullptr) {
    VELOX_FAIL("Can't abort the memory pool {} without reclaimer", name_);
  }
  aborted_ = true;
  reclaimer_->abort(this);
}

void MemoryPoolImpl::testingSetCapacity(int64_t bytes) {
  if (parent_ != nullptr) {
    return toImpl(parent_)->testingSetCapacity(bytes);
  }
  std::lock_guard<std::mutex> l(mutex_);
  capacity_ = bytes;
}
} // namespace facebook::velox::memory
