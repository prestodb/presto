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

#include "velox/common/memory/MemoryUsageTracker.h"

#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"

DECLARE_bool(velox_memory_leak_check_enabled);

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::memory {

std::shared_ptr<MemoryUsageTracker> MemoryUsageTracker::addChild(
    bool leafTracker,
    bool threadSafe) {
  VELOX_CHECK(
      !leafTracker_,
      "Can only create child usage tracker from a non-leaf memory usage tracker: {}",
      toString());
  VELOX_CHECK(
      leafTracker || threadSafe,
      "Can only create leaf memory usage tracker with thread-safe off");
  ++numChildren_;
  return create(
      shared_from_this(), leafTracker, threadSafe, kMaxMemory, checkUsageLeak_);
}

std::shared_ptr<MemoryUsageTracker> MemoryUsageTracker::create(
    const std::shared_ptr<MemoryUsageTracker>& parent,
    bool leafTracker,
    bool threadSafe,
    int64_t maxMemory,
    bool checkUsageLeak) {
  auto* tracker = new MemoryUsageTracker(
      parent, leafTracker, threadSafe, maxMemory, checkUsageLeak);
  return std::shared_ptr<MemoryUsageTracker>(tracker);
}

MemoryUsageTracker::~MemoryUsageTracker() {
  if (checkUsageLeak_) {
    VELOX_CHECK(
        (usedReservationBytes_ == 0) && (reservationBytes_ == 0) &&
            (minReservationBytes_ == 0),
        "Bad tracker state: {}",
        toString());
  }
}

void MemoryUsageTracker::update(int64_t size) {
  reservationCheck();

  if (size == 0) {
    return;
  }

  if (size > 0) {
    ++numAllocs_;
    reserve(size, false);
    return;
  }

  VELOX_CHECK_LT(size, 0);
  ++numFrees_;
  release(-size);
}

void MemoryUsageTracker::reserve(uint64_t size) {
  reservationCheck();
  if (size == 0) {
    return;
  }
  ++numReserves_;
  reserve(size, true);
}

void MemoryUsageTracker::reserve(uint64_t size, bool reserveOnly) {
  if (threadSafe_) {
    reserveThreadSafe(size, reserveOnly);
  } else {
    reserveNonThreadSafe(size, reserveOnly);
  }
}

void MemoryUsageTracker::reserveThreadSafe(uint64_t size, bool reserveOnly) {
  VELOX_CHECK(threadSafe_);
  VELOX_CHECK_GT(size, 0);

  int32_t numAttempts = 0;
  int64_t increment = 0;
  for (;; ++numAttempts) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      increment = reservationSizeNoLock(size);
      if (increment == 0) {
        if (reserveOnly) {
          minReservationBytes_ = reservationBytes_;
        } else {
          usedReservationBytes_ += size;
        }
        sanityCheckNoLock();
        break;
      }
    }
    TestValue::adjust(
        "facebook::velox::memory::MemoryUsageTracker::reserveThreadSafe", this);
    incrementReservationThreadSafe(increment);
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

void MemoryUsageTracker::reserveNonThreadSafe(uint64_t size, bool reserveOnly) {
  VELOX_CHECK(!threadSafe_);
  VELOX_CHECK_GT(size, 0);

  int32_t numAttempts{0};
  for (;; ++numAttempts) {
    int64_t increment = reservationSizeNoLock(size);
    if (FOLLY_LIKELY(increment == 0)) {
      if (FOLLY_UNLIKELY(reserveOnly)) {
        minReservationBytes_ = reservationBytes_;
      } else {
        usedReservationBytes_ += size;
      }
      sanityCheckNoLock();
      break;
    }
    incrementReservationNonThreadSafe(increment);
  }

  // NOTE: in case of concurrent reserve requests to the same root memory pool
  // from the other leaf memory pools, we might have to retry
  // incrementReservation(). This should happen rarely in production
  // as the leaf tracker does quantized memory reservation so that we don't
  // expect high concurrency at the root memory pool.
  if (FOLLY_UNLIKELY(numAttempts > 1)) {
    numCollisions_ += numAttempts - 1;
  }
}

bool MemoryUsageTracker::incrementReservationThreadSafe(uint64_t size) {
  VELOX_CHECK(threadSafe_);
  VELOX_CHECK_GT(size, 0);

  // Update parent first. If one of the ancestor's limits are exceeded, it will
  // throw MEM_CAP_EXCEEDED exception. This exception will be caught and
  // re-thrown with an additional message appended to it if a
  // makeMemoryCapExceededMessage_ is set.
  if (parent_ != nullptr) {
    try {
      if (!parent_->incrementReservationThreadSafe(size)) {
        return false;
      }
    } catch (const VeloxRuntimeError& e) {
      if (makeMemoryCapExceededMessage_ == nullptr) {
        throw;
      }
      auto errorMessage =
          e.message() + ". " + makeMemoryCapExceededMessage_(*this);
      VELOX_MEM_CAP_EXCEEDED(errorMessage);
    }
  }

  {
    std::lock_guard<std::mutex> l(mutex_);
    if (maybeIncrementReservationLocked(size)) {
      return true;
    }
  }
  VELOX_CHECK_NULL(parent_);

  if ((growCallback_ != nullptr) && growCallback_(size, *this)) {
    TestValue::adjust(
        "facebook::velox::memory::MemoryUsageTracker::incrementReservationThreadSafe::AfterGrowCallback",
        this);
    // NOTE: the memory reservation might still fail even if the memory grow
    // callback succeeds. The reason is that we don't hold the root tracker's
    // mutex lock while running the grow callback. Therefore, there is a
    // possibility in theory that a concurrent memory reservation request
    // might steal away the increased memory capacity after the grow callback
    // finishes and before we increase the reservation. If it happens, we can
    // simply fall back to retry the memory reservation from the leaf track
    // which should happen rarely.
    return maybeIncrementReservation(size);
  }

  std::lock_guard<std::mutex> l(mutex_);
  auto errorMessage = fmt::format(
      MEM_CAP_EXCEEDED_ERROR_FORMAT,
      succinctBytes(maxMemory_),
      succinctBytes(size));
  if (makeMemoryCapExceededMessage_) {
    errorMessage += ". " + makeMemoryCapExceededMessage_(*this);
  }
  VELOX_MEM_CAP_EXCEEDED(errorMessage);
}

bool MemoryUsageTracker::incrementReservationNonThreadSafe(uint64_t size) {
  VELOX_CHECK_NOT_NULL(parent_);
  VELOX_CHECK(!threadSafe_ && leafTracker_);
  try {
    if (!parent_->incrementReservationThreadSafe(size)) {
      return false;
    }
  } catch (const VeloxRuntimeError& e) {
    if (makeMemoryCapExceededMessage_ == nullptr) {
      throw;
    }
    auto errorMessage =
        e.message() + ". " + makeMemoryCapExceededMessage_(*this);
    VELOX_MEM_CAP_EXCEEDED(errorMessage);
  }

  reservationBytes_ += size;
  cumulativeBytes_ += size;
  maybeUpdatePeakBytesNoLock(reservationBytes_);
  return true;
}

void MemoryUsageTracker::release() {
  reservationCheck();
  ++numReleases_;
  release(0);
}

void MemoryUsageTracker::release(uint64_t size) {
  if (threadSafe_) {
    releaseThreadSafe(size);
  } else {
    releaseNonThreadSafe(size);
  }
}

void MemoryUsageTracker::releaseThreadSafe(uint64_t size) {
  int64_t freeable = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    int64_t newQuantized;
    if (size == 0) {
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
    sanityCheckNoLock();
  }
  if (freeable > 0) {
    // NOTE: we can only release memory from a leaf memory usage tracker.
    VELOX_DCHECK_NOT_NULL(parent_);
    parent_->decrementReservation(freeable);
  }
}

void MemoryUsageTracker::releaseNonThreadSafe(uint64_t size) {
  int64_t newQuantized;
  if (FOLLY_UNLIKELY(size == 0)) {
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

  const int64_t freeable = reservationBytes_ - newQuantized;
  if (FOLLY_UNLIKELY(freeable > 0)) {
    // NOTE: we can only release memory from a leaf memory usage tracker.
    VELOX_DCHECK_NOT_NULL(parent_);
    reservationBytes_ = newQuantized;
    sanityCheckNoLock();
    parent_->decrementReservation(freeable);
  }
}

bool MemoryUsageTracker::maybeIncrementReservation(uint64_t size) {
  std::lock_guard<std::mutex> l(mutex_);
  return maybeIncrementReservationLocked(size);
}

bool MemoryUsageTracker::maybeIncrementReservationLocked(uint64_t size) {
  if (parent_ != nullptr || (reservationBytes_ + size <= maxMemory_)) {
    reservationBytes_ += size;
    cumulativeBytes_ += size;
    maybeUpdatePeakBytesNoLock(reservationBytes_);
    return true;
  }
  return false;
}

void MemoryUsageTracker::decrementReservation(uint64_t size) noexcept {
  VELOX_CHECK_GT(size, 0);

  if (parent_ != nullptr) {
    parent_->decrementReservation(size);
  }
  std::lock_guard<std::mutex> l(mutex_);
  reservationBytes_ -= size;
  VELOX_CHECK_GE(reservationBytes_, 0, "decrement reservation size {}", size);
}

void MemoryUsageTracker::sanityCheckNoLock() const {
  if ((reservationBytes_ < usedReservationBytes_) ||
      (reservationBytes_ < minReservationBytes_) ||
      (usedReservationBytes_ < 0)) {
    VELOX_FAIL("Bad tracker state: {}", toStringNoLock());
  }
}

bool MemoryUsageTracker::maybeReserve(uint64_t increment) {
  reservationCheck();
  TestValue::adjust(
      "facebook::velox::memory::MemoryUsageTracker::maybeReserve", this);
  constexpr int32_t kGrowthQuantum = 8 << 20;
  const auto reservationToAdd = bits::roundUp(increment, kGrowthQuantum);
  try {
    reserve(reservationToAdd);
  } catch (const std::exception& e) {
    return false;
  }
  return true;
}

void MemoryUsageTracker::maybeUpdatePeakBytesNoLock(int64_t newPeak) {
  peakBytes_ = std::max(peakBytes_, newPeak);
}

std::string MemoryUsageTracker::toString() const {
  std::lock_guard<std::mutex> l(mutex_);
  return toStringNoLock();
}

std::string MemoryUsageTracker::toStringNoLock() const {
  std::stringstream out;
  out << "<tracker used " << succinctBytes(currentBytesLocked())
      << " available " << succinctBytes(availableReservationLocked());
  if (maxMemory() != kMaxMemory) {
    out << " limit " << succinctBytes(maxMemory());
  }
  out << " reservation [used " << succinctBytes(usedReservationBytes_)
      << ", reserved " << succinctBytes(reservationBytes_) << ", min "
      << succinctBytes(minReservationBytes_);
  out << "] counters [allocs " << numAllocs_ << ", frees " << numFrees_
      << ", reserves " << numReserves_ << ", releases " << numReleases_
      << ", collisions " << numCollisions_ << ", children " << numChildren_
      << "])";
  out << ">";
  return out.str();
}

int64_t MemoryUsageTracker::reservationSizeNoLock(int64_t size) {
  const int64_t neededSize = size - (reservationBytes_ - usedReservationBytes_);
  if (neededSize <= 0) {
    return 0;
  }
  return roundedDelta(reservationBytes_, neededSize);
}

std::string MemoryUsageTracker::Stats::toString() const {
  return fmt::format(
      "peakBytes:{} cumulativeBytes:{} numAllocs:{} numFrees:{} numReserves:{} numReleases:{} numCollisions:{} numChildren:{}",
      peakBytes,
      cumulativeBytes,
      numAllocs,
      numFrees,
      numReserves,
      numReleases,
      numCollisions,
      numChildren);
}

MemoryUsageTracker::Stats MemoryUsageTracker::stats() const {
  std::lock_guard<std::mutex> l(mutex_);
  Stats stats;
  stats.peakBytes = peakBytes_;
  stats.cumulativeBytes = cumulativeBytes_;
  stats.numAllocs = numAllocs_;
  stats.numFrees = numFrees_;
  stats.numReserves = numReserves_;
  stats.numReleases = numReleases_;
  stats.numCollisions = numCollisions_;
  stats.numChildren = numChildren_;
  return stats;
}

bool MemoryUsageTracker::Stats::operator==(
    const MemoryUsageTracker::Stats& other) const {
  return std::tie(
             peakBytes,
             cumulativeBytes,
             numAllocs,
             numFrees,
             numReserves,
             numReleases,
             numCollisions,
             numChildren) ==
      std::tie(
             other.peakBytes,
             other.cumulativeBytes,
             other.numAllocs,
             other.numFrees,
             other.numReserves,
             other.numReleases,
             other.numCollisions,
             other.numChildren);
}

std::ostream& operator<<(
    std::ostream& os,
    const MemoryUsageTracker::Stats& stats) {
  return os << stats.toString();
}
} // namespace facebook::velox::memory
