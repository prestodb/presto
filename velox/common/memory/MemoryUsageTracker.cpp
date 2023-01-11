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

namespace facebook::velox::memory {
std::shared_ptr<MemoryUsageTracker> MemoryUsageTracker::create(
    const std::shared_ptr<MemoryUsageTracker>& parent,
    int64_t maxMemory) {
  auto* tracker = new MemoryUsageTracker(parent, maxMemory);
  return std::shared_ptr<MemoryUsageTracker>(tracker);
}

MemoryUsageTracker::~MemoryUsageTracker() {
  VELOX_CHECK_EQ(usedReservationBytes_, 0);
  VELOX_CHECK_EQ(reservationBytes_, 0);
  VELOX_DCHECK_EQ(grantedReservationBytes_, 0);
  VELOX_DCHECK_EQ(minReservationBytes_, 0);
}

void MemoryUsageTracker::update(int64_t size) {
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
  if (size == 0) {
    return;
  }
  ++numReserves_;
  reserve(size, true);
}

void MemoryUsageTracker::reserve(uint64_t size, bool reserveOnly) {
  VELOX_CHECK_GT(size, 0);

  int32_t numAttempts = 0;
  int64_t increment = 0;
  for (;; ++numAttempts) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      grantedReservationBytes_ += increment;
      increment = reservationSizeLocked(size);
      if (increment == 0) {
        if (reserveOnly) {
          minReservationBytes_ = grantedReservationBytes_.load();
        } else {
          usedReservationBytes_ += size;
        }
        sanityCheckLocked();
        break;
      }
    }
    incrementReservation(increment);
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

void MemoryUsageTracker::release() {
  ++numReleases_;
  release(0);
}

void MemoryUsageTracker::release(uint64_t size) {
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
      const int64_t newUsed = usedReservationBytes_;
      const int64_t newCap = std::max(minReservationBytes_.load(), newUsed);
      newQuantized = quantizedSize(newCap);
    }
    VELOX_CHECK_GE(grantedReservationBytes_, newQuantized);
    freeable = grantedReservationBytes_ - newQuantized;
    if (freeable > 0) {
      grantedReservationBytes_ = newQuantized;
    }
    sanityCheckLocked();
  }
  if (freeable != 0) {
    decrementReservation(freeable);
  }
}

void MemoryUsageTracker::incrementReservation(uint64_t size) {
  VELOX_CHECK_GT(size, 0);

  // Update parent first. If one of the ancestor's limits are exceeded, it will
  // throw MEM_CAP_EXCEEDED exception. This exception will be caught and
  // re-thrown with an additional message appended to it if a
  // makeMemoryCapExceededMessage_ is set.
  if (parent_ != nullptr) {
    try {
      parent_->incrementReservation(size);
    } catch (const VeloxRuntimeError& e) {
      if (makeMemoryCapExceededMessage_ == nullptr) {
        throw;
      }
      auto errorMessage =
          e.message() + ". " + makeMemoryCapExceededMessage_(*this);
      VELOX_MEM_CAP_EXCEEDED(errorMessage);
    }
  }

  const auto newUsage =
      reservationBytes_.fetch_add(size, std::memory_order_relaxed) + size;
  VELOX_DCHECK(parent_ == nullptr || growCallback_ == nullptr);
  // Enforce the limit.
  if (parent_ == nullptr && newUsage > maxMemory_) {
    if ((growCallback_ == nullptr) || !growCallback_(size, *this)) {
      // Exceeded the limit.  revert the change to current usage.
      decrementReservation(size);
      checkNonNegativeSizes("after exceeding cap");
      auto errorMessage = fmt::format(
          MEM_CAP_EXCEEDED_ERROR_FORMAT,
          succinctBytes(maxMemory_),
          succinctBytes(size));
      if (makeMemoryCapExceededMessage_) {
        errorMessage += ". " + makeMemoryCapExceededMessage_(*this);
      }
      VELOX_MEM_CAP_EXCEEDED(errorMessage);
    }
  }
  cumulativeBytes_ += size;
  maybeUpdatePeakBytes(newUsage);
  checkNonNegativeSizes("after update");
}

void MemoryUsageTracker::decrementReservation(uint64_t size) noexcept {
  VELOX_CHECK_GT(size, 0);

  if (parent_ != nullptr) {
    parent_->decrementReservation(size);
  }
  reservationBytes_ -= size;
}

void MemoryUsageTracker::sanityCheckLocked() const {
  VELOX_CHECK_GE(grantedReservationBytes_, usedReservationBytes_);
  VELOX_CHECK_GE(grantedReservationBytes_, minReservationBytes_);
}

bool MemoryUsageTracker::maybeReserve(uint64_t increment) {
  constexpr int32_t kGrowthQuantum = 8 << 20;
  const auto reservationToAdd = bits::roundUp(increment, kGrowthQuantum);
  try {
    reserve(reservationToAdd);
  } catch (const std::exception& e) {
    return false;
  }
  return true;
}

void MemoryUsageTracker::maybeUpdatePeakBytes(int64_t newPeak) {
  int64_t oldPeak = peakBytes_;
  while (oldPeak < newPeak &&
         !peakBytes_.compare_exchange_weak(oldPeak, newPeak)) {
    oldPeak = peakBytes_;
  }
}

void MemoryUsageTracker::checkNonNegativeSizes(const char* errmsg) const {
  // TODO: make these CHECK failures after making usage tracker thread-safe.
  if (reservationBytes_ < 0) {
    LOG_EVERY_N(ERROR, 100) << "MEMORY: Negagtive reservation bytes " << errmsg
                            << " " << reservationBytes_;
  }
}

std::string MemoryUsageTracker::toString() const {
  std::stringstream out;
  out << "<tracker used " << (currentBytes() >> 20) << " available "
      << (availableReservation() >> 20);
  if (maxMemory() != kMaxMemory) {
    out << " limit " << (maxMemory() >> 20);
  }
  out << ">";
  return out.str();
}

int64_t MemoryUsageTracker::reservationSizeLocked(int64_t size) {
  const int64_t neededSize =
      size - (grantedReservationBytes_ - usedReservationBytes_);
  if (neededSize <= 0) {
    return 0;
  }
  return roundedDelta(grantedReservationBytes_, neededSize);
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
