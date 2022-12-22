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

void MemoryUsageTracker::update(int64_t size) {
  if (size > 0) {
    int64_t increment = 0;
    ++numAllocs_;
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (usedReservationBytes_ + size > reservationBytes_) {
        increment = reserveLocked(size);
      }
    }
    checkAndPropagateReservationIncrement(increment, false);
    usedReservationBytes_.fetch_add(size);
    return;
  }

  // Decreasing usage. See if need to propagate upward.
  int64_t decrement = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    usedReservationBytes_ += size;
    auto newUsed = usedReservationBytes_.load();
    auto newCap = std::max(minReservationBytes_.load(), newUsed);
    auto newQuantized = quantizedSize(newCap);
    if (newQuantized != reservationBytes_) {
      decrement = reservationBytes_ - newQuantized;
      reservationBytes_ = newQuantized;
    }
  }
  if (decrement != 0) {
    decrementUsage(decrement);
  }
}

void MemoryUsageTracker::reserve(int64_t size) {
  int64_t increment;
  {
    std::lock_guard<std::mutex> l(mutex_);
    increment = reserveLocked(size);
    minReservationBytes_ = reservationBytes_.load();
  }
  checkAndPropagateReservationIncrement(increment, true);
}

void MemoryUsageTracker::release() {
  int64_t freeable = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (minReservationBytes_ == 0) {
      return;
    }
    const auto quantizedUsedBytes = quantizedSize(usedReservationBytes_);
    VELOX_CHECK_GE(reservationBytes_, quantizedUsedBytes);
    freeable = reservationBytes_ - quantizedUsedBytes;
    if (freeable > 0) {
      reservationBytes_ = quantizedUsedBytes;
    }
    minReservationBytes_ = 0;
  }
  if (freeable != 0) {
    decrementUsage(freeable);
  }
}

void MemoryUsageTracker::checkAndPropagateReservationIncrement(
    int64_t increment,
    bool updateMinReservation) {
  if (increment == 0) {
    return;
  }
  std::exception_ptr exception;
  try {
    incrementUsage(increment);
  } catch (std::exception& e) {
    exception = std::current_exception();
  }
  // Process the exception outside of catch so as not to kill the process if
  // 'mutex_' throws deadlock.
  if (exception != nullptr) {
    // Compensate for the increase after exceeding limit.
    std::lock_guard<std::mutex> l(mutex_);
    reservationBytes_ -= increment;
    if (updateMinReservation) {
      minReservationBytes_ -= increment;
    }
    std::rethrow_exception(exception);
  }
}

void MemoryUsageTracker::incrementUsage(int64_t size) {
  // Update parent first. If one of the ancestor's limits are exceeded, it will
  // throw MEM_CAP_EXCEEDED exception. This exception will be caught and
  // re-thrown with an additional message appended to it if a
  // makeMemoryCapExceededMessage_ is set.
  if (parent_ != nullptr) {
    try {
      parent_->incrementUsage(size);
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
      currentBytes_.fetch_add(size, std::memory_order_relaxed) + size;
  // Enforce the limit.
  if (newUsage > maxMemory_) {
    if ((growCallback_ == nullptr) || !growCallback_(size, *this)) {
      // Exceeded the limit.  revert the change to current usage.
      decrementUsage(size);
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

void MemoryUsageTracker::decrementUsage(int64_t size) noexcept {
  if (parent_ != nullptr) {
    parent_->decrementUsage(size);
  }
  currentBytes_.fetch_sub(size);
}

bool MemoryUsageTracker::maybeReserve(int64_t increment) {
  constexpr int32_t kGrowthQuantum = 8 << 20;
  const auto reservationToAdd = bits::roundUp(increment, kGrowthQuantum);
  MemoryUsageTracker* candidate = this;
  while (candidate != nullptr) {
    auto limit = candidate->maxMemory();
    // If this tracker has no limit, proceed to its parent.
    if (limit == kMaxMemory && candidate->parent_ != nullptr) {
      candidate = candidate->parent_.get();
      continue;
    }
    if (limit - candidate->currentBytes() >= reservationToAdd) {
      try {
        reserve(reservationToAdd);
      } catch (const std::exception& e) {
        return false;
      }
      return true;
    }
    candidate = candidate->parent_.get();
  }
  return false;
}

void MemoryUsageTracker::maybeUpdatePeakBytes(int64_t newPeak) {
  int64_t oldPeak = peakBytes_;
  while (oldPeak < newPeak &&
         !peakBytes_.compare_exchange_weak(oldPeak, newPeak)) {
    oldPeak = peakBytes_;
  }
}

void MemoryUsageTracker::checkNonNegativeSizes(const char* errmsg) const {
  // TODO: make this a CHECK failure after making usage tracker thread-safe.
  if (currentBytes_ < 0) {
    LOG_EVERY_N(ERROR, 100)
        << "MEMORY: Negagtive usage " << errmsg << " " << currentBytes_;
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

int64_t MemoryUsageTracker::reserveLocked(int64_t size) {
  const int64_t neededSize = size - (reservationBytes_ - usedReservationBytes_);
  if (neededSize <= 0) {
    return 0;
  }
  const auto increment = roundedDelta(reservationBytes_, neededSize);
  reservationBytes_ += increment;
  return increment;
}
} // namespace facebook::velox::memory
