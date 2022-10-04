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
    MemoryUsageTracker::UsageType type,
    const MemoryUsageConfig& config) {
  struct SharedMemoryUsageTracker : public MemoryUsageTracker {
    SharedMemoryUsageTracker(
        const std::shared_ptr<MemoryUsageTracker>& parent,
        MemoryUsageTracker::UsageType type,
        const MemoryUsageConfig& config)
        : MemoryUsageTracker(parent, type, config) {}
  };

  return std::make_shared<SharedMemoryUsageTracker>(parent, type, config);
}

void MemoryUsageTracker::update(int64_t size, bool /* mock */) {
  if (size > 0) {
    int64_t increment = 0;
    ++usage(numAllocs_, UsageType::kTotalMem);
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (usedReservation_ + size > reservation_) {
        increment = reserveLocked(size);
      }
    }
    checkAndPropagateReservationIncrement(increment, false);
    usedReservation_.fetch_add(size);
    return;
  }
  // Decreasing usage. See if need to propagate upward.
  int64_t decrement = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto newUsed = usedReservation_ += size;
    auto newCap = std::max(minReservation_.load(), newUsed);
    auto newQuantized = quantizedSize(newCap);
    if (newQuantized != reservation_) {
      decrement = reservation_ - newQuantized;
      reservation_ = newQuantized;
    }
  }
  if (decrement) {
    decrementUsage(type_, decrement);
  }
}

void MemoryUsageTracker::reserve(int64_t size) {
  int64_t increment;
  {
    std::lock_guard<std::mutex> l(mutex_);
    increment = reserveLocked(size);
    minReservation_ = reservation_.load();
  }
  if (increment) {
    checkAndPropagateReservationIncrement(increment, true);
  }
}

void MemoryUsageTracker::release() {
  if (!minReservation_) {
    return;
  }
  int64_t freeable;
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto reservationByUsage = quantizedSize(usedReservation_);
    freeable = reservation_ - reservationByUsage;
    if (freeable > 0) {
      reservation_ = reservationByUsage;
    }
    minReservation_ = 0;
  }
  if (freeable) {
    decrementUsage(type_, freeable);
  }
}

void MemoryUsageTracker::checkAndPropagateReservationIncrement(
    int64_t increment,
    bool updateMinReservation) {
  if (!increment) {
    return;
  }
  std::exception_ptr exception;
  try {
    incrementUsage(type_, increment);
  } catch (std::exception& e) {
    exception = std::current_exception();
  }
  // Process the exception outside of catch so as not to kill the process if
  // 'mutex_' throws deadlock.
  if (exception) {
    // Compensate for the increase after exceeding limit.
    std::lock_guard<std::mutex> l(mutex_);
    reservation_ -= increment;
    if (updateMinReservation) {
      minReservation_ -= increment;
    }
    std::rethrow_exception(exception);
  }
}

void MemoryUsageTracker::incrementUsage(UsageType type, int64_t size) {
  // Update parent first. If one of the ancestor's limits are exceeded, it
  // will throw MEM_CAP_EXCEEDED exception. This exception will be caught
  // and re-thrown with an additional message appended to it if a
  // makeMemoryCapExceededMessage_ is set.
  if (parent_) {
    try {
      parent_->incrementUsage(type, size);
    } catch (const VeloxRuntimeError& e) {
      if (!makeMemoryCapExceededMessage_) {
        throw;
      }
      auto errorMessage =
          e.message() + ". " + makeMemoryCapExceededMessage_(*this);
      VELOX_MEM_CAP_EXCEEDED(errorMessage);
    }
  }
  auto newUsage = usage(currentUsageInBytes_, type)
                      .fetch_add(size, std::memory_order_relaxed) +
      size;

  // We track the peak usage of total memory independent of user and
  // system memory since freed user memory can be reallocated as system
  // memory and vice versa.
  auto otherUsageType =
      type == UsageType::kUserMem ? UsageType::kSystemMem : UsageType::kUserMem;
  int64_t totalBytes = newUsage + usage(currentUsageInBytes_, otherUsageType);

  // Enforce the limit.
  if (newUsage > usage(maxMemory_, type) || totalBytes > total(maxMemory_)) {
    if (!growCallback_ || !growCallback_(type, size, *this)) {
      // Exceeded the limit.  revert the change to current usage.
      decrementUsage(type, size);
      checkNonNegativeSizes("after exceeding cap");
      auto errorMessage = fmt::format(
          MEM_CAP_EXCEEDED_ERROR_FORMAT,
          succinctBytes(std::min(total(maxMemory_), usage(maxMemory_, type))),
          succinctBytes(size));
      if (makeMemoryCapExceededMessage_) {
        errorMessage += ". " + makeMemoryCapExceededMessage_(*this);
      }
      VELOX_MEM_CAP_EXCEEDED(errorMessage);
    }
  }
  usage(cumulativeBytes_, type) += size;
  maySetMax(type, newUsage);
  maySetMax(UsageType::kTotalMem, totalBytes);
  checkNonNegativeSizes("after update");
}

void MemoryUsageTracker::decrementUsage(UsageType type, int64_t size) noexcept {
  if (parent_) {
    parent_->decrementUsage(type, size);
  }
  usage(currentUsageInBytes_, type).fetch_sub(size);
}

bool MemoryUsageTracker::maybeReserve(int64_t increment) {
  constexpr int32_t kGrowthQuantum = 8 << 20;
  auto addedReservation = bits::roundUp(increment, kGrowthQuantum);
  auto candidate = this;
  while (candidate) {
    auto limit = candidate->maxTotalBytes();
    // If this tracker has no limit, proceed to its parent.
    if (limit == memory::kMaxMemory && candidate->parent_) {
      candidate = candidate->parent_.get();
      continue;
    }
    if (limit - candidate->getCurrentTotalBytes() > addedReservation) {
      try {
        reserve(addedReservation);
      } catch (const std::exception& e) {
        return false;
      }
      return true;
    }
    candidate = candidate->parent_.get();
  }
  return false;
}

void MemoryUsageTracker::maySetMax(UsageType type, int64_t newPeak) {
  auto& peakUsage = peakUsageInBytes_[static_cast<int>(type)];
  int64_t oldPeak = peakUsage;
  while (oldPeak < newPeak &&
         !peakUsage.compare_exchange_weak(oldPeak, newPeak)) {
    oldPeak = peakUsage;
  }
}

void MemoryUsageTracker::checkNonNegativeSizes(
    const char* FOLLY_NONNULL message) const {
  if (user(currentUsageInBytes_) < 0 || system(currentUsageInBytes_) < 0 ||
      total(currentUsageInBytes_) < 0) {
    LOG_EVERY_N(ERROR, 100)
        << "MEMR: Negative usage " << message << user(currentUsageInBytes_)
        << " " << system(currentUsageInBytes_) << " "
        << total(currentUsageInBytes_);
  }
}

std::string MemoryUsageTracker::toString() const {
  std::stringstream out;
  out << "<tracker total " << (getCurrentTotalBytes() >> 20) << " available "
      << (getAvailableReservation() >> 20);
  if (maxTotalBytes() != kMaxMemory) {
    out << "limit " << (maxTotalBytes() >> 20);
  }
  out << ">";
  return out.str();
}

SimpleMemoryTracker::SimpleMemoryTracker(const MemoryUsageConfig& config)
    : MemoryUsageTracker{nullptr, UsageType::kUserMem, config},
      userMemoryQuota_{config.maxUserMemory.value_or(kMaxMemory)} {}

// Simple memory tracker wants to be accurate for its memory accounting, so
// it ignores mock updates.
void SimpleMemoryTracker::update(int64_t size, bool mock) {
  if (mock) {
    return;
  }
  int64_t previousUsage =
      totalUserMemory_.fetch_add(size, std::memory_order_relaxed);
  if (previousUsage + size > userMemoryQuota_) {
    VELOX_MEM_CAP_EXCEEDED(userMemoryQuota_);
  }
}

int64_t SimpleMemoryTracker::getCurrentUserBytes() const {
  return totalUserMemory_.load(std::memory_order_relaxed);
}

/* static */ std::shared_ptr<SimpleMemoryTracker> SimpleMemoryTracker::create(
    const MemoryUsageConfig& config) {
  return std::make_shared<SimpleMemoryTracker>(config);
}
} // namespace facebook::velox::memory
