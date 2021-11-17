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

#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <optional>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::memory {
constexpr int64_t kMaxMemory = std::numeric_limits<int64_t>::max();

#define VELOX_MEM_CAP_EXCEEDED()                                    \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemCapExceeded.c_str(),       \
      /* isRetriable */ true);

struct MemoryUsageConfig {
  std::optional<int64_t> maxUserMemory;
  std::optional<int64_t> maxSystemMemory;
  std::optional<int64_t> maxTotalMemory;
};

struct MemoryUsageConfigBuilder {
  MemoryUsageConfig config;

  MemoryUsageConfigBuilder(MemoryUsageConfig c = MemoryUsageConfig())
      : config(c) {}

  MemoryUsageConfigBuilder& maxUserMemory(int64_t max) {
    config.maxUserMemory = max;
    return *this;
  }

  MemoryUsageConfigBuilder& maxSystemMemory(int64_t max) {
    config.maxSystemMemory = max;
    return *this;
  }

  MemoryUsageConfigBuilder& maxTotalMemory(int64_t max) {
    config.maxTotalMemory = max;
    return *this;
  }

  MemoryUsageConfig build() {
    return config;
  }
};

// Keeps track of currently outstanding and peak outstanding memory
// and cumulative allocation volume for a MemoryPool or MappedMemory
// allocator. This is supplied at construction when creating a child
// MemoryPool or MappedMemory. These trackers form a tree. The
// tracking information is aggregated from leaf to root. Each level in
// the tree may specify a maximum allocation. Updating the allocated
// amount past the maximum will throw. This is why the update should
// precede the actual allocation. A tracker can specify a
// reservation. Making a reservation means that no memory is allocated
// but the allocated size is updated so as to make sure that at least
// the reserved amount can be allocated. Allocations that fit within
// the reserved are not counted as new because the counting was done
// when making the reservation.  Allocating past the reservation is
// possible and will increase the reservation by a size-dependent
// quantum. This may fail if the new size in some parent exceeds a
// cap. Freeing data within the reservation drops the usage but not
// the reservation.  Automatically updated reservations are freed when
// enough memory is freed.  Explicitly made reservations must be freed
// with release(). Parents are updated only at reservation time to
// avoid cache coherence traffic that arises when every thread
// constantly updates the same top level allocation counter.
class MemoryUsageTracker
    : public std::enable_shared_from_this<MemoryUsageTracker> {
 public:
  // Create default usage tracker. It aggregates both 'user' and 'system' memory
  // from its children and tracks the allocations as 'user' memory. It returns a
  // 'root' tracker.
  static std::shared_ptr<MemoryUsageTracker> create(
      const MemoryUsageConfig& config = MemoryUsageConfig()) {
    return create(nullptr, UsageType::kUserMem, config);
  }

  // FIXME(venkatra): Remove this once presto_cpp is updated to use above
  // function.
  static std::shared_ptr<MemoryUsageTracker> create(
      int64_t maxUserMemory,
      int64_t maxSystemMemory,
      int64_t maxTotalMemory) {
    return create(
        nullptr,
        UsageType::kUserMem,
        MemoryUsageConfigBuilder()
            .maxUserMemory(maxUserMemory)
            .maxSystemMemory(maxSystemMemory)
            .maxTotalMemory(maxTotalMemory)
            .build());
  }

  // Increments the reservation for 'this' so that we can allocate
  // at least 'size' bytes on top of the current allocation. This is
  // used when an a memory user needs to allocate more memory and
  // needs a guarantee of at least 'size' being available. If less
  // memory ends up being needed, the unused reservation should be released with
  // release().
  //   If the new reserved amount exceeds the
  // usage limit, an exception will be thrown.  Note that this
  // function is not thread-safe. We reserve on a
  // leaf memory pool or mapped memory, which is accessed within a
  // single thread.
  void reserve(int64_t size) {
    std::lock_guard<std::mutex> l(mutex_);

    minReservation_ += reserveLocked(size);
  }

  // Release unused reservation. Used reservation will be released as the
  // allocations are freed.
  void release() {
    std::lock_guard<std::mutex> l(mutex_);
    int64_t remaining = reservation_ - usedReservation_;
    if (remaining) {
      updateInternal(type_, -remaining);
    }
    reservation_ = 0;
    minReservation_ = 0;
    usedReservation_ = 0;
  }

  // Increments outstanding memory by 'size', which is positive for
  // allocation and negative for free. If there is no reservation or
  // the new allocated amount exceeds the reservation, propagates the
  // change upward.
  void update(int64_t size) {
    std::lock_guard<std::mutex> l(mutex_);
    if (size > 0) {
      if (usedReservation_ + size > reservation_) {
        reserveLocked(size);
      }
      usedReservation_ += size;
      return;
    }
    auto newUsed = usedReservation_ + size;
    auto newCap = std::max(minReservation_, newUsed);
    auto newQuantized = quantizedSize(newCap);
    if (newQuantized != reservation_) {
      auto increment = newQuantized - reservation_;
      updateInternal(type_, increment);
      reservation_ = newQuantized;
    }
    usedReservation_ += size;
  }

  int64_t getCurrentUserBytes() const {
    return adjustByReservation(user(currentUsageInBytes_));
  }
  int64_t getCurrentSystemBytes() const {
    return adjustByReservation(system(currentUsageInBytes_));
  }
  int64_t getCurrentTotalBytes() const {
    return adjustByReservation(
        user(currentUsageInBytes_) + system(currentUsageInBytes_));
  }

  int64_t getPeakUserBytes() const {
    return user(peakUsageInBytes_);
  }
  int64_t getPeakSystemBytes() const {
    return system(peakUsageInBytes_);
  }
  int64_t getPeakTotalBytes() const {
    return total(peakUsageInBytes_);
  }

  int64_t getAvailableReservation() const {
    return std::max<int64_t>(0, reservation_ - usedReservation_);
  }

  int64_t getNumAllocs() const {
    return total(numAllocs_);
  }

  int64_t getCumulativeBytes() const {
    return total(cumulativeBytes_);
  }

  std::shared_ptr<MemoryUsageTracker> addChild(
      bool trackSystemMem = false,
      const MemoryUsageConfig& config = MemoryUsageConfig()) {
    return create(
        shared_from_this(),
        trackSystemMem ? UsageType::kSystemMem : UsageType::kUserMem,
        config);
  }

  int64_t getUserMemoryCap() const {
    return user(maxMemory_);
  }

 private:
  static constexpr int64_t kMB = 1 << 20;

  enum class UsageType : int { kUserMem = 0, kSystemMem = 1, kTotalMem = 2 };

  template <typename T, size_t size>
  static T& usage(std::array<T, size>& array, UsageType type) {
    return array[static_cast<int32_t>(type)];
  }

  template <typename T, size_t size>
  static T& user(std::array<T, size>& array) {
    return usage(array, UsageType::kUserMem);
  }

  template <typename T, size_t size>
  static T& system(std::array<T, size>& array) {
    return usage(array, UsageType::kSystemMem);
  }

  template <typename T, size_t size>
  static T& total(std::array<T, size>& array) {
    return usage(array, UsageType::kTotalMem);
  }

  template <typename T, size_t size>
  static int64_t usage(const std::array<T, size>& array, UsageType type) {
    return array[static_cast<int32_t>(type)];
  }

  template <typename T, size_t size>
  static int64_t user(const std::array<T, size>& array) {
    return usage(array, UsageType::kUserMem);
  }

  template <typename T, size_t size>
  static int64_t system(const std::array<T, size>& array) {
    return usage(array, UsageType::kSystemMem);
  }

  template <typename T, size_t size>
  static int64_t total(const std::array<T, size>& array) {
    return usage(array, UsageType::kTotalMem);
  }

  int64_t reserveLocked(int64_t size) {
    int64_t neededSize = size - (reservation_ - usedReservation_);
    if (neededSize > 0) {
      auto increment = roundedDelta(reservation_, neededSize);
      updateInternal(type_, increment);
      reservation_ += increment;
      return increment;
    }
    return 0;
  }

  // Returns a rounded up delta based on adding 'delta' to
  // 'size'. Adding the rounded delta to 'size' will result in 'size'
  // a quantized size, rounded to the MB or 8MB for larger sizes.
  static int64_t roundedDelta(int64_t size, int64_t delta) {
    return quantizedSize(size + delta) - size;
  }

  // Returns the next higher quantized size. Small sizes are at MB granularity,
  // larger ones at coarser granularity.
  static uint64_t quantizedSize(uint64_t size) {
    if (size < 16 * kMB) {
      return bits::roundUp(size, kMB);
    }
    if (size < 64 * kMB) {
      return bits::roundUp(size, 4 * kMB);
    }
    return bits::roundUp(size, 8 * kMB);
  }

  int64_t adjustByReservation(int64_t total) const {
    return reservation_
        ? std::max<int64_t>(total - getAvailableReservation(), 0)
        : std::max<int64_t>(0, total);
  }

  // Serializes update(). UpdateInternal works based on atomics so
  // children updating the same parent do not have to be serialized
  // but multiple threads updating the same leaf must be serialized
  // because the reservation decision needs a consistent read/write of
  // both reservation and used reservation.
  std::mutex mutex_;
  std::shared_ptr<MemoryUsageTracker> parent_;
  UsageType type_;
  std::array<std::atomic<int64_t>, 2> currentUsageInBytes_{};
  std::array<std::atomic<int64_t>, 3> peakUsageInBytes_{};
  std::array<int64_t, 3> maxMemory_;
  std::array<int64_t, 3> numAllocs_{};
  std::array<int64_t, 3> cumulativeBytes_{};

  int64_t reservation_{0};

  // Minimum amount of reserved memory to hold until explicit release().
  int64_t minReservation_{0};
  std::atomic<int64_t> usedReservation_{};

  explicit MemoryUsageTracker(
      const std::shared_ptr<MemoryUsageTracker>& parent,
      UsageType type,
      const MemoryUsageConfig& config)
      : parent_(parent),
        type_(type),
        maxMemory_{
            config.maxUserMemory.value_or(kMaxMemory),
            config.maxSystemMemory.value_or(kMaxMemory),
            config.maxTotalMemory.value_or(kMaxMemory)} {}

  static std::shared_ptr<MemoryUsageTracker> create(
      const std::shared_ptr<MemoryUsageTracker>& parent,
      UsageType type,
      const MemoryUsageConfig& config);

  void maySetMax(UsageType type, int64_t newPeak) {
    auto& peakUsage = peakUsageInBytes_[static_cast<int>(type)];
    int64_t oldPeak = peakUsage;
    while (oldPeak < newPeak &&
           !peakUsage.compare_exchange_weak(oldPeak, newPeak)) {
      oldPeak = peakUsage;
    }
  }

  void updateInternal(UsageType type, int64_t size);

  void checkNonNegativeSizes(const char* message) const {
    if (user(currentUsageInBytes_) < 0 || system(currentUsageInBytes_) < 0 ||
        total(currentUsageInBytes_) < 0) {
      LOG_EVERY_N(ERROR, 100)
          << "MEMR: Negative usage " << message << user(currentUsageInBytes_)
          << " " << system(currentUsageInBytes_) << " "
          << total(currentUsageInBytes_);
    }
  }
};
} // namespace facebook::velox::memory
