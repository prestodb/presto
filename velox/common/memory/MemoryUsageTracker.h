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

#define VELOX_MEM_CAP_EXCEEDED(cap)                                 \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemCapExceeded.c_str(),       \
      /* isRetriable */ true,                                       \
      "Exceeded memory cap of {} MB",                               \
      (cap) / 1024 / 1024);

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
  enum class UsageType : int { kUserMem = 0, kSystemMem = 1, kTotalMem = 2 };

  // Function to increase a MemoryUsageTracker's limits. This is called when an
  // allocation would exceed the tracker's size limit. The usage in
  // 'tracker' at the time of call is as if the allocation had
  // succeeded.
  // If this returns true, this must set
  // the limits to be >= the usage in 'tracker'. If this returns
  // false, this should not modify 'tracker'. The caller will revert
  // the allocation that invoked this and signal an error.
  //
  // This may be called on one tracker from several threads. This is responsible
  // for serializing these. When this is called, the 'tracker's 'mutex_' must
  // not be held by the caller. A GrowCallback must not throw.
  using GrowCallback = std::function<
      bool(UsageType type, int64_t size, MemoryUsageTracker& tracker)>;

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

  // Increments the reservation for 'this' so that we can allocate at
  // least 'size' bytes on top of the current allocation. This is used
  // when an a memory user needs to allocate more memory and needs a
  // guarantee of at least 'size' being available. If less memory ends
  // up being needed, the unused reservation should be released with
  // release().  If the new reserved amount exceeds the usage limit,
  // an exception will be thrown.
  void reserve(int64_t size) {
    int64_t increment;
    {
      std::lock_guard<std::mutex> l(mutex_);
      increment = reserveLocked(size);
      minReservation_ += increment;
    }
    if (increment) {
      checkAndPropagateReservationIncrement(increment, true);
    }
  }

  // Release unused reservation. Used reservation will be released as the
  // allocations are freed.
  void release() {
    int64_t remaining;
    {
      std::lock_guard<std::mutex> l(mutex_);
      remaining = reservation_ - usedReservation_;
      reservation_ = 0;
      minReservation_ = 0;
      usedReservation_ = 0;
    }
    if (remaining) {
      decrementUsage(type_, remaining);
    }
  }

  // Increments outstanding memory by 'size', which is positive for
  // allocation and negative for free. If there is no reservation or
  // the new allocated amount exceeds the reservation, propagates the
  // change upward.
  void update(int64_t size) {
    if (size > 0) {
      int64_t increment = 0;
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
      auto newCap = std::max(minReservation_, newUsed);
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

  // Returns the total size including unused reservation.
  int64_t totalReservedBytes() {
    return user(currentUsageInBytes_) + system(currentUsageInBytes_);
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

  void updateConfig(const MemoryUsageConfig& config) {
    if (config.maxUserMemory.has_value()) {
      usage(maxMemory_, UsageType::kUserMem) = config.maxUserMemory.value();
    }
    if (config.maxSystemMemory.has_value()) {
      usage(maxMemory_, UsageType::kSystemMem) = config.maxSystemMemory.value();
    }
    if (config.maxTotalMemory.has_value()) {
      usage(maxMemory_, UsageType::kTotalMem) = config.maxTotalMemory.value();
    }
  }

  int64_t maxTotalBytes() const {
    return usage(maxMemory_, UsageType::kTotalMem);
  }

  void setGrowCallback(GrowCallback func) {
    growCallback_ = func;
  }

  /// Checks if it is likely that the reservation on 'this' can be
  /// incremented by 'increment'. Returns false if this seems
  /// unlikely. Otherwise attempts the reservation increment and returns
  /// true if succeeded.
  bool maybeReserve(int64_t increment);

 private:
  static constexpr int64_t kMB = 1 << 20;

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

  // Increments usage of 'this' and parents. Must be called without
  // holding 'mutex_'. Reverts the increment of reservation on before
  // rethrowing the error. If 'updateMinReservation' is true, also
  // decrements 'minReservation_' on error.
  void checkAndPropagateReservationIncrement(
      int64_t increment,
      bool updateMinReservation);

  // Serializes update(). decrease/increaseUsage work based on atomics so
  // children updating the same parent do not have to be serialized
  // but multiple threads updating the same leaf must be serialized
  // because the reservation decision needs a consistent read/write of
  // both reservation and used reservation.
  std::mutex mutex_;
  std::shared_ptr<MemoryUsageTracker> parent_;
  UsageType type_;
  std::array<std::atomic<int64_t>, 3> currentUsageInBytes_{};
  std::array<std::atomic<int64_t>, 3> peakUsageInBytes_{};
  std::array<int64_t, 3> maxMemory_;
  std::array<std::atomic<int64_t>, 3> numAllocs_{};
  std::array<std::atomic<int64_t>, 3> cumulativeBytes_{};

  int64_t reservation_{0};

  // Minimum amount of reserved memory to hold until explicit release().
  int64_t minReservation_{0};
  std::atomic<int64_t> usedReservation_{};

  GrowCallback growCallback_{};

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

  // Incrementss the reservation of 'type' and checks against
  // limits. Calls 'growCallback_' if this is set and limit
  // exceeded. Should be called without holding 'mutex_'. This throws if a limit
  // is exceeded and there is no corresponding GrowCallback or the GrowCallback
  // fails.
  void incrementUsage(UsageType type, int64_t size);

  //  Decrements usage in 'this' and parents.
  void decrementUsage(UsageType type, int64_t size) noexcept;

  void checkNonNegativeSizes(const char* FOLLY_NONNULL message) const {
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
