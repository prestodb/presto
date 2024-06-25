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

#include <stdint.h>
#include <mutex>
#include <random>

#include "velox/common/caching/CachedFactory.h"
#include "velox/common/caching/SimpleLRUCache.h"
#include "velox/common/io/IoStatistics.h"

#pragma once

namespace facebook::velox::dwio::common {

/// A throttler that can be used to backoff IO when the storage is overloaded.
class Throttler {
 public:
  /// The configuration of the throttler.
  struct Config {
    /// If true, enables throttling of IO.
    bool throttleEnabled;

    /// The minimum backoff duration in milliseconds.
    uint64_t minThrottleBackoffMs;

    /// The maximum backoff duration in milliseconds.
    uint64_t maxThrottleBackoffMs;

    /// The backoff duration scale factor.
    double backoffScaleFactor;

    /// The minimum number of received local throttled signals before starting
    /// backoff.
    uint32_t minLocalThrottledSignals;

    /// The minimum number of received global throttled signals before starting
    /// backoff.
    uint32_t minGlobalThrottledSignals;

    /// The maximum number of entries in the throttled signal cache. There is
    /// one cache for each throttle signal type. For local throttle signal
    /// cache, each cache entry corresponds to a unqiue file direcotry in a
    /// storage system. For global throttle signal cache, each entry corresponds
    /// to a unique storage system.
    uint32_t maxCacheEntries;

    /// The TTL of the throttled signal cache entries in milliseconds. We only
    /// track the recently throtted signals.
    uint32_t cacheTTLMs;

    static constexpr bool kThrottleEnabledDefault{true};
    static constexpr uint64_t kMinThrottleBackoffMsDefault{200};
    static constexpr uint64_t kMaxThrottleBackoffMsDefault{30'000};
    static constexpr double kBackoffScaleFactorDefault{2.0};
    static constexpr uint32_t kMinLocalThrottledSignalsDefault{1'000};
    static constexpr uint32_t kMinGlobalThrottledSignalsDefault{100'000};
    static constexpr uint32_t kMaxCacheEntriesDefault{10'000};
    static constexpr uint32_t kCacheTTLMsDefault{3 * 60 * 1'000};

    Config(
        bool throttleEnabled = kThrottleEnabledDefault,
        uint64_t minThrottleBackoffMs = kMinThrottleBackoffMsDefault,
        uint64_t maxThrottleBackoffMs = kMaxThrottleBackoffMsDefault,
        double backoffScaleFactor = kBackoffScaleFactorDefault,
        uint32_t minLocalThrottledSignals = kMinLocalThrottledSignalsDefault,
        uint32_t minGlobalThrottledSignals = kMinGlobalThrottledSignalsDefault,
        uint32_t maxCacheEntries = kMaxCacheEntriesDefault,
        uint32_t cacheTTLMs = kCacheTTLMsDefault);

    std::string toString() const;
  };

  /// The throttler stats.
  struct Stats {
    std::atomic_uint64_t localThrottled{0};
    std::atomic_uint64_t globalThrottled{0};
    /// Counts the backoff delay in milliseconds.
    io::IoCounter backOffDelay;
  };

  static void init(const Config& config);

  static Throttler* instance();

  /// The type of throttle signal type.
  enum class SignalType {
    /// No throttled signal.
    kNone,
    /// A file directory throttled signal.
    kLocal,
    /// A cluster-wise throttled signal.
    kGlobal,
  };
  static std::string signalTypeName(SignalType type);

  /// Invoked to backoff when received a throttled signal on a particular
  /// storage location. 'type' specifies the throttled signal type received from
  /// the storage system. 'cluster' specifies the storage system. A query system
  /// might access data from different storage systems. 'directory' specifies
  /// the file directory within the storage system. The function returns the
  /// actual throttled duration in milliseconds. It returns zero if not
  /// throttled.
  uint64_t throttleBackoff(
      SignalType type,
      const std::string& cluster,
      const std::string& directory);

  const Stats& stats() const {
    return stats_;
  }

  static void testingReset() {
    instanceRef().reset();
  }

 private:
  static folly::SharedMutex& instanceLock() {
    static folly::SharedMutex mu;
    return mu;
  }

  static std::unique_ptr<Throttler>& instanceRef() {
    static std::unique_ptr<Throttler> instance;
    return instance;
  }

  explicit Throttler(const Config& config);

  bool throttleEnabled() const {
    return throttleEnabled_;
  }

  // Calculates the delay in milliseconds with exponential backoff for a storage
  // location, using the signal counters in cache and flags in config, and
  // update the throttle signal caches.
  uint64_t calculateBackoffDurationAndUpdateThrottleCache(
      SignalType type,
      const std::string& cluster,
      const std::string& directory);

  struct ThrottleSignal {
    uint64_t count{0};

    explicit ThrottleSignal(uint64_t _count) : count(_count) {}
  };

  // Creates ThrottleSignal via the Generator interface the CachedFactory
  // requires.
  class ThrottleSignalGenerator {
   public:
    ThrottleSignalGenerator() = default;

    std::unique_ptr<ThrottleSignal> operator()(
        const std::string& /*unused*/,
        const void* /*unused*/);
  };

  using CachedThrottleSignalPtr = CachedPtr<std::string, ThrottleSignal>;

  using ThrottleSignalFactory = facebook::velox::
      CachedFactory<std::string, ThrottleSignal, ThrottleSignalGenerator>;

  void updateThrottleCacheLocked(
      SignalType type,
      const std::string& cluster,
      const std::string& directory,
      CachedThrottleSignalPtr& localSignal,
      CachedThrottleSignalPtr& globalSignal);

  void updateThrottleStats(SignalType type, uint64_t backoffDelayMs);

  static const uint64_t kNoBackOffMs_{0};

  const bool throttleEnabled_;
  const uint64_t minThrottleBackoffDurationMs_;
  const uint64_t maxThrottleBackoffDurationMs_;
  const double backoffScaleFactor_;
  const uint32_t minLocalThrottledSignalsToBackoff_;
  const uint32_t minGlobalThrottledSignalsToBackoff_;
  const std::unique_ptr<ThrottleSignalFactory> localThrottleCache_;
  const std::unique_ptr<ThrottleSignalFactory> globalThrottleCache_;

  mutable std::mutex mu_;

  std::mt19937 rng_;
  Stats stats_;
};

std::ostream& operator<<(std::ostream& os, Throttler::SignalType type);
} // namespace facebook::velox::dwio::common
