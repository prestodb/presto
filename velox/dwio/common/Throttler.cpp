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

#include "velox/dwio/common/Throttler.h"

#include <boost/random/uniform_int_distribution.hpp>

#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/base/SuccinctPrinter.h"

namespace facebook::velox::dwio::common {
namespace {
// Builds key in local throttled cache to make it unique across storage
// clusters.
std::string localThrottleCacheKey(
    const std::string& cluster,
    const std::string& directory) {
  return fmt::format("{}:{}", cluster, directory);
}
} // namespace

Throttler::Config::Config(
    bool _throttleEnabled,
    uint64_t _minThrottleBackoffMs,
    uint64_t _maxThrottleBackoffMs,
    double _backoffScaleFactor,
    uint32_t _minLocalThrottledSignals,
    uint32_t _minGlobalThrottledSignals,
    uint32_t _minNetworkThrottledSignals,
    uint32_t _maxCacheEntries,
    uint32_t _cacheTTLMs)
    : throttleEnabled(_throttleEnabled),
      minThrottleBackoffMs(_minThrottleBackoffMs),
      maxThrottleBackoffMs(_maxThrottleBackoffMs),
      backoffScaleFactor(_backoffScaleFactor),
      minLocalThrottledSignals(_minLocalThrottledSignals),
      minGlobalThrottledSignals(_minGlobalThrottledSignals),
      minNetworkThrottledSignals(_minNetworkThrottledSignals),
      maxCacheEntries(_maxCacheEntries),
      cacheTTLMs(_cacheTTLMs) {}

std::string Throttler::Config::toString() const {
  return fmt::format(
      "throttleEnabled:{} minThrottleBackoffMs:{} maxThrottleBackoffMs:{} backoffScaleFactor:{} minLocalThrottledSignals:{} minGlobalThrottledSignals:{} minNetworkThrottledSignals:{} maxCacheEntries:{} cacheTTLMs:{}",
      throttleEnabled,
      succinctMillis(minThrottleBackoffMs),
      succinctMillis(maxThrottleBackoffMs),
      backoffScaleFactor,
      minLocalThrottledSignals,
      minGlobalThrottledSignals,
      minNetworkThrottledSignals,
      maxCacheEntries,
      succinctMillis(cacheTTLMs));
};

std::string Throttler::signalTypeName(SignalType type) {
  switch (type) {
    case SignalType::kNone:
      return "None";
    case SignalType::kLocal:
      return "Local";
    case SignalType::kGlobal:
      return "Global";
    case SignalType::kNetwork:
      return "Network";
    default:
      return fmt::format("Unknown Signal Type: {}", static_cast<int>(type));
  }
}

std::ostream& operator<<(std::ostream& os, Throttler::SignalType type) {
  os << Throttler::signalTypeName(type);
  return os;
}

void Throttler::init(const Config& config) {
  std::unique_lock guard{instanceLock()};
  auto& instance = instanceRef();
  VELOX_CHECK_NULL(instance, "Throttler has already been set");
  instance = std::unique_ptr<Throttler>(new Throttler(config));
}

Throttler* Throttler::instance() {
  std::shared_lock guard{instanceLock()};
  auto& instance = instanceRef();
  if (instance == nullptr) {
    return nullptr;
  }
  return instance.get();
}

Throttler::Throttler(const Config& config)
    : throttleEnabled_(config.throttleEnabled),
      minThrottleBackoffDurationMs_(config.minThrottleBackoffMs),
      maxThrottleBackoffDurationMs_(config.maxThrottleBackoffMs),
      backoffScaleFactor_(config.backoffScaleFactor),
      localThrottleCache_(maybeMakeThrottleSignalCache(
          config.throttleEnabled,
          config.minLocalThrottledSignals,
          config.maxCacheEntries,
          config.cacheTTLMs)),
      globalThrottleCache_(maybeMakeThrottleSignalCache(
          config.throttleEnabled,
          config.minGlobalThrottledSignals,
          config.maxCacheEntries,
          config.cacheTTLMs)),
      networkThrottleCache_(maybeMakeThrottleSignalCache(
          config.throttleEnabled,
          config.minNetworkThrottledSignals,
          config.maxCacheEntries,
          config.cacheTTLMs)) {
  LOG(INFO) << "IO throttler config: " << config.toString();
}

uint64_t Throttler::throttleBackoff(
    SignalType type,
    const std::string& cluster,
    const std::string& directory) {
  if (!throttleEnabled() || type == SignalType::kNone) {
    return kNoBackOffMs_;
  }

  const uint64_t backOffDurationMs =
      calculateBackoffDurationAndUpdateThrottleCache(type, cluster, directory);
  if (backOffDurationMs == kNoBackOffMs_) {
    return kNoBackOffMs_;
  }

  updateThrottleStats(type, backOffDurationMs);

  std::this_thread::sleep_for(
      std::chrono::milliseconds(backOffDurationMs)); // NOLINT
  return backOffDurationMs;
}

void Throttler::updateThrottleStats(SignalType type, uint64_t backoffDelayMs) {
  stats_.backOffDelay.increment(backoffDelayMs);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricStorageThrottledDurationMs, backoffDelayMs);

  switch (type) {
    case SignalType::kLocal:
      ++stats_.localThrottled;
      RECORD_METRIC_VALUE(kMetricStorageLocalThrottled);
      break;
    case SignalType::kGlobal:
      ++stats_.globalThrottled;
      RECORD_METRIC_VALUE(kMetricStorageGlobalThrottled);
      break;
    case SignalType::kNetwork:
      ++stats_.networkThrottled;
      RECORD_METRIC_VALUE(kMetricStorageNetworkThrottled);
      break;
    default:
      break;
  }
}

void Throttler::updateThrottleCacheLocked(
    SignalType type,
    const std::string& cluster,
    const std::string& directory,
    CachedThrottleSignalPtr& localSignal,
    CachedThrottleSignalPtr& globalSignal,
    CachedThrottleSignalPtr& networkSignal) {
  VELOX_CHECK(throttleEnabled());
  switch (type) {
    case SignalType::kLocal:
      if (localSignal.get() == nullptr) {
        localThrottleCache_.throttleCache->generate(
            localThrottleCacheKey(cluster, directory));
      } else {
        ++localSignal->count;
      }
      return;
    case SignalType::kGlobal:
      if (globalSignal.get() == nullptr) {
        globalThrottleCache_.throttleCache->generate(cluster);
      } else {
        ++globalSignal->count;
      }
      return;
    case SignalType::kNetwork:
      if (networkSignal.get() == nullptr) {
        networkThrottleCache_.throttleCache->generate(cluster);
      } else {
        ++networkSignal->count;
      }
      return;
    default:
      VELOX_UNREACHABLE("Invalid type provided: {}", signalTypeName(type));
  };
}

uint64_t Throttler::calculateBackoffDurationAndUpdateThrottleCache(
    SignalType type,
    const std::string& cluster,
    const std::string& directory) {
  std::lock_guard<std::mutex> l(mu_);
  // Gets maximum count of local, global, and network throttle signals in Cache.
  auto localThrottleCachePtr = localThrottleCache_.throttleCache->get(
      localThrottleCacheKey(cluster, directory));
  const int64_t localThrottleCount =
      (localThrottleCachePtr.get() != nullptr ? localThrottleCachePtr->count
                                              : 0) +
      (type == SignalType::kLocal ? 1 : 0) -
      localThrottleCache_.minThrottledSignalsToBackOff;

  auto globalThrottleCachePtr =
      globalThrottleCache_.throttleCache->get(cluster);
  const int64_t globalThrottleCount =
      (globalThrottleCachePtr.get() != nullptr ? globalThrottleCachePtr->count
                                               : 0) +
      (type == SignalType::kGlobal ? 1 : 0) -
      globalThrottleCache_.minThrottledSignalsToBackOff;

  auto networkThrottleCachePtr =
      networkThrottleCache_.throttleCache->get(cluster);
  const int64_t networkThrottleCount =
      (networkThrottleCachePtr.get() != nullptr ? networkThrottleCachePtr->count
                                                : 0) +
      (type == SignalType::kNetwork ? 1 : 0) -
      networkThrottleCache_.minThrottledSignalsToBackOff;

  // Update throttling signal cache.
  updateThrottleCacheLocked(
      type,
      cluster,
      directory,
      localThrottleCachePtr,
      globalThrottleCachePtr,
      networkThrottleCachePtr);

  const int64_t throttleAttempts = std::max(
      networkThrottleCount, std::max(localThrottleCount, globalThrottleCount));

  // Calculates the delay with exponential backoff
  if (throttleAttempts <= 0) {
    return kNoBackOffMs_;
  }

  const uint64_t backoffDelayMs = std::round(
      minThrottleBackoffDurationMs_ *
      pow(backoffScaleFactor_, throttleAttempts - 1));

  // Adds some casualness so requests can be waken up at different timestamp
  return std::min(
      backoffDelayMs +
          boost::random::uniform_int_distribution<uint64_t>(
              1,
              std::max<uint64_t>(
                  1, static_cast<uint64_t>(backoffDelayMs * 0.1)))(rng_),
      maxThrottleBackoffDurationMs_);
}

std::unique_ptr<Throttler::ThrottleSignal>
Throttler::ThrottleSignalGenerator::operator()(
    const std::string& /*unused*/,
    const void* /*unused*/,
    void* /*unused*/) {
  return std::unique_ptr<ThrottleSignal>(new ThrottleSignal{1});
}

/* static */
Throttler::ThrottleSignalCache Throttler::maybeMakeThrottleSignalCache(
    bool enabled,
    uint32_t minThrottledSignals,
    uint32_t maxCacheEntries,
    uint32_t cacheTTLMs) {
  return {
      .throttleCache = !enabled
          ? nullptr
          : std::make_unique<ThrottleSignalFactory>(
                std::make_unique<SimpleLRUCache<std::string, ThrottleSignal>>(
                    maxCacheEntries, cacheTTLMs),
                std::make_unique<ThrottleSignalGenerator>()),
      .minThrottledSignalsToBackOff = minThrottledSignals,
  };
}
} // namespace facebook::velox::dwio::common
