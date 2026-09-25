/*
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

#include "presto_cpp/main/PeriodicMemoryChecker.h"

#include <cstdint>
#include <utility>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/common/Utils.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/time/Timer.h"

namespace facebook::presto {
PeriodicMemoryChecker::PeriodicMemoryChecker(const Config& config)
    : config_(config) {
  if (config_.systemMemPushbackEnabled) {
    VELOX_CHECK_GT(config_.systemMemLimitBytes, 0);
    VELOX_CHECK_GT(
        config_.systemMemLimitBytes,
        config_.systemMemShrinkBytes,
        "systemMemShrinkBytes must be less than systemMemLimitBytes");
  }
  if (config_.mallocMemHeapDumpEnabled) {
    VELOX_CHECK(
        !config_.heapDumpLogDir.empty(),
        "heapDumpLogDir cannot be empty when heap dump is enabled.");
    VELOX_CHECK(
        !config_.heapDumpFilePrefix.empty(),
        "heapDumpFilePrefix cannot be empty when heap dump is enabled.");
  }
}

void PeriodicMemoryChecker::start() {
  if (!config_.systemMemPushbackEnabled) {
    PRESTO_STARTUP_LOG(INFO) << "Server memory pushback is not enabled";
  } else {
    VELOX_CHECK_GT(
        config_.systemMemLimitBytes, 0, "Invalid system mem limit provided");
    VELOX_CHECK_GT(
        config_.systemMemShrinkBytes, 0, "Invalid system mem shrink provided");
    PRESTO_STARTUP_LOG(INFO)
        << "Creating server memory pushback checker, memory check interval "
        << config_.memoryCheckerIntervalMs << "ms, system memory limit: "
        << velox::succinctBytes(config_.systemMemLimitBytes)
        << ", memory shrink size: "
        << velox::succinctBytes(config_.systemMemShrinkBytes);
  }

  if (!config_.mallocMemHeapDumpEnabled) {
    PRESTO_STARTUP_LOG(INFO) << "Malloc memory heap dumper is not enabled";
  } else {
    PRESTO_STARTUP_LOG(INFO)
        << "Enabling Malloc memory heap dumper"
        << ", malloc'd memory dump threshold: "
        << velox::succinctBytes(config_.mallocBytesUsageDumpThreshold)
        << ", max dump files: " << config_.maxHeapDumpFiles
        << ", heap dump folder: " << config_.heapDumpLogDir
        << ", heap dump interval: " << config_.minHeapDumpIntervalSec
        << " seconds";
  }

  VELOX_CHECK_NULL(memoryCheckScheduler_, "start() called more than once");
  memoryCheckScheduler_ = std::make_unique<folly::FunctionScheduler>();
  memoryCheckScheduler_->setThreadName("MemoryCheckerThread");
  memoryCheckScheduler_->addFunction(
      [self = this]() {
        // Exceptions are caught and logged by FunctionScheduler itself; a
        // throwing tick must not take down the sampling thread.
        self->loadSystemMemoryUsage();
        self->periodicCb();
        if (self->config_.mallocMemHeapDumpEnabled) {
          self->maybeDumpHeap();
        }
      },
      std::chrono::milliseconds(config_.memoryCheckerIntervalMs),
      "periodic-sys-mem-check",
      std::chrono::seconds(0));

  if (config_.systemMemPushbackEnabled) {
    memoryPushbackScheduler_ = std::make_unique<folly::FunctionScheduler>();
    memoryPushbackScheduler_->setThreadName("MemoryPushback");
    memoryPushbackScheduler_->addFunction(
        [self = this]() {
          // Exceptions are caught and logged by FunctionScheduler itself; a
          // throwing tick must not take down the pushback thread.
          if (std::cmp_greater(
                  self->systemUsedMemoryBytes(),
                  self->config_.systemMemLimitBytes)) {
            self->pushbackMemory();
          }
        },
        std::chrono::milliseconds(config_.memoryCheckerIntervalMs),
        "periodic-memory-pushback",
        std::chrono::seconds(0));
  }

  memoryCheckScheduler_->start();
  if (memoryPushbackScheduler_ != nullptr) {
    memoryPushbackScheduler_->start();
  }
}

void PeriodicMemoryChecker::stop() {
  VELOX_CHECK_NOT_NULL(memoryCheckScheduler_);
  if (memoryPushbackScheduler_ != nullptr) {
    memoryPushbackScheduler_->shutdown();
    memoryPushbackScheduler_.reset();
  }
  memoryCheckScheduler_->shutdown();
  memoryCheckScheduler_.reset();
}

int64_t PeriodicMemoryChecker::systemUsedMemoryBytes() const {
  return cachedSystemUsedMemoryBytes_.load(std::memory_order_relaxed);
}

std::string PeriodicMemoryChecker::createHeapDumpFilePath() const {
  const size_t now = velox::getCurrentTimeMs() / 1000;
  // Format as follow:
  // <heapDumpFilePrefix>.<pid>.<global_sequence>.i<sequence> =>
  // prefix.1234.235.i565
  return fmt::format(
      "{}/{}.{}.{}.i{}.heap",
      config_.heapDumpLogDir,
      config_.heapDumpFilePrefix,
      getpid(),
      now,
      now);
}

void PeriodicMemoryChecker::maybeDumpHeap() {
  const auto now = velox::getCurrentTimeMs() / 1000;
  const auto allocatedSize = mallocBytes();
  if (allocatedSize >= config_.mallocBytesUsageDumpThreshold &&
      now - lastHeapDumpAttemptTimestamp_ >= config_.minHeapDumpIntervalSec) {
    lastHeapDumpAttemptTimestamp_ = now;
    LOG(INFO) << fmt::format(
        "Memory usage allocated via malloc exceeded threshold of {}, current "
        "allocation: {}",
        velox::succinctBytes(config_.mallocBytesUsageDumpThreshold),
        velox::succinctBytes(allocatedSize));

    const auto minMemUsageDumped = dumpFilesByHeapMemUsageMinPq_.empty()
        ? 0
        : dumpFilesByHeapMemUsageMinPq_.top().mallocUsedBytes;
    if (dumpFilesByHeapMemUsageMinPq_.size() == config_.maxHeapDumpFiles &&
        allocatedSize <= minMemUsageDumped) {
      LOG(INFO) << fmt::format(
          "Heap profile not dumped as current usage {} is below the "
          "minimum usage dumped {} and we already have {} files in rotation",
          velox::succinctBytes(allocatedSize),
          velox::succinctBytes(minMemUsageDumped),
          config_.maxHeapDumpFiles);
      return;
    }

    const auto filePath = createHeapDumpFilePath();
    if (!heapDumpCb(filePath)) {
      LOG(ERROR) << "Error dumping Heap profile";
      return;
    }
    LOG(INFO) << fmt::format(
        "Heap profile with usage {} dumped to {}",
        velox::succinctBytes(allocatedSize),
        filePath);

    dumpFilesByHeapMemUsageMinPq_.push({allocatedSize, filePath});
    if (dumpFilesByHeapMemUsageMinPq_.size() <= config_.maxHeapDumpFiles) {
      return;
    }
    auto& evicted = dumpFilesByHeapMemUsageMinPq_.top();
    LOG(INFO) << fmt::format(
        "Removing Heap profile with lowest usage {} : {}",
        velox::succinctBytes(evicted.mallocUsedBytes),
        evicted.filePath);
    removeDumpFile(evicted.filePath.c_str());
    dumpFilesByHeapMemUsageMinPq_.pop();
  }
}

void PeriodicMemoryChecker::pushbackMemory() {
  VELOX_CHECK(config_.systemMemPushbackEnabled);
  const int64_t currentMemBytes = systemUsedMemoryBytes();
  // The snapshot can go stale between the scheduler guard and here because
  // the sampling thread updates it independently; a stale trigger is a no-op.
  if (!std::cmp_greater(currentMemBytes, config_.systemMemLimitBytes)) {
    return;
  }
  const uint64_t targetMemBytes =
      config_.systemMemLimitBytes - config_.systemMemShrinkBytes;
  const uint64_t bytesToShrink =
      static_cast<uint64_t>(currentMemBytes) - targetMemBytes;
  RECORD_METRIC_VALUE(kCounterMemoryPushbackCount);
  LOG(WARNING) << "System used memory " << velox::succinctBytes(currentMemBytes)
               << " exceeded limit: "
               << velox::succinctBytes(config_.systemMemLimitBytes);

  uint64_t latencyUs{0};
  uint64_t freedBytes{0};
  {
    velox::MicrosecondTimer timer(&latencyUs);
    auto* cache = velox::cache::AsyncDataCache::getInstance();
    auto systemConfig = SystemConfig::instance();
    freedBytes = cache != nullptr ? cache->shrink(bytesToShrink) : 0;
    if (freedBytes < bytesToShrink) {
      try {
        auto* memoryManager = velox::memory::memoryManager();
        freedBytes += velox::memory::AllocationTraits::pageBytes(
            memoryManager->allocator()->unmap(
                velox::memory::AllocationTraits::numPages(
                    bytesToShrink - freedBytes)));
        if (freedBytes < bytesToShrink &&
            systemConfig->systemMemPushBackAbortEnabled()) {
          memoryManager->shrinkPools(
              bytesToShrink - freedBytes,
              /*allowSpill=*/false,
              /*allowAbort=*/true);

          // Try to shrink from cache again as aborted query might hold cache
          // reference.
          if (cache != nullptr) {
            freedBytes += cache->shrink(bytesToShrink - freedBytes);
          }
          if (freedBytes < bytesToShrink) {
            freedBytes += velox::memory::AllocationTraits::pageBytes(
                memoryManager->allocator()->unmap(
                    velox::memory::AllocationTraits::numPages(
                        bytesToShrink - freedBytes)));
          }
        }
      } catch (const velox::VeloxException& ex) {
        LOG(ERROR) << ex.what();
      }
    }
  }
  RECORD_HISTOGRAM_METRIC_VALUE(
      kCounterMemoryPushbackLatencyMs, latencyUs / 1000);
  const auto actualFreedBytes = std::max<int64_t>(
      0, static_cast<int64_t>(currentMemBytes) - systemUsedMemoryBytes());
  RECORD_HISTOGRAM_METRIC_VALUE(
      kCounterMemoryPushbackExpectedReductionBytes, freedBytes);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kCounterMemoryPushbackReductionBytes, actualFreedBytes);
  LOG(INFO) << "Memory pushback shrunk " << velox::succinctBytes(freedBytes)
            << " Effective bytes shrunk: "
            << velox::succinctBytes(actualFreedBytes);
}

#ifndef PRESTO_MEMORY_CHECKER_TYPE
// Initialize singleton for the checker to be nullptr if
// PRESTO_MEMORY_CHECKER_TYPE is not defined.
std::unique_ptr<PeriodicMemoryChecker> createMemoryChecker() {
  return nullptr;
}
#endif
} // namespace facebook::presto
