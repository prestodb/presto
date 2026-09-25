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
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include <folly/executors/FunctionScheduler.h>

namespace facebook::presto {
/// Periodically samples system memory usage and reclaims memory under
/// pressure. Sampling and reclamation run on separate scheduler threads so a
/// blocked reclaim cannot stall usage updates: the sampling thread is the
/// sole writer of the cached usage snapshot, every other thread only reads
/// it.
class PeriodicMemoryChecker {
 public:
  struct Config {
    /// The frequency 'PeriodicMemoryChecker' runs at.
    uint64_t memoryCheckerIntervalMs{1'000};

    /// If true, starts memory limit checker to trigger memory pushback when
    /// server is under low memory pressure.
    bool systemMemPushbackEnabled{false};

    /// Specifies the system memory limit that triggers the memory pushback if
    /// the server memory usage is beyond this limit. This only applies if
    /// 'systemMemPushbackEnabled_' is true.
    uint64_t systemMemLimitBytes{0};

    /// Specifies the amount of memory to shrink when the memory pushback is
    /// triggered. This only applies if 'systemMemPushbackEnabled' is true.
    uint64_t systemMemShrinkBytes{0};

    /// Starts memory limit checker that dumps the heap profile if memory
    /// allocated via malloc exceeds 'mallocBytesUsageDumpThreshold'. See
    /// mallocCheckingCb() for more details.
    bool mallocMemHeapDumpEnabled{false};

    /// Only applies if 'mallocMemHeapDumpEnabled' is true. Minimum Interval in
    /// seconds between two consecutive heap dumps.
    size_t minHeapDumpIntervalSec{10};

    /// Only applies if 'mallocMemHeapDumpEnabled' is true. The directory where
    /// heap profiles are dumped.
    std::string heapDumpLogDir;

    /// Only applies if 'mallocMemHeapDumpEnabled' is true. The prefix of
    /// heapdump file name.
    std::string heapDumpFilePrefix;

    /// Only applies if 'mallocMemHeapDumpEnabled' is true. Maximum number of
    /// heap dump files to maintain in rotation.
    uint32_t maxHeapDumpFiles{10};

    /// Only applies if 'mallocMemHeapDumpEnabled' is true. Memory (in bytes)
    /// allocated via malloc() that triggers the heap dump. Default is 20GB.
    size_t mallocBytesUsageDumpThreshold{20UL * 1024 * 1024 * 1024};
  };

  explicit PeriodicMemoryChecker(const Config& config);

  virtual ~PeriodicMemoryChecker() = default;

  /// Starts the background schedulers. This should only be called once.
  virtual void start();

  /// Stops the background schedulers.
  virtual void stop();

  /// Last sampled system memory usage in bytes. Written only by the sampling
  /// thread, read from any thread.
  int64_t systemUsedMemoryBytes() const;

 protected:
  /// Samples system memory usage and publishes it to the cache. Runs only
  /// on the sampling thread.
  virtual void loadSystemMemoryUsage() = 0;

  /// Returns current bytes allocated by malloc. The returned value is used to
  /// compare with 'Config::mallocBytesUsageDumpThreshold'
  virtual int64_t mallocBytes() const = 0;

  /// Callback function that is invoked by 'PeriodicMemoryChecker' periodically.
  /// Light operations such as stats reporting can be done in this call back.
  virtual void periodicCb() = 0;

  /// Callback function that performs a heap dump. Returns true if dump is
  /// successful.
  virtual bool heapDumpCb(const std::string& filePath) const = 0;

  /// Callback function that removes a dumped file with given 'filePath'.
  /// Returns true if dump is successful.
  virtual void removeDumpFile(const std::string& filePath) const = 0;

  /// Reclaims memory when usage is above 'Config::systemMemLimitBytes'.
  /// Runs on the pushback thread, concurrently with 'periodicCb()'.
  virtual void pushbackMemory();

  const Config config_;
  std::atomic<int64_t> cachedSystemUsedMemoryBytes_{0};

 private:
  // Struct that stores the file names of the heap profiles dumped and the
  // memory allocated by jemalloc when they were dumped. Used to create a min
  // priority queue ordered by the memory allocated by jemalloc when the heap
  // dump was generated. Used to determine which heap profile to delete when the
  // number of files generated exceeds 'maxHeapDumpFiles_'.
  struct DumpFileInfo {
    // The memory allocated via jemalloc when the heap dump was generated.
    int64_t mallocUsedBytes;
    // Path to the heap dump file.
    std::string filePath;

    bool operator>(const DumpFileInfo& other) const {
      return mallocUsedBytes > other.mallocUsedBytes;
    }
    bool operator<(const DumpFileInfo& other) const {
      return mallocUsedBytes < other.mallocUsedBytes;
    }
  };

  // Invoked by the periodic checker when 'Config::mallocMemHeapDumpEnabled' is
  // true.
  void maybeDumpHeap();

  std::string createHeapDumpFilePath() const;

  size_t lastHeapDumpAttemptTimestamp_{0};
  std::priority_queue<
      DumpFileInfo,
      std::vector<DumpFileInfo>,
      std::greater<DumpFileInfo>>
      dumpFilesByHeapMemUsageMinPq_;
  // Samples usage on the sampling thread without running reclamation.
  std::unique_ptr<folly::FunctionScheduler> memoryCheckScheduler_;
  // Runs reclamation on its own thread so a blocked reclaim cannot stall
  // sampling.
  std::unique_ptr<folly::FunctionScheduler> memoryPushbackScheduler_;
};

std::unique_ptr<PeriodicMemoryChecker> createMemoryChecker();
} // namespace facebook::presto
