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
#include <folly/experimental/FunctionScheduler.h>
#include <cstdint>
#include <queue>
#include <string>

namespace facebook::presto {
/// Utility class that spawns a thread which periodically checks the memory
/// usage and perform the following actions:
class PeriodicMemoryChecker {
 public:
  struct Config {
    /// The frequency 'PeriodicMemoryChecker' runs at.
    uint64_t MemoryCheckerIntervalSec{1};

    /// If true, starts memory limit checker to trigger memory pushback when
    /// server is under low memory pressure.
    bool systemMemPushbackEnabled{false};

    /// Specifies the system memory limit that triggers the memory pushback if
    /// the server memory usage is beyond this limit. This only applies if
    /// 'systemMemPushbackEnabled_' is true.
    uint64_t systemMemLimitBytes{0};

    /// Specifies the amount of memory to shrink when the memory pushback is
    /// triggered. This only applies if 'systemMemPushbackEnabled_' is true.
    uint64_t systemMemShrinkBytes{0};

    /// If true, starts memory limit checker that dumps the heap profile if
    /// memory allocated via malloc exceeds 'mallocBytesUsageDumpThreshold_'.
    /// See 'mallocCheckingCb()' for more details.
    bool mallocMemHeapDumpEnabled{false};

    /// Minimum Interval in seconds between two consecutive heap dumps.
    size_t minHeapDumpIntervalSec{10};

    /// The directory where heap profiles are dumped.
    std::string heapDumpLogDir;

    /// The prefix of heapdump file name.
    std::string heapDumpFilePrefix;

    /// Maximum number of heap dump files to maintain in rotation.
    int maxHeapDumpFiles{10};

    /// Memory (in bytes) allocated via malloc() that triggers the heap dump.
    /// Default is 20GB.
    size_t mallocBytesUsageDumpThreshold{20UL * 1024 * 1024 * 1024};
  };

  explicit PeriodicMemoryChecker(Config config);

  void start();

  void stop();

 protected:
  /// Returns current system memory usage. The returned value is used to compare
  /// with 'Config::systemMemLimitBytes'.
  virtual int64_t systemUsedMemoryBytes() const = 0;

  /// Returns current bytes allocated by malloc. The returned value is used to
  /// compare with 'Config::mallocBytesUsageDumpThreshold'
  virtual int64_t mallocBytes() const = 0;

  /// Callback function that is invoked by 'PeriodicMemoryChecker' at all time.
  /// Light operations such as stats reporting can be done in this call back.
  virtual void periodicCb() const = 0;

  /// Callback function that performs a heap dump. Returns true if dump is
  /// successful.
  virtual bool heapDumpCb(const std::string& filePath) const = 0;

  const Config config_;

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

  // Invoked by the periodic checker when 'Config::systemMemPushbackEnabled'
  // is true and system memory usage is above 'Config::systemMemLimitBytes'.
  void pushbackMemory() const;

  // Invoked by the periodic checker when 'Config::mallocMemHeapDumpEnabled' is
  // true.
  void maybeDumpHeap();

  std::string createHeapDumpFilePath() const;

  std::shared_ptr<folly::FunctionScheduler> scheduler_;
  size_t lastHeapDumpAttemptTimestamp_{0};
  std::priority_queue<
      DumpFileInfo,
      std::vector<DumpFileInfo>,
      std::greater<DumpFileInfo>>
      dumpFilesByHeapMemUsageMinPq_;
};
}
