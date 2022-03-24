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

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <folly/dynamic.h>

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

struct OperationCounters {
  uint64_t resourceThrottleCount{0};
  uint64_t localThrottleCount{0};
  uint64_t globalThrottleCount{0};
  uint64_t retryCount{0};
  uint64_t latencyInMs{0};
  uint64_t requestCount{0};
  uint64_t delayInjectedInSecs{0};
};

class IoCounter {
 public:
  uint64_t count() const {
    return count_;
  }

  uint64_t bytes() const {
    return bytes_;
  }

  void increment(uint64_t bytes) {
    ++count_;
    bytes_ += bytes;
  }

 private:
  std::atomic<uint64_t> count_{0};
  std::atomic<uint64_t> bytes_{0};
};

class IoStatistics {
 public:
  uint64_t rawBytesRead() const;
  uint64_t rawOverreadBytes() const;
  uint64_t rawBytesWritten() const;
  uint64_t inputBatchSize() const;
  uint64_t outputBatchSize() const;

  uint64_t incRawBytesRead(int64_t);
  uint64_t incRawOverreadBytes(int64_t);
  uint64_t incRawBytesWritten(int64_t);
  uint64_t incInputBatchSize(int64_t);
  uint64_t incOutputBatchSize(int64_t);

  IoCounter& prefetch() {
    return prefetch_;
  }

  IoCounter& read() {
    return read_;
  }

  IoCounter& ssdRead() {
    return ssdRead_;
  }

  IoCounter& ramHit() {
    return ramHit_;
  }

  IoCounter& queryThreadIoLatency() {
    return queryThreadIoLatency_;
  }

  void incOperationCounters(
      const std::string& operation,
      const uint64_t resourceThrottleCount,
      const uint64_t localThrottleCount,
      const uint64_t globalThrottleCount,
      const uint64_t retryCount,
      const uint64_t latencyInMs,
      const uint64_t delayInjectedInSecs);

  std::unordered_map<std::string, OperationCounters> operationStats() const;

  folly::dynamic getOperationStatsSnapshot() const;

 private:
  std::atomic<uint64_t> rawBytesRead_{0};
  std::atomic<uint64_t> rawBytesWritten_{0};
  std::atomic<uint64_t> inputBatchSize_{0};
  std::atomic<uint64_t> outputBatchSize_{0};
  std::atomic<uint64_t> rawOverreadBytes_{0};

  // Planned read from storage or SSD.
  IoCounter prefetch_;

  // Read from storage, for sparsely accessed columns.
  IoCounter read_;

  // Hits from RAM cache. Does not include first use of prefetched data.
  IoCounter ramHit_;

  // Read from SSD cache instead of storage. Includes both random and planned
  // reads.
  IoCounter ssdRead_;

  // Time spent by a query processing thread waiting for synchronously
  // issued IO or for an in-progress read-ahead to finish.
  IoCounter queryThreadIoLatency_;

  std::unordered_map<std::string, OperationCounters> operationStats_;
  mutable std::mutex operationStatsMutex_;
};

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
