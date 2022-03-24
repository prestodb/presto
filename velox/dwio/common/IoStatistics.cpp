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

#include "velox/dwio/common/IoStatistics.h"
#include <glog/logging.h>
#include <atomic>
#include <utility>

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

uint64_t IoStatistics::rawBytesRead() const {
  return rawBytesRead_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::rawOverreadBytes() const {
  return rawOverreadBytes_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::rawBytesWritten() const {
  return rawBytesWritten_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::inputBatchSize() const {
  return inputBatchSize_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::outputBatchSize() const {
  return outputBatchSize_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::incRawBytesRead(int64_t v) {
  return rawBytesRead_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incRawBytesWritten(int64_t v) {
  return rawBytesWritten_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incInputBatchSize(int64_t v) {
  return inputBatchSize_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incOutputBatchSize(int64_t v) {
  return outputBatchSize_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incRawOverreadBytes(int64_t v) {
  return rawOverreadBytes_.fetch_add(v, std::memory_order_relaxed);
}

void IoStatistics::incOperationCounters(
    const std::string& operation,
    const uint64_t resourceThrottleCount,
    const uint64_t localThrottleCount,
    const uint64_t globalThrottleCount,
    const uint64_t retryCount,
    const uint64_t latencyInMs,
    const uint64_t delayInjectedInSecs) {
  std::lock_guard<std::mutex> lock{operationStatsMutex_};
  operationStats_[operation].localThrottleCount += localThrottleCount;
  operationStats_[operation].resourceThrottleCount += resourceThrottleCount;
  operationStats_[operation].globalThrottleCount += globalThrottleCount;
  operationStats_[operation].retryCount += retryCount;
  operationStats_[operation].latencyInMs += latencyInMs;
  operationStats_[operation].requestCount++;
  operationStats_[operation].delayInjectedInSecs += delayInjectedInSecs;
}

std::unordered_map<std::string, OperationCounters>
IoStatistics::operationStats() const {
  std::lock_guard<std::mutex> lock{operationStatsMutex_};
  return operationStats_;
}

folly::dynamic serialize(const OperationCounters& counters) {
  folly::dynamic json = folly::dynamic::object;
  json["latencyInMs"] = counters.latencyInMs;
  json["localThrottleCount"] = counters.localThrottleCount;
  json["resourceThrottleCount"] = counters.resourceThrottleCount;
  json["globalThrottleCount"] = counters.globalThrottleCount;
  json["retryCount"] = counters.retryCount;
  json["requestCount"] = counters.requestCount;
  json["delayInjectedInSecs"] = counters.delayInjectedInSecs;
  return json;
}

folly::dynamic IoStatistics::getOperationStatsSnapshot() const {
  auto snapshot = operationStats();
  folly::dynamic json = folly::dynamic::object;
  for (auto stat : snapshot) {
    json[stat.first] = serialize(stat.second);
  }
  return json;
}

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
