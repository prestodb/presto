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

#include "velox/common/base/RuntimeMetrics.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/exec/ExchangeSource.h"

namespace facebook::velox::exec {

// Handle for a set of producers. This may be shared by multiple Exchanges, one
// per consumer thread.
class ExchangeClient {
 public:
  static constexpr int32_t kDefaultMinSize = 32 << 20; // 32 MB.

  ExchangeClient(
      int destination,
      memory::MemoryPool* pool,
      int64_t minSize = kDefaultMinSize)
      : destination_(destination),
        pool_(pool),
        queue_(std::make_shared<ExchangeQueue>(minSize)) {
    VELOX_CHECK_NOT_NULL(pool_);
    VELOX_CHECK(
        destination >= 0,
        "Exchange client destination must be greater than zero, got {}",
        destination);
  }

  ~ExchangeClient();

  memory::MemoryPool* pool() const {
    return pool_;
  }

  // Creates an exchange source and starts fetching data from the specified
  // upstream task. If 'close' has been called already, creates an exchange
  // source and immediately closes it to notify the upstream task that data is
  // no longer needed. Repeated calls with the same 'taskId' are ignored.
  void addRemoteTaskId(const std::string& taskId);

  void noMoreRemoteTasks();

  // Closes exchange sources.
  void close();

  // Returns runtime statistics aggregated across all of the exchange sources.
  folly::F14FastMap<std::string, RuntimeMetric> stats() const;

  std::shared_ptr<ExchangeQueue> queue() const {
    return queue_;
  }

  std::unique_ptr<SerializedPage> next(bool* atEnd, ContinueFuture* future);

  std::string toString() const;

  std::string toJsonString() const;

 private:
  const int destination_;
  memory::MemoryPool* const pool_;
  std::shared_ptr<ExchangeQueue> queue_;
  std::unordered_set<std::string> taskIds_;
  std::vector<std::shared_ptr<ExchangeSource>> sources_;
  bool closed_{false};
};

} // namespace facebook::velox::exec
