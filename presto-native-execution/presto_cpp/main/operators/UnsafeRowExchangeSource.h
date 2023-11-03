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

#include <folly/executors/IOThreadPoolExecutor.h>
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Operator.h"

namespace folly {
class IOThreadPoolExecutor;
} // namespace folly

namespace facebook::presto::operators {

class UnsafeRowExchangeSource : public velox::exec::ExchangeSource {
 public:
  UnsafeRowExchangeSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<velox::exec::ExchangeQueue> queue,
      const std::shared_ptr<ShuffleReader>& shuffle,
      velox::memory::MemoryPool* FOLLY_NONNULL pool,
      folly::IOThreadPoolExecutor* ioThreadPoolExecutor)
      : ExchangeSource(taskId, destination, queue, pool), shuffle_(shuffle),
        ioThreadPoolExecutor_(ioThreadPoolExecutor) {}

  bool shouldRequestLocked() override {
    return !atEnd_ && !isRequestPendingLocked();
  }

  bool supportsFlowControlV2() const override {
    return true;
  }

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      uint32_t maxWaitSeconds) override;

  void close() override {}

  folly::F14FastMap<std::string, int64_t> stats() const override;

  /// url needs to follow below format:
  /// batch://<taskid>?shuffleInfo=<serialized-shuffle-info>
  static std::unique_ptr<velox::exec::ExchangeSource> createExchangeSource(
      const std::string& url,
      int32_t destination,
      std::shared_ptr<velox::exec::ExchangeQueue> queue,
      velox::memory::MemoryPool* FOLLY_NONNULL pool,
      folly::IOThreadPoolExecutor* ioThreadPoolExecutor);

 private:
  void fetchDataAndUpdateQueueAsync();
  const std::shared_ptr<ShuffleReader> shuffle_;

  // The number of batches read from 'shuffle_'.
  uint64_t numBatches_{0};
  velox::VeloxPromise<Response> promise_{
      velox::VeloxPromise<Response>::makeEmpty()};
  folly::IOThreadPoolExecutor* ioThreadPoolExecutor_;
};
} // namespace facebook::presto::operators
