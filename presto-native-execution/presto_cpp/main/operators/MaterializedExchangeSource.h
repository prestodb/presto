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

#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

/// Exchange source for materialized shuffle data using promise-based
/// concurrency control to prevent multiple concurrent requests.
/// Follows the same pattern as PrestoExchangeSource.
class MaterializedExchangeSource : public velox::exec::ExchangeSource {
 public:
  MaterializedExchangeSource(
      const std::string& taskId,
      int partitionId,
      const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
      const std::shared_ptr<ShuffleReader>& shuffle,
      velox::memory::MemoryPool* FOLLY_NONNULL pool)
      : ExchangeSource(taskId, partitionId, queue, pool), shuffle_(shuffle) {}

  /// Implements gating mechanism to prevent multiple concurrent requests.
  /// Checks stream end status and pending requests before allowing new
  ///  requests.
  bool shouldRequestLocked() override;

  /// Initiates async request for shuffle data and returns
  ///  future which will befulfilled when data is available.
  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait) override;

  /// Returns estimated remaining bytes in shuffle stream without fetching data.
  /// Uses default size estimate as shuffle interface doesn't provide sizes.
  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override;

  /// Closes the exchange source and fulfills any pending promises to prevent
  /// hangs.
  void close() override;

  /// Returns shuffle operation statistics from the underlying reader.
  folly::F14FastMap<std::string, int64_t> stats() const override;

  /// Factory method to create exchange source from URL.
  /// URL format: batch://<taskid>?shuffleInfo=<serialized-shuffle-info>
  static std::shared_ptr<velox::exec::ExchangeSource> createExchangeSource(
      const std::string& url,
      int32_t partitionId,
      const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
      velox::memory::MemoryPool* FOLLY_NONNULL pool);

 private:
  /// Underlying shuffle reader providing access to materialized data.
  const std::shared_ptr<ShuffleReader> shuffle_;

  // The number of batches read from 'shuffle_'..
  uint64_t numBatches_{0};
};
} // namespace facebook::presto::operators
