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

#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

class ShuffleRowBatch : public velox::exec::SerializedPage {
 public:
  explicit ShuffleRowBatch(
      std::unique_ptr<ReadBatch> rowBatch)
      : velox::exec::
            SerializedPage{folly::IOBuf::wrapBuffer(
                  rowBatch->data->as<char>(), rowBatch->data->size()), nullptr, rowBatch->rows.size()},
        rowBatch_{std::move(rowBatch)} {}

  ~ShuffleRowBatch() override {}

  const std::vector<std::string_view>& rows() const {
    return rowBatch_->rows;
  }

 private:
  const std::unique_ptr<ReadBatch> rowBatch_;
};

class ShuffleExchangeSource : public velox::exec::ExchangeSource {
 public:
  ShuffleExchangeSource(
      const std::string& taskId,
      int destination,
      const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
      const std::shared_ptr<ShuffleReader>& shuffleReader,
      velox::memory::MemoryPool* pool)
      : ExchangeSource(taskId, destination, queue, pool),
        shuffleReader_(shuffleReader) {}

  bool shouldRequestLocked() override {
    return !atEnd_;
  }

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait) override;

  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override;

  void close() override {
    shuffleReader_->noMoreData(true);
  }

  folly::F14FastMap<std::string, int64_t> stats() const override;

  /// url needs to follow below format:
  /// batch://<taskid>?shuffleInfo=<serialized-shuffle-info>
  static std::shared_ptr<velox::exec::ExchangeSource> createExchangeSource(
      const std::string& url,
      int32_t destination,
      const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
      velox::memory::MemoryPool* pool);

 private:
  const std::shared_ptr<ShuffleReader> shuffleReader_;

  // The number of batches read from 'shuffleReader_'.
  uint64_t numBatches_{0};
};
} // namespace facebook::presto::operators
