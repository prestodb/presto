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

#include "presto_cpp/main/operators/BroadcastFactory.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

/// Used for files based broadcast.
/// Reads split location uri in format :
/// batch://<taskid>?broadcastInfo={fileInfos:[<fileInfo>]}.
class BroadcastExchangeSource : public velox::exec::ExchangeSource {
 public:
  BroadcastExchangeSource(
      const std::string& taskId,
      int destination,
      const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
      const std::shared_ptr<BroadcastFileReader>& reader,
      velox::memory::MemoryPool* pool)
      : ExchangeSource(taskId, destination, queue, pool), reader_(reader) {}

  bool shouldRequestLocked() override {
    return !atEnd_;
  }

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      uint32_t maxWaitSeconds) override;

  void close() override {}

  folly::F14FastMap<std::string, int64_t> stats() const override;

  /// Url format for this exchange source:
  /// batch://<taskid>?broadcastInfo={fileInfos:[<fileInfo>]}.
  static std::shared_ptr<ExchangeSource> createExchangeSource(
      const std::string& url,
      int destination,
      const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
      velox::memory::MemoryPool* pool);

 private:
  const std::shared_ptr<BroadcastFileReader> reader_;
};
} // namespace facebook::presto::operators