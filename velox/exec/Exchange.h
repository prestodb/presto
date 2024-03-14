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

#include <algorithm>
#include <random>
#include "velox/exec/ExchangeClient.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/serializers/PrestoSerializer.h"

namespace facebook::velox::exec {

struct RemoteConnectorSplit : public connector::ConnectorSplit {
  const std::string taskId;

  explicit RemoteConnectorSplit(const std::string& _taskId)
      : ConnectorSplit(""), taskId(_taskId) {}

  std::string toString() const override {
    return fmt::format("Remote: {}", taskId);
  }
};

class Exchange : public SourceOperator {
 public:
  Exchange(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::ExchangeNode>& exchangeNode,
      std::shared_ptr<ExchangeClient> exchangeClient,
      const std::string& operatorType = "Exchange")
      : SourceOperator(
            driverCtx,
            exchangeNode->outputType(),
            operatorId,
            exchangeNode->id(),
            operatorType),
        preferredOutputBatchBytes_{
            driverCtx->queryConfig().preferredOutputBatchBytes()},
        processSplits_{operatorCtx_->driverCtx()->driverId == 0},
        exchangeClient_{std::move(exchangeClient)} {
    options_.compressionKind =
        OutputBufferManager::getInstance().lock()->compressionKind();
  }

  ~Exchange() override {
    close();
  }

  RowVectorPtr getOutput() override;

  void close() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

 protected:
  virtual VectorSerde* getSerde();

 private:
  // Invoked to create exchange client for remote tasks.
  // The function shuffles the source task ids first to randomize the source
  // tasks we fetch data from. This helps to avoid different tasks fetching from
  // the same source task in a distributed system.
  void addTaskIds(std::vector<std::string>& taskIds);

  /// Fetches splits from the task until there are no more splits or task
  /// returns a future that will be complete when more splits arrive. Adds
  /// splits to exchangeClient_. Returns true if received a future from the task
  /// and sets the 'future' parameter. Returns false if fetched all splits or if
  /// this operator is not the first operator in the pipeline and therefore is
  /// not responsible for fetching splits and adding them to the
  /// exchangeClient_.
  bool getSplits(ContinueFuture* future);

  /// Fetches runtime stats from ExchangeClient and replaces these in this
  /// operator's stats.
  void recordExchangeClientStats();

  const uint64_t preferredOutputBatchBytes_;

  /// True if this operator is responsible for fetching splits from the Task and
  /// passing these to ExchangeClient.
  const bool processSplits_;
  bool noMoreSplits_ = false;

  /// A future received from Task::getSplitOrFuture(). It will be complete when
  /// there are more splits available or no-more-splits signal has arrived.
  ContinueFuture splitFuture_{ContinueFuture::makeEmpty()};

  // Reusable result vector.
  RowVectorPtr result_;

  std::shared_ptr<ExchangeClient> exchangeClient_;
  std::vector<std::unique_ptr<SerializedPage>> currentPages_;
  bool atEnd_{false};
  std::default_random_engine rng_{std::random_device{}()};
  serializer::presto::PrestoVectorSerde::PrestoOptions options_;
};

} // namespace facebook::velox::exec
