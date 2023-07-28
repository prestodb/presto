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

#include "velox/exec/ExchangeClient.h"
#include "velox/exec/Operator.h"

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
      DriverCtx* ctx,
      const std::shared_ptr<const core::ExchangeNode>& exchangeNode,
      std::shared_ptr<ExchangeClient> exchangeClient,
      const std::string& operatorType = "Exchange")
      : SourceOperator(
            ctx,
            exchangeNode->outputType(),
            operatorId,
            exchangeNode->id(),
            operatorType),
        planNodeId_(exchangeNode->id()),
        exchangeClient_(std::move(exchangeClient)) {}

  ~Exchange() override {
    close();
  }

  RowVectorPtr getOutput() override;

  void close() override {
    SourceOperator::close();
    currentPage_ = nullptr;
    result_ = nullptr;
    if (exchangeClient_) {
      exchangeClient_->close();
    }
    exchangeClient_ = nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

 protected:
  virtual VectorSerde* getSerde();

 private:
  /// Fetches splits from the task until there are no more splits or task
  /// returns a future that will be complete when more splits arrive. Adds
  /// splits to exchangeClient_. Returns true if received a future from the task
  /// and sets the 'future' parameter. Returns false if fetched all splits or if
  /// this operator is not the first operator in the pipeline and therefore is
  /// not responsible for fetching splits and adding them to the
  /// exchangeClient_.
  bool getSplits(ContinueFuture* future);

  void recordStats();

  const core::PlanNodeId planNodeId_;
  bool noMoreSplits_ = false;

  /// A future received from Task::getSplitOrFuture(). It will be complete when
  /// there are more splits available or no-more-splits signal has arrived.
  ContinueFuture splitFuture_{ContinueFuture::makeEmpty()};

  RowVectorPtr result_;
  std::shared_ptr<ExchangeClient> exchangeClient_;
  std::unique_ptr<SerializedPage> currentPage_;
  std::unique_ptr<ByteStream> inputStream_;
  bool atEnd_{false};
};

} // namespace facebook::velox::exec
