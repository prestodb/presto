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

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

class TableScan : public SourceOperator {
 public:
  TableScan(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const core::TableScanNode> tableScanNode);

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* future) override {
    if (blockingFuture_.valid()) {
      *future = std::move(blockingFuture_);
      return BlockingReason::kWaitForSplit;
    }
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  bool canAddDynamicFilter() const override {
    // TODO Consult with the connector. Return true only if connector can accept
    // dynamic filters.
    return true;
  }

  void addDynamicFilter(
      ChannelIndex outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

 private:
  static constexpr int32_t kDefaultBatchSize = 1024;

  // Adjust batch size according to split information.
  void setBatchSize();

  const std::shared_ptr<connector::ConnectorTableHandle> tableHandle_;
  const std::
      unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
          columnHandles_;
  DriverCtx* driverCtx_;
  ContinueFuture blockingFuture_{ContinueFuture::makeEmpty()};
  bool needNewSplit_ = true;
  std::shared_ptr<connector::Connector> connector_;
  std::unique_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  std::shared_ptr<connector::DataSource> dataSource_;
  bool noMoreSplits_ = false;
  // Dynamic filters to add to the data source when it gets created.
  std::unordered_map<ChannelIndex, std::shared_ptr<common::Filter>>
      pendingDynamicFilters_;
  int32_t readBatchSize_{kDefaultBatchSize};
};
} // namespace facebook::velox::exec
