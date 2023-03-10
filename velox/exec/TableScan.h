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

DECLARE_int32(split_preload_per_driver);

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
      return blockingReason_;
    }
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  bool canAddDynamicFilter() const override {
    return connector_->canAddDynamicFilter();
  }

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

 private:
  static constexpr int32_t kDefaultBatchSize = 1024;

  // Sets 'maxPreloadSplits' and 'splitPreloader' if prefetching
  // splits is appropriate. The preloader will be applied to the
  // 'first 'maxPreloadSplits' of the Tasks's split queue for 'this'
  // when getting splits.
  void checkPreload();

  // Sets 'split->dataSource' to be a Asyncsource that makes a
  // DataSource to read 'split'. This source will be prepared in the
  // background on the executor of the connector. If the DataSource is
  // needed before prepare is done, it will be made when needed.
  void preload(std::shared_ptr<connector::ConnectorSplit> split);

  // Adjust batch size according to split information.
  void setBatchSize();

  const std::shared_ptr<connector::ConnectorTableHandle> tableHandle_;
  const std::
      unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
          columnHandles_;
  DriverCtx* driverCtx_;
  ContinueFuture blockingFuture_{ContinueFuture::makeEmpty()};
  BlockingReason blockingReason_;
  bool needNewSplit_ = true;
  std::shared_ptr<connector::Connector> connector_;
  std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  std::shared_ptr<connector::DataSource> dataSource_;
  bool noMoreSplits_ = false;
  // Dynamic filters to add to the data source when it gets created.
  std::unordered_map<column_index_t, std::shared_ptr<common::Filter>>
      pendingDynamicFilters_;

  int32_t maxPreloadedSplits_{0};

  // Callback passed to getSplitOrFuture() for triggering async
  // preload. The callback's lifetime is the lifetime of 'this'. This
  // callback can schedule preloads on an executor. These preloads may
  // outlive the Task and therefore need to capture a shared_ptr to
  // it.
  std::function<void(std::shared_ptr<connector::ConnectorSplit>)>
      splitPreloader_{nullptr};

  // Count of splits that started background preload.
  int32_t numPreloadedSplits_{0};

  // Count of splits that finished preloading before being read.
  int32_t numReadyPreloadedSplits_{0};

  int32_t readBatchSize_{kDefaultBatchSize};

  // String shown in ExceptionContext inside DataSource and LazyVector loading.
  std::string debugString_;
};
} // namespace facebook::velox::exec
