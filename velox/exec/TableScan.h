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
#include "velox/exec/ScaledScanController.h"

namespace facebook::velox::exec {

class TableScan : public SourceOperator {
 public:
  TableScan(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::TableScanNode>& tableScanNode);

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* future) override {
    if (blockingFuture_.valid()) {
      *future = std::move(blockingFuture_);
      return blockingReason_;
    }
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override;

  bool canAddDynamicFilter() const override {
    return connector_->canAddDynamicFilter();
  }

  void addDynamicFilter(
      const core::PlanNodeId& producer,
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

  /// The name of runtime stats specific to table scan.
  /// The number of running table scan drivers.
  ///
  /// NOTE: we only report the number of running scan drivers at the point that
  /// all the splits have been dispatched.
  static inline const std::string kNumRunningScaleThreads{
      "numRunningScaleThreads"};

  std::shared_ptr<ScaledScanController> testingScaledController() const {
    return scaledController_;
  }

 private:
  // Checks if this table scan operator needs to yield before processing the
  // next split.
  bool shouldYield(StopReason taskStopReason, size_t startTimeMs) const;

  // Checks if this table scan operator needs to stop because the task has been
  // terminated.
  bool shouldStop(StopReason taskStopReason) const;

  // Returns true if a new split is fetched from the task otherwise false.
  bool getSplit();

  // Sets 'maxPreloadSplits' and 'splitPreloader' if prefetching splits is
  // appropriate. The preloader will be applied to the 'first 'maxPreloadSplits'
  // of the Task's split queue for 'this' when getting splits.
  void checkPreload();

  // Sets 'split->dataSource' to be an AsyncSource that makes a DataSource to
  // read 'split'. This source will be prepared in the background on the
  // executor of the connector. If the DataSource is needed before prepare is
  // done, it will be made when needed.
  void preload(const std::shared_ptr<connector::ConnectorSplit>& split);

  // Invoked by scan operator to check if it needs to stop to wait for scale up.
  bool shouldWaitForScaleUp();

  // Invoked after scan operator finishes processing a non-empty split to update
  // the scan driver memory usage and check to see if we need to scale up scan
  // processing or not.
  void tryScaleUp();

  const std::shared_ptr<connector::ConnectorTableHandle> tableHandle_;
  const std::
      unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
          columnHandles_;
  DriverCtx* const driverCtx_;
  const int32_t maxSplitPreloadPerDriver_{0};
  const vector_size_t maxReadBatchSize_;
  memory::MemoryPool* const connectorPool_;
  const std::shared_ptr<connector::Connector> connector_;
  // Exits getOutput() method after this many milliseconds. Zero means 'no
  // limit'.
  const size_t getOutputTimeLimitMs_{0};

  // If set, used for scan scale processing. It is shared by all the scan
  // operators instantiated from the same table scan node.
  const std::shared_ptr<ScaledScanController> scaledController_;

  vector_size_t readBatchSize_;

  ContinueFuture blockingFuture_{ContinueFuture::makeEmpty()};
  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
  int64_t currentSplitWeight_{0};
  bool needNewSplit_ = true;
  std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  std::unique_ptr<connector::DataSource> dataSource_;
  bool noMoreSplits_ = false;
  // Dynamic filters to add to the data source when it gets created.
  std::unordered_map<column_index_t, std::shared_ptr<common::Filter>>
      dynamicFilters_;

  int32_t maxPreloadedSplits_{0};

  // Callback passed to getSplitOrFuture() for triggering async preload. The
  // callback's lifetime is the lifetime of 'this'. This callback can schedule
  // preloads on an executor. These preloads may outlive the Task and therefore
  // need to capture a shared_ptr to it.
  std::function<void(const std::shared_ptr<connector::ConnectorSplit>&)>
      splitPreloader_{nullptr};

  // Count of splits that started background preload.
  int32_t numPreloadedSplits_{0};

  // Count of splits that finished preloading before being read.
  int32_t numReadyPreloadedSplits_{0};

  double maxFilteringRatio_{0};

  // String shown in ExceptionContext inside DataSource and LazyVector loading.
  std::string debugString_;

  // The total number of raw input rows read up till the last finished split.
  // This is used to detect if a finished split is empty or not.
  uint64_t rawInputRowsSinceLastSplit_{0};
};
} // namespace facebook::velox::exec
