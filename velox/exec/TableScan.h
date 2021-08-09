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
    if (hasBlockingFuture_) {
      hasBlockingFuture_ = false;
      *future = std::move(blockingFuture_);
      return BlockingReason::kWaitForSplit;
    }
    return BlockingReason::kNotBlocked;
  }

  void finish() override {
    isFinishing_ = true;
    close();
  }

  void close() override;

 private:
  static constexpr int32_t kDefaultBatchSize = 1024;

  const core::PlanNodeId planNodeId_;
  const std::shared_ptr<connector::ConnectorTableHandle> tableHandle_;
  const std::
      unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
          columnHandles_;
  DriverCtx* driverCtx_;
  ContinueFuture blockingFuture_;
  bool hasBlockingFuture_ = false;
  bool needNewSplit_ = true;
  std::shared_ptr<connector::Connector> connector_;
  std::unique_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  std::shared_ptr<connector::DataSource> dataSource_;
  bool noMoreSplits_ = false;
  // The bucketed group id we are in the middle of processing.
  int32_t currentSplitGroupId_{-1};
};
} // namespace facebook::velox::exec
