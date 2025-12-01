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

#include "presto_cpp/main/tvf/core/TableFunctionProcessorNode.h"
#include "presto_cpp/main/tvf/exec/TableFunctionSplit.h"

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::presto::tvf {

class LeafTableFunctionOperator : public velox::exec::SourceOperator {
 public:
  LeafTableFunctionOperator(
      int32_t operatorId,
      velox::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const TableFunctionProcessorNode>&
          tableFunctionProcessorNode);

  void initialize() override;

  velox::RowVectorPtr getOutput() override;

  velox::exec::BlockingReason isBlocked(
      velox::ContinueFuture* /* unused */) override {
    return velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreSplits_;
  }

  void reclaim(
      uint64_t targetBytes,
      velox::memory::MemoryReclaimer::Stats& stats) override;

 private:
  bool spillEnabled() const {
    return spillConfig_.has_value();
  }

  void createTableFunctionSplitProcessor();

  void clear();

  velox::exec::DriverCtx* const driverCtx_;

  velox::memory::MemoryPool* pool_;
  // HashStringAllocator required by functions that allocate out of line
  // buffers.
  velox::HashStringAllocator stringAllocator_;

  std::shared_ptr<const TableFunctionProcessorNode> tableFunctionProcessorNode_;

  std::shared_ptr<TableFunctionResult> result_;

  // This should be constructed for each split.
  std::unique_ptr<TableFunctionSplitProcessor> splitProcessor_;

  bool noMoreSplits_ = false;
  std::shared_ptr<TableFunctionSplit> currentSplit_;

  velox::ContinueFuture blockingFuture_{velox::ContinueFuture::makeEmpty()};
  velox::exec::BlockingReason blockingReason_{
      velox::exec::BlockingReason::kNotBlocked};
  std::function<void(const std::shared_ptr<velox::connector::ConnectorSplit>&)>
      splitPreloader_{nullptr};
};

} // namespace facebook::presto::tvf
