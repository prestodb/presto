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

#include "presto_cpp/main/tvf/core/TableFunctionNode.h"
#include "presto_cpp/main/tvf/exec/TableFunctionPartition.h"

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::presto::tvf {

class TableFunctionOperator : public velox::exec::Operator {
 public:
  TableFunctionOperator(
      int32_t operatorId,
      velox::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const TableFunctionNode>& tableFunctionNode);

  void initialize() override;

  void addInput(velox::RowVectorPtr input) override;

  void noMoreInput() override;

  velox::RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_ && needsInput_;
  }

  velox::exec::BlockingReason isBlocked(
      velox::ContinueFuture* /* unused */) override {
    return velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    // There is no input and the function has completed as well.
    return (noMoreInput_ && input_ == nullptr);
  }

  void reclaim(
      uint64_t targetBytes,
      velox::memory::MemoryReclaimer::Stats& stats) override;

 private:
  bool spillEnabled() const {
    return spillConfig_.has_value();
  }

  void createTableFunction(
      const std::shared_ptr<const TableFunctionNode>& tableFunctionNode);

  void assembleInput();

  void clear();

  velox::memory::MemoryPool* pool_;
  // HashStringAllocator required by functions that allocate out of line
  // buffers.
  velox::HashStringAllocator stringAllocator_;

  std::shared_ptr<const TableFunctionNode> tableFunctionNode_;

  // TODO : Figure how this works for a multi-input table parameter case.
  velox::RowTypePtr inputType_;

  // This would be a list when the operator supports multiple TableArguments.
  const velox::RowTypePtr requiredColummType_;

  /// The RowContainer holds all the input rows in WindowBuild. Columns are
  /// already reordered according to inputChannels_.
  std::unique_ptr<velox::exec::RowContainer> data_;

  /// The decodedInputVectors_ are reused across addInput() calls to decode the
  /// partition and sort keys for the above RowContainer.
  std::vector<velox::DecodedVector> decodedInputVectors_;

  // Vector of pointers to each input row in the data_ RowContainer.
  std::vector<char*> rows_;

  std::unique_ptr<TableFunctionPartition> tableFunctionPartition_;

  // The TableFunction operator processes input rows one batch at a time.
  // Algorithm : addInput -> batches input in RowContainer. Sets needsInput_ to
  // false. getOutput() calls TableFunction::process. Returns output from
  // function. If all input rows are processed then set needsInput_ to true for
  // next batch of input. Else, continue to process current batch in next
  // process call when getOutput.
  bool needsInput_;

  std::shared_ptr<TableFunctionResult> result_;

  velox::RowVectorPtr input_;

  // This should be constructed for each partition.
  std::unique_ptr<TableFunction> function_;
};

// Custom translation logic to hook into Velox Driver.
class TableFunctionTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node) {
    if (auto tableFunctionNodeNode =
            std::dynamic_pointer_cast<const TableFunctionNode>(node)) {
      return std::make_unique<TableFunctionOperator>(
          id, ctx, tableFunctionNodeNode);
    }
    return nullptr;
  }
};
} // namespace facebook::presto::tvf
