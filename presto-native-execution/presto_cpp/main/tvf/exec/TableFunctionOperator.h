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

#include "presto_cpp/main/tvf/core/TableFunctionProcessorNode.h"
#include "presto_cpp/main/tvf/exec/TableFunctionPartition.h"
#include "presto_cpp/main/tvf/exec/TablePartitionBuild.h"

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
      const std::shared_ptr<const TableFunctionProcessorNode>&
          tableFunctionProcessorNode);

  void initialize() override;

  void addInput(velox::RowVectorPtr input) override;

  void noMoreInput() override;

  velox::RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_;
  }

  velox::exec::BlockingReason isBlocked(
      velox::ContinueFuture* /* unused */) override {
    return velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    // There is no input and the function has completed as well.
    return (noMoreInput_ && input_ == nullptr);
  }

  void reclaim(uint64_t targetBytes, velox::memory::MemoryReclaimer::Stats& stats)
      override;

  private:
  bool spillEnabled() const {
    return spillConfig_.has_value();
  }

  void createTableFunctionDataProcessor(
      const std::shared_ptr<const TableFunctionProcessorNode>&
          tableFunctionProcessorNode);

  void assembleInput();

  velox::memory::MemoryPool* pool_;
  // HashStringAllocator required by functions that allocate out of line
  // buffers.
  velox::HashStringAllocator stringAllocator_;

  std::shared_ptr<const TableFunctionProcessorNode> tableFunctionProcessorNode_;

  // TODO : Figure how this works for a multi-input table parameter case.
  velox::RowTypePtr inputType_;

  // This would be a list when the operator supports multiple TableArguments.
  const velox::RowTypePtr requiredColummType_;

  // TablePartitionBuild is used to store input rows and return
  // TableFunctionPartitions for the processing.
  std::unique_ptr<TablePartitionBuild> tablePartitionBuild_;

  std::shared_ptr<TableFunctionPartition> tableFunctionPartition_;

  velox::RowVectorPtr input_;

  // This should be constructed for each partition.
  std::unique_ptr<TableFunctionDataProcessor> dataProcessor_;

  velox::vector_size_t numRows_ = 0;
  velox::vector_size_t numProcessedRows_ = 0;
  velox::vector_size_t numPartitionProcessedRows_ = 0;
  // Number of rows that be fit into an output block.
  velox::vector_size_t numRowsPerOutput_;
};

} // namespace facebook::presto::tvf
