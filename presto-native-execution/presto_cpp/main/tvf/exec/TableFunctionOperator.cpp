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

#include "presto_cpp/main/tvf/exec/TableFunctionOperator.h"

#include "presto_cpp/main/tvf/exec/TableFunctionPartition.h"

#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

const std::vector<RowTypePtr> requiredColumnTypes(
    const TableFunctionProcessorNodePtr& tableFunctionProcessorNode) {
  auto columns = tableFunctionProcessorNode->requiredColumns();
  auto inputType = tableFunctionProcessorNode->sources()[0]->outputType();
  std::vector<RowTypePtr> result;
  result.reserve(columns.size());
  for (const auto& sourceColumns : columns) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (const auto& idx : sourceColumns) {
      names.push_back(inputType->nameOf(idx));
      types.push_back(inputType->childAt(idx));
    }
    result.push_back(ROW(std::move(names), std::move(types)));
  }
  return result;
}
} // namespace

TableFunctionOperator::TableFunctionOperator(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const TableFunctionProcessorNodePtr& tableFunctionProcessorNode)
    : Operator(
          driverCtx,
          tableFunctionProcessorNode->outputType(),
          operatorId,
          tableFunctionProcessorNode->id(),
          "TableFunctionOperator",
          tableFunctionProcessorNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId, "TableFunction")
              : std::nullopt),
      pool_(pool()),
      stringAllocator_(pool_),
      tableFunctionProcessorNode_(tableFunctionProcessorNode),
      inputType_(tableFunctionProcessorNode->sources()[0]->outputType()),
      requiredColumnTypes_(requiredColumnTypes(tableFunctionProcessorNode)),
      tableFunctionPartition_(nullptr),
      functionInput_({}),
      future_(nullptr) {
  tablePartitionBuild_ = std::make_unique<TablePartitionBuild>(
      tableFunctionProcessorNode,
      requiredColumnTypes_,
      pool(),
      common::PrefixSortConfig{
          driverCtx->queryConfig().prefixSortNormalizedKeyMaxBytes(),
          driverCtx->queryConfig().prefixSortMinRows(),
          driverCtx->queryConfig().prefixSortMaxStringPrefixLength()});
  numRowsPerOutput_ = outputBatchRows(tablePartitionBuild_->estimateRowSize());
}

void TableFunctionOperator::initialize() {
  Operator::initialize();
  VELOX_CHECK_NOT_NULL(tableFunctionProcessorNode_);
}

void TableFunctionOperator::createTableFunctionDataProcessor(
    const std::shared_ptr<const TableFunctionProcessorNode>& node) {
  dataProcessor_ = TableFunction::createDataProcessor(
      node->functionName(),
      node->handle(),
      pool_,
      &stringAllocator_,
      operatorCtx_->driverCtx()->queryConfig());
  VELOX_CHECK(dataProcessor_);
}

// Writing the code to add the input rows -> call TableFunction::process and
// return the rows from it. This is done per input vectors basis. If we have
// partition by an order by this would need a change but just testing with a
// simple model for now.
void TableFunctionOperator::addInput(RowVectorPtr input) {
  numRows_ += input->size();

  tablePartitionBuild_->addInput(input);
}

void TableFunctionOperator::noMoreInput() {
  Operator::noMoreInput();
  tablePartitionBuild_->noMoreInput();
}

RowVectorPtr TableFunctionOperator::getOutputFromFunction() {
  VELOX_CHECK(tableFunctionPartition_);
  VELOX_CHECK(dataProcessor_);

  if (!validFunctionInput_) {
    bool allRowsProcessed =
        (tableFunctionPartition_->numRows() - numPartitionProcessedRows_) == 0;
    bool hasMultipleInputs = requiredColumnTypes_.size() > 1;
    bool shouldTryAssemble = !allRowsProcessed ||
        (allRowsProcessed && numPartitionProcessedRows_ == 0 &&
         hasMultipleInputs);

    if (allRowsProcessed && numPartitionProcessedRows_ > 0 &&
        !calledWithNullptr_ && hasMultipleInputs) {
      functionInput_.clear();
      for (size_t i = 0; i < requiredColumnTypes_.size(); i++) {
        functionInput_.push_back(nullptr);
      }
      calledWithNullptr_ = true;
    } else if (shouldTryAssemble) {
      functionInput_ = tableFunctionPartition_->assembleInput(
          numRowsPerOutput_, numPartitionProcessedRows_);
    } else {
      return nullptr;
    }
    validFunctionInput_ = true;
  }

  auto result = dataProcessor_->apply(functionInput_);

  if (result->state() == TableFunctionResult::TableFunctionState::kFinished) {
    numProcessedRows_ +=
        (tableFunctionPartition_->numRows() - numPartitionProcessedRows_);
    tableFunctionPartition_ = nullptr;
    numPartitionProcessedRows_ = 0;
    validFunctionInput_ = false;
    if (numRows_ == 0) {
      finishedEmptyPartition_ = true;
    }
    return nullptr;
  }

  if (result->state() == TableFunctionResult::TableFunctionState::kBlocked) {
    future_ = std::move(result->future());
    return nullptr;
  }

  VELOX_CHECK(
      result->state() == TableFunctionResult::TableFunctionState::kProcessed);
  auto resultRows = result->result();

  if (result->usedInput()) {
    velox::vector_size_t totalInputRows = 0;
    for (const auto& input : functionInput_) {
      if (input) {
        totalInputRows = std::max(totalInputRows, input->size());
      }
    }
    numPartitionProcessedRows_ += totalInputRows;
    numProcessedRows_ += totalInputRows;
    validFunctionInput_ = false;
  }

  if (!resultRows || resultRows->size() == 0) {
    return nullptr;
  }

  auto finalResult =
      tableFunctionPartition_->appendPassThroughColumns(resultRows);
  return (finalResult && finalResult->size() > 0) ? std::move(finalResult)
                                                  : nullptr;
}

RowVectorPtr TableFunctionOperator::getOutput() {
  if (!noMoreInput_) {
    return nullptr;
  }

  auto initNewPartition = [&]() -> void {
    createTableFunctionDataProcessor(tableFunctionProcessorNode_);
    numPartitionProcessedRows_ = 0;
    validFunctionInput_ = false;
    calledWithNullptr_ = false;
    finishedEmptyPartition_ = false;
  };

  auto noRemainingInputForPartition = [&]() -> bool {
    bool hasPartition = tableFunctionPartition_ != nullptr;
    bool allRowsProcessed = hasPartition &&
        (tableFunctionPartition_->numRows() - numPartitionProcessedRows_ == 0);
    // Only multiple inputs need the nullptr call
    bool hasMultipleInputs = requiredColumnTypes_.size() > 1;
    // IMPORTANT: Don't consider partition done if validFunctionInput_ is true
    // This means the function has output to produce but hasn't consumed the
    // input yet
    return hasPartition && allRowsProcessed &&
        (!hasMultipleInputs || calledWithNullptr_) && !validFunctionInput_;
  };

  if (numRows_ == 0) {
    bool hasMultipleInputs = requiredColumnTypes_.size() > 1;

    if (!hasMultipleInputs && tableFunctionProcessorNode_->pruneWhenEmpty()) {
      return nullptr;
    }
    if (finishedEmptyPartition_) {
      return nullptr;
    }
    if (tableFunctionPartition_ == nullptr) {
      tableFunctionPartition_ = tablePartitionBuild_->emptyPartition();
      initNewPartition();
    }
  } else {
    if (numRows_ - numProcessedRows_ == 0 && noRemainingInputForPartition()) {
      return nullptr;
    }

    if (tableFunctionPartition_ == nullptr || noRemainingInputForPartition()) {
      if (tablePartitionBuild_->hasNextPartition()) {
        tableFunctionPartition_ = tablePartitionBuild_->nextPartition();
        initNewPartition();
      } else {
        if (requiredColumnTypes_.size() > 1 && !calledWithNullptr_) {
          calledWithNullptr_ = true;
        }
        return nullptr;
      }
    }
  }

  return getOutputFromFunction();
}

void TableFunctionOperator::reclaim(
    uint64_t /*targetBytes*/,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_NYI("TableFunctionOperator::reclaim not implemented");
}

} // namespace facebook::presto::tvf
