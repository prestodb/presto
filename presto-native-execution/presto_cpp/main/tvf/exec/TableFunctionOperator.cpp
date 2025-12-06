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

const RowTypePtr requiredColumnType(
    const std::string& name,
    const TableFunctionProcessorNodePtr& tableFunctionProcessorNode) {
  auto columns = tableFunctionProcessorNode->requiredColumns();

  // TODO: This assumes single source.
  auto inputType = tableFunctionProcessorNode->sources()[0]->outputType();
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (const auto& idx : columns) {
    names.push_back(inputType->nameOf(idx));
    types.push_back(inputType->childAt(idx));
  }
  return ROW(std::move(names), std::move(types));
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
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      pool_(pool()),
      stringAllocator_(pool_),
      tableFunctionProcessorNode_(tableFunctionProcessorNode),
      inputType_(tableFunctionProcessorNode->sources()[0]->outputType()),
      requiredColummType_(requiredColumnType("t1", tableFunctionProcessorNode)),
      tableFunctionPartition_(nullptr),
      input_(nullptr) {
  tablePartitionBuild_ = std::make_unique<TablePartitionBuild>(
      inputType_,
      tableFunctionProcessorNode->partitionKeys(),
      tableFunctionProcessorNode->sortingKeys(),
      tableFunctionProcessorNode->sortingOrders(),
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

void TableFunctionOperator::assembleInput() {
  VELOX_CHECK(tableFunctionPartition_);

  const auto numRowsLeft =
      tableFunctionPartition_->numRows() - numPartitionProcessedRows_;
  VELOX_CHECK_GT(numRowsLeft, 0);
  const auto numOutputRows = std::min(numRowsPerOutput_, numRowsLeft);
  auto input =
      BaseVector::create<RowVector>(requiredColummType_, numOutputRows, pool_);

  auto columns = tableFunctionProcessorNode_->requiredColumns();
  for (int i = 0; i < requiredColummType_->children().size(); i++) {
    input->childAt(i)->resize(numOutputRows);
    tableFunctionPartition_->extractColumn(
        columns[i],
        numPartitionProcessedRows_,
        numOutputRows,
        0,
        input->childAt(i));
  }
  input_ = std::move(input);
}

RowVectorPtr TableFunctionOperator::getOutput() {
  if (!noMoreInput_) {
    return nullptr;
  }

  if (numRows_ == 0) {
    return nullptr;
  }

  const auto numRowsLeft = numRows_ - numProcessedRows_;
  if (numRowsLeft == 0) {
    return nullptr;
  }

  if (tableFunctionPartition_ == nullptr ||
      (!input_ &&
       (tableFunctionPartition_->numRows() - numPartitionProcessedRows_ ==
        0))) {
    if (tablePartitionBuild_->hasNextPartition()) {
      tableFunctionPartition_ = tablePartitionBuild_->nextPartition();
      createTableFunctionDataProcessor(tableFunctionProcessorNode_);
      numPartitionProcessedRows_ = 0;
    } else {
      // There is no partition to output.
      return nullptr;
    }
  }

  // This is the first call to TableFunction::apply for this partition
  // or a previous apply for this input has completed.
  if (input_ == nullptr) {
    assembleInput();
  }

  VELOX_CHECK(dataProcessor_);
  auto result = dataProcessor_->apply({input_});
  if (result->state() == TableFunctionResult::TableFunctionState::kFinished) {
    // Skip the rest of this partition processing.
    numProcessedRows_ +=
        (tableFunctionPartition_->numRows() - numPartitionProcessedRows_);
    tableFunctionPartition_ = nullptr;
    numPartitionProcessedRows_ = 0;
    return nullptr;
  }

  VELOX_CHECK(
      result->state() == TableFunctionResult::TableFunctionState::kProcessed);
  auto resultRows = result->result();
  VELOX_CHECK(resultRows);
  if (result->usedInput()) {
    // The input rows were consumed, so we need to re-assemble input at the
    // next call.
    numPartitionProcessedRows_ += input_->size();
    numProcessedRows_ += input_->size();
    input_ = nullptr;
  }
  return std::move(resultRows);
}

void TableFunctionOperator::reclaim(
    uint64_t /*targetBytes*/,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_NYI("TableFunctionOperator::reclaim not implemented");
}

} // namespace facebook::presto::tvf
