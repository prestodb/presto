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
#include "presto_cpp/main/tvf/exec/LeafTableFunctionOperator.h"

#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/exec/Task.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;
using namespace facebook::velox::exec;

LeafTableFunctionOperator::LeafTableFunctionOperator(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const TableFunctionNodePtr& tableFunctionNode)
    : SourceOperator(
          driverCtx,
          tableFunctionNode->outputType(),
          operatorId,
          tableFunctionNode->id(),
          "LeafTableFunctionOperator"),
      driverCtx_(driverCtx),
      pool_(pool()),
      stringAllocator_(pool_),
      tableFunctionNode_(tableFunctionNode),
      result_(nullptr) {
  VELOX_CHECK(tableFunctionNode->sources().empty());
}

void LeafTableFunctionOperator::initialize() {
  Operator::initialize();
  VELOX_CHECK_NOT_NULL(tableFunctionNode_);
  createTableFunction(tableFunctionNode_);
  // TODO: Why was this needed
  // tableFunctionNode_.reset();
}

void LeafTableFunctionOperator::createTableFunction(
    const std::shared_ptr<const TableFunctionNode>& node) {
  function_ = TableFunction::create(
      node->functionName(),
      node->handle(),
      pool_,
      &stringAllocator_,
      operatorCtx_->driverCtx()->queryConfig());
  VELOX_CHECK(function_);
}

RowVectorPtr LeafTableFunctionOperator::getOutput() {
  if (noMoreSplits_) {
    return nullptr;
  }

  if (currentSplit_ == nullptr) {
    // Try to retrieve the next split. If no more splits then return.
    exec::Split split;
    blockingReason_ = driverCtx_->task->getSplitOrFuture(
        driverCtx_->splitGroupId,
        planNodeId(),
        split,
        blockingFuture_,
        0,
        splitPreloader_);

    if (blockingReason_ != BlockingReason::kNotBlocked) {
      return nullptr;
    }

    if (!split.hasConnectorSplit()) {
      noMoreSplits_ = true;
      return nullptr;
    }

    currentSplit_ =
        std::dynamic_pointer_cast<TableFunctionSplit>(split.connectorSplit);
    VELOX_CHECK(currentSplit_, "Invalid Table Function Split");
  }

  // This split could be one retrieved above or a incompletely processed one
  // from the previous getOutput.
  VELOX_CHECK_NOT_NULL(
      currentSplit_, "No split to process.");

  // GetOutput from table function.
  VELOX_CHECK(function_);
  auto result = function_->apply(currentSplit_->splitHandle());
  if (result->state() == TableFunctionResult::TableFunctionState::kFinished) {
    // Clear the split as the input rows are completely consumed.
    currentSplit_ = nullptr;
    return nullptr;
  }

  VELOX_CHECK(
      result->state() == TableFunctionResult::TableFunctionState::kProcessed);
  VELOX_CHECK(!result->usedInput());

  // Don't really understand why the dynamic_pointer_cast is needed.
  auto resultRows = dynamic_pointer_cast<RowVector>(result->result());
  VELOX_CHECK(resultRows);

  return std::move(resultRows);
}

void LeafTableFunctionOperator::reclaim(
    uint64_t /*targetBytes*/,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_NYI("LeafTableFunctionOperator::reclaim not implemented");
}

} // namespace facebook::presto::tvf
