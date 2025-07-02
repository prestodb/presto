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
#include "presto_cpp/main/tvf/exec/TableFunctionConnector.h"

#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;
using namespace facebook::velox::exec;

TableFunctionDataSource::TableFunctionDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    velox::memory::MemoryPool* pool)
    : pool_(pool), stringAllocator_(pool_) {
  auto functionHandle =
      std::dynamic_pointer_cast<TableFunctionTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      functionHandle, "TableHandle must be an instance of TableFunctionHandle");

  // Initialize the Table Function here.
  // TODO: Don't know how to retrieve QueryConfig here. So creating an empty
  // one.
  velox::core::QueryConfig c{{}};
  function_ = TableFunction::create(
      functionHandle->functionName(),
      functionHandle->handle(),
      pool_,
      &stringAllocator_,
      c);
  VELOX_CHECK(function_);

  for (const auto& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column '{}' on table function '{}'",
        outputName,
        functionHandle->functionName());

    auto handle = std::dynamic_pointer_cast<TableFunctionHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of TableFunctionHandle "
        "for '{}' on table function '{}'",
        it->second->name(),
        functionHandle->functionName());
  }
  outputType_ = outputType;
}

void TableFunctionDataSource::addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) {
  VELOX_CHECK_EQ(
      currentSplit_,
      nullptr,
      "Previous split has not been processed yet. Call next() to process the split.");

  currentSplit_ = std::dynamic_pointer_cast<TableFunctionConnectorSplit>(split);
  VELOX_CHECK(currentSplit_, "Wrong type of split for Table function.");
}

std::optional<RowVectorPtr> TableFunctionDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(
      currentSplit_, "No split to process. Call addSplit() first.");

  // GetOutput from table function.
  VELOX_CHECK(function_);
  auto result = function_->apply(currentSplit_->splitHandle());
  if (result->state() == TableFunctionResult::TableFunctionState::kFinished) {
    // Clear the split as the input rows are completely consumed.
    currentSplit_ = nullptr;
    return std::nullopt;
  }

  VELOX_CHECK(
      result->state() == TableFunctionResult::TableFunctionState::kProcessed);
  VELOX_CHECK(!result->usedInput());

  // Don't really understand why the dynamic_pointer_cast is needed.
  auto resultRows = dynamic_pointer_cast<RowVector>(result->result());
  VELOX_CHECK(resultRows);

  return std::move(resultRows);
}

} // namespace facebook::presto::tvf
