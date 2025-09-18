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

#include "velox/experimental/cudf/tests/utils/CudfPlanBuilder.h"

#include "velox/dwio/common/Options.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::cudf_velox::exec::test {

std::function<PlanNodePtr(std::string, PlanNodePtr)> addCudfTableWriter(
    const RowTypePtr& inputColumns,
    const std::vector<std::string>& tableColumnNames,
    const std::optional<core::ColumnStatsSpec>& columnStatsSpec,
    const std::shared_ptr<core::InsertTableHandle>& insertHandle,
    facebook::velox::connector::CommitStrategy commitStrategy) {
  return [=](core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    return std::make_shared<core::TableWriteNode>(
        nodeId,
        inputColumns,
        tableColumnNames,
        columnStatsSpec,
        insertHandle,
        false,
        TableWriteTraits::outputType(columnStatsSpec),
        commitStrategy,
        std::move(source));
  };
}

std::function<PlanNodePtr(std::string, PlanNodePtr)> cudfTableWrite(
    const std::string& outputDirectoryPath,
    const dwio::common::FileFormat fileFormat,
    const std::optional<core::ColumnStatsSpec>& columnStatsSpec,
    const std::shared_ptr<dwio::common::WriterOptions>& options,
    const std::string& outputFileName) {
  return cudfTableWrite(
      outputDirectoryPath,
      fileFormat,
      columnStatsSpec,
      kCudfHiveConnectorId,
      {},
      options,
      outputFileName);
}

std::function<PlanNodePtr(std::string, PlanNodePtr)> cudfTableWrite(
    const std::string& outputDirectoryPath,
    const dwio::common::FileFormat fileFormat,
    const std::optional<core::ColumnStatsSpec>& columnStatsSpec,
    const std::string_view& connectorId,
    const std::unordered_map<std::string, std::string>& serdeParameters,
    const std::shared_ptr<dwio::common::WriterOptions>& options,
    const std::string& outputFileName,
    const common::CompressionKind compression,
    const RowTypePtr& schema) {
  return [=](core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    auto rowType = schema ? schema : source->outputType();

    auto locationHandle = CudfHiveConnectorTestBase::makeLocationHandle(
        outputDirectoryPath,
        cudf_velox::connector::hive::LocationHandle::TableType::kNew,
        outputFileName);
    auto parquetHandle =
        CudfHiveConnectorTestBase::makeCudfHiveInsertTableHandle(
            rowType->names(), rowType->children(), locationHandle, compression);
    auto insertHandle = std::make_shared<core::InsertTableHandle>(
        std::string(connectorId), parquetHandle);

    return std::make_shared<core::TableWriteNode>(
        nodeId,
        rowType,
        rowType->names(),
        columnStatsSpec,
        insertHandle,
        false,
        TableWriteTraits::outputType(columnStatsSpec),
        facebook::velox::connector::CommitStrategy::kNoCommit,
        std::move(source));
  };
}

} // namespace facebook::velox::cudf_velox::exec::test
