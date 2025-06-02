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

#include "velox/experimental/cudf/connectors/parquet/ParquetConfig.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetConnectorSplit.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetDataSource.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetTableHandle.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/transform.hpp>

#include <filesystem>
#include <memory>
#include <string>

namespace facebook::velox::cudf_velox::connector::parquet {

using namespace facebook::velox::connector;

ParquetDataSource::ParquetDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
        columnHandles,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<ParquetConfig>& parquetConfig)
    : NvtxHelper(
          nvtx3::rgb{80, 171, 241}, // Parquet blue,
          std::nullopt,
          fmt::format("[{}]", tableHandle->name())),
      parquetConfig_(parquetConfig),
      executor_(executor),
      connectorQueryCtx_(connectorQueryCtx),
      pool_(connectorQueryCtx->memoryPool()),
      outputType_(outputType),
      expressionEvaluator_(connectorQueryCtx->expressionEvaluator()) {
  // Set up column projection if needed
  auto readColumnTypes = outputType_->children();
  for (const auto& outputName : outputType_->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column: {}",
        outputName);

    auto* handle = static_cast<const ParquetColumnHandle*>(it->second.get());
    readColumnNames_.emplace_back(handle->name());
  }

  // Dynamic cast tableHandle to ParquetTableHandle
  tableHandle_ = std::dynamic_pointer_cast<ParquetTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      tableHandle_, "TableHandle must be an instance of ParquetTableHandle");

  // Create empty IOStats for later use
  ioStats_ = std::make_shared<io::IoStatistics>();

  // Create subfield filter
  auto subfieldFilter = tableHandle_->subfieldFilterExpr();
  if (subfieldFilter) {
    subfieldFilterExprSet_ = expressionEvaluator_->compile(subfieldFilter);
  }

  // Create remaining filter
  auto remainingFilter = tableHandle_->remainingFilter();
  if (remainingFilter) {
    remainingFilterExprSet_ = expressionEvaluator_->compile(remainingFilter);
    cudfExpressionEvaluator_ = velox::cudf_velox::ExpressionEvaluator(
        remainingFilterExprSet_->exprs(), outputType_);
    // TODO(kn): Get column names and subfields from remaining filter and add to
    // readColumnNames_
  }
}

std::optional<RowVectorPtr> ParquetDataSource::next(
    uint64_t /*size*/,
    velox::ContinueFuture& /* future */) {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  // Basic sanity checks
  VELOX_CHECK_NOT_NULL(split_, "No split to process. Call addSplit first.");
  VELOX_CHECK_NOT_NULL(splitReader_, "No split reader present");

  if (not splitReader_->has_next()) {
    return nullptr;
  }

  // Read a table chunk
  auto [table, metadata] = splitReader_->read_chunk();
  auto cudfTable = std::move(table);
  // Fill in the column names if reading the first chunk.
  if (columnNames_.empty()) {
    for (auto schema : metadata.schema_info) {
      columnNames_.emplace_back(schema.name);
    }
  }

  // Apply remaining filter if present
  if (remainingFilterExprSet_) {
    auto cudfTableColumns = cudfTable->release();
    const auto originalNumColumns = cudfTableColumns.size();
    // Filter may need addtional computed columns which are added to
    // cudfTableColumns
    auto filterResult = cudfExpressionEvaluator_.compute(
        cudfTableColumns, stream_, cudf::get_current_device_resource_ref());
    // discard computed columns
    std::vector<std::unique_ptr<cudf::column>> originalColumns;
    originalColumns.reserve(originalNumColumns);
    std::move(
        cudfTableColumns.begin(),
        cudfTableColumns.begin() + originalNumColumns,
        std::back_inserter(originalColumns));
    auto originalTable =
        std::make_unique<cudf::table>(std::move(originalColumns));
    // Keep only rows where the filter is true
    cudfTable = cudf::apply_boolean_mask(
        *originalTable,
        *filterResult[0],
        stream_,
        cudf::get_current_device_resource_ref());
  }

  // Output RowVectorPtr
  const auto nRows = cudfTable->num_rows();

  // keep only outputType_.size() columns in cudfTable_
  if (outputType_->size() < cudfTable->num_columns()) {
    auto cudfTableColumns = cudfTable->release();
    std::vector<std::unique_ptr<cudf::column>> originalColumns;
    originalColumns.reserve(outputType_->size());
    std::move(
        cudfTableColumns.begin(),
        cudfTableColumns.begin() + outputType_->size(),
        std::back_inserter(originalColumns));
    cudfTable = std::make_unique<cudf::table>(std::move(originalColumns));
  }

  auto output = cudfIsRegistered()
      ? std::make_shared<CudfVector>(
            pool_, outputType_, nRows, std::move(cudfTable), stream_)
      : with_arrow::toVeloxColumn(
            cudfTable->view(), pool_, outputType_->names(), stream_);
  stream_.synchronize();

  // Check if conversion yielded a nullptr
  VELOX_CHECK_NOT_NULL(output, "Cudf to Velox conversion yielded a nullptr");

  // Update completedRows_.
  completedRows_ += output->size();

  // TODO: Update `completedBytes_` here instead of in `addSplit()`

  return output;
}

void ParquetDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  // Dynamic cast split to `ParquetConnectorSplit`
  split_ = std::dynamic_pointer_cast<ParquetConnectorSplit>(split);
  VLOG(1) << "Adding split " << split_->toString();

  // Split reader already exists, reset
  if (splitReader_) {
    splitReader_.reset();
  }

  // Clear columnNames if not empty
  if (not columnNames_.empty()) {
    columnNames_.clear();
  }

  // Create a `cudf::io::chunked_parquet_reader` SplitReader
  splitReader_ = createSplitReader();

  // TODO: `completedBytes_` should be updated in `next()` as we read more and
  // more table bytes
  const auto& filePaths = split_->getCudfSourceInfo().filepaths();
  for (const auto& filePath : filePaths) {
    completedBytes_ += std::filesystem::file_size(filePath);
  }
}

std::unique_ptr<cudf::io::chunked_parquet_reader>
ParquetDataSource::createSplitReader() {
  // Reader options
  auto readerOptions =
      cudf::io::parquet_reader_options::builder(split_->getCudfSourceInfo())
          .skip_rows(parquetConfig_->skipRows())
          .use_pandas_metadata(parquetConfig_->isUsePandasMetadata())
          .use_arrow_schema(parquetConfig_->isUseArrowSchema())
          .allow_mismatched_pq_schemas(
              parquetConfig_->isAllowMismatchedParquetSchemas())
          .timestamp_type(parquetConfig_->timestampType())
          .build();

  // Set num_rows only if available
  if (parquetConfig_->numRows().has_value()) {
    readerOptions.set_num_rows(parquetConfig_->numRows().value());
  }

  // Set column projection if needed
  if (readColumnNames_.size()) {
    readerOptions.set_columns(readColumnNames_);
  }
  if (subfieldFilterExprSet_) {
    auto subfieldFilterExpr = subfieldFilterExprSet_->expr(0);
    // non-ast instructions in filter is not supported for SubFieldFilter.
    // precomputeInstructions which are non-ast instructions should be empty.
    std::vector<PrecomputeInstruction> precomputeInstructions;
    createAstTree(
        subfieldFilterExpr,
        subfieldTree_,
        subfieldScalars_,
        outputType_,
        precomputeInstructions);
    VELOX_CHECK_EQ(precomputeInstructions.size(), 0);
    readerOptions.set_filter(subfieldTree_.back());
  }
  stream_ = cudfGlobalStreamPool().get_stream();
  // Create a parquet reader
  return std::make_unique<cudf::io::chunked_parquet_reader>(
      parquetConfig_->maxChunkReadLimit(),
      parquetConfig_->maxPassReadLimit(),
      readerOptions,
      stream_,
      cudf::get_current_device_resource_ref());
}

void ParquetDataSource::resetSplit() {
  split_.reset();
  splitReader_.reset();
  columnNames_.clear();
}

} // namespace facebook::velox::cudf_velox::connector::parquet
