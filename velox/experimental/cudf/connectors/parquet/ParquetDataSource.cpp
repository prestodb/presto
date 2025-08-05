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

#include "velox/common/time/Timer.h"
#include "velox/expression/FieldReference.h"

#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/transform.hpp>

#include <cuda_runtime.h>

#include <filesystem>
#include <memory>
#include <string>

namespace facebook::velox::cudf_velox::connector::parquet {

using namespace facebook::velox::connector;

ParquetDataSource::ParquetDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& columnHandles,
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
  tableHandle_ =
      std::dynamic_pointer_cast<const ParquetTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      tableHandle_, "TableHandle must be an instance of ParquetTableHandle");

  // Create empty IOStats for later use
  ioStats_ = std::make_shared<io::IoStatistics>();

  // Create subfield filter
  auto subfieldFilter = tableHandle_->subfieldFilterExpr();
  if (subfieldFilter) {
    subfieldFilterExprSet_ = expressionEvaluator_->compile(subfieldFilter);
    // Add fields in the filter to the columns to read if not there
    for (const auto& field : subfieldFilterExprSet_->distinctFields()) {
      if (std::find(
              readColumnNames_.begin(),
              readColumnNames_.end(),
              field->name()) == readColumnNames_.end()) {
        readColumnNames_.push_back(field->name());
      }
    }
  }

  // Create remaining filter
  auto remainingFilter = tableHandle_->remainingFilter();
  if (remainingFilter) {
    remainingFilterExprSet_ = expressionEvaluator_->compile(remainingFilter);
    for (const auto& field : remainingFilterExprSet_->distinctFields()) {
      // Add fields in the filter to the columns to read if not there
      if (std::find(
              readColumnNames_.begin(),
              readColumnNames_.end(),
              field->name()) == readColumnNames_.end()) {
        readColumnNames_.push_back(field->name());
      }
    }

    const RowTypePtr remainingFilterType_ = [&] {
      if (tableHandle_->dataColumns()) {
        std::vector<std::string> new_names;
        std::vector<TypePtr> new_types;

        for (const auto& name : readColumnNames_) {
          auto parsedType = tableHandle_->dataColumns()->findChild(name);
          new_names.emplace_back(std::move(name));
          new_types.push_back(parsedType);
        }

        return ROW(std::move(new_names), std::move(new_types));
      } else {
        return outputType_;
      }
    }();

    cudfExpressionEvaluator_ = velox::cudf_velox::ExpressionEvaluator(
        remainingFilterExprSet_->exprs(), remainingFilterType_);
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

  // Record start time before reading chunk
  auto startTimeUs = getCurrentTimeMicro();

  std::unique_ptr<cudf::table> cudfTable;
  // Read a table chunk
  auto [table, metadata] = splitReader_->read_chunk();
  cudfTable = std::move(table);
  // Fill in the column names if reading the first chunk.
  if (columnNames_.empty()) {
    for (const auto& schema : metadata.schema_info) {
      columnNames_.emplace_back(schema.name);
    }
  }

  TotalScanTimeCallbackData* callbackData =
      new TotalScanTimeCallbackData{startTimeUs, ioStats_};

  // Launch host callback to calculate timing when scan completes
  cudaLaunchHostFunc(
      stream_.value(),
      &ParquetDataSource::totalScanTimeCalculator,
      callbackData);

  uint64_t filterTimeUs{0};
  // Apply remaining filter if present
  if (remainingFilterExprSet_) {
    MicrosecondTimer filterTimer(&filterTimeUs);
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
  totalRemainingFilterTime_.fetch_add(
      filterTimeUs * 1000, std::memory_order_relaxed);

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

void ParquetDataSource::totalScanTimeCalculator(void* userData) {
  TotalScanTimeCallbackData* data =
      static_cast<TotalScanTimeCallbackData*>(userData);

  // Record end time in callback
  auto endTimeUs = getCurrentTimeMicro();

  // Calculate elapsed time in microseconds and convert to nanoseconds
  auto elapsedUs = endTimeUs - data->startTimeUs;
  auto elapsedNs = elapsedUs * 1000; // Convert microseconds to nanoseconds

  // Update totalScanTime
  data->ioStats->incTotalScanTime(elapsedNs);

  delete data;
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

  if (subfieldFilterExprSet_) {
    auto subfieldFilterExpr = subfieldFilterExprSet_->expr(0);

    // non-ast instructions in filter is not supported for SubFieldFilter.
    // precomputeInstructions which are non-ast instructions should be empty.
    std::vector<PrecomputeInstruction> precomputeInstructions;

    const RowTypePtr readerFilterType_ = [&] {
      if (tableHandle_->dataColumns()) {
        std::vector<std::string> new_names;
        std::vector<TypePtr> new_types;

        for (const auto& name : readColumnNames_) {
          // Ensure all columns being read are available to the filter
          auto parsedType = tableHandle_->dataColumns()->findChild(name);
          new_names.emplace_back(std::move(name));
          new_types.push_back(parsedType);
        }

        return ROW(std::move(new_names), std::move(new_types));
      } else {
        return outputType_;
      }
    }();

    createAstTree(
        subfieldFilterExpr,
        subfieldTree_,
        subfieldScalars_,
        readerFilterType_,
        precomputeInstructions);
    VELOX_CHECK_EQ(precomputeInstructions.size(), 0);
    readerOptions.set_filter(subfieldTree_.back());
  }

  // Set column projection if needed
  if (readColumnNames_.size()) {
    readerOptions.set_columns(readColumnNames_);
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

std::unordered_map<std::string, RuntimeCounter>
ParquetDataSource::runtimeStats() {
  auto res = runtimeStats_.toMap();
  res.insert({
      {"totalScanTime",
       RuntimeCounter(ioStats_->totalScanTime(), RuntimeCounter::Unit::kNanos)},
      {"totalRemainingFilterTime",
       RuntimeCounter(
           totalRemainingFilterTime_.load(std::memory_order_relaxed),
           RuntimeCounter::Unit::kNanos)},
  });
  return res;
}

} // namespace facebook::velox::cudf_velox::connector::parquet
