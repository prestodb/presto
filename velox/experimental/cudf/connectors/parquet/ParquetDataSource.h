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

#include "velox/experimental/cudf/connectors/parquet/ParquetConfig.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetConnectorSplit.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetTableHandle.h"
#include "velox/experimental/cudf/exec/ExpressionEvaluator.h"
#include "velox/experimental/cudf/exec/NvtxHelper.h"

#include "velox/common/base/RandomUtil.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/connectors/Connector.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/type/Type.h"

#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>

namespace facebook::velox::cudf_velox::connector::parquet {

using namespace facebook::velox::connector;

class ParquetDataSource : public DataSource, public NvtxHelper {
 public:
  ParquetDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
          columnHandles,
      folly::Executor* executor,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<ParquetConfig>& ParquetConfig);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  void addDynamicFilter(
      column_index_t /*outputChannel*/,
      const std::shared_ptr<facebook::velox::common::Filter>& /*filter*/)
      override {
    VELOX_NYI("Dynamic filters not yet implemented by cudf::ParquetConnector.");
  }

  std::optional<RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& /* future */) override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    // TODO: Which stats do we want to expose here?
    return {};
  }

 private:
  // Create a cudf::io::chunked_parquet_reader with the given split.
  std::unique_ptr<cudf::io::chunked_parquet_reader> createSplitReader();
  // Clear split_ and splitReader after split has been fully processed.  Keep
  // readers around to hold adaptation.
  void resetSplit();
  // Clear cudfTable_ and currentCudfTableView_ once we have successfully
  // converted it to `RowVectorPtr` and returned.
  void resetCudfTableAndView();
  const RowVectorPtr& getEmptyOutput() {
    if (!emptyOutput_) {
      emptyOutput_ = RowVector::createEmpty(outputType_, pool_);
    }
    return emptyOutput_;
  }
  RowVectorPtr emptyOutput_;

  std::shared_ptr<ParquetConnectorSplit> split_;
  std::shared_ptr<ParquetTableHandle> tableHandle_;

  const std::shared_ptr<ParquetConfig> parquetConfig_;

  folly::Executor* const executor_;
  const ConnectorQueryCtx* const connectorQueryCtx_;

  memory::MemoryPool* const pool_;

  // cuDF Parquet reader stuff.
  cudf::io::parquet_reader_options readerOptions_;
  std::unique_ptr<cudf::io::chunked_parquet_reader> splitReader_;
  rmm::cuda_stream_view stream_;

  // Table column names read from the Parquet file
  std::vector<std::string> columnNames_;

  // Output type from file reader.  This is different from outputType_ that it
  // contains column names before assignment, and columns that only used in
  // remaining filter.
  RowTypePtr readerOutputType_;

  // Columns to read.
  std::vector<std::string> readColumnNames_;

  std::shared_ptr<io::IoStatistics> ioStats_;

  size_t completedRows_{0};
  size_t completedBytes_{0};

  // The row type for the data source output, not including filter-only columns
  const RowTypePtr outputType_;

  // Expression evaluator for remaining filter.
  core::ExpressionEvaluator* const expressionEvaluator_;
  std::unique_ptr<exec::ExprSet> remainingFilterExprSet_;
  velox::cudf_velox::ExpressionEvaluator cudfExpressionEvaluator_;

  // Expression evaluator for subfield filter.
  std::vector<std::unique_ptr<cudf::scalar>> subfieldScalars_;
  cudf::ast::tree subfieldTree_;
  std::unique_ptr<exec::ExprSet> subfieldFilterExprSet_;

  dwio::common::RuntimeStatistics runtimeStats_;
};

} // namespace facebook::velox::cudf_velox::connector::parquet
