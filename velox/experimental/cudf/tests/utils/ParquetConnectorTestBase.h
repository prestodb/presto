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
#include "velox/experimental/cudf/connectors/parquet/ParquetConnector.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetDataSink.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetDataSource.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetTableHandle.h"

#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

namespace facebook::velox::cudf_velox::exec::test {

static const std::string kParquetConnectorId = "test-parquet";

using ColumnHandleMap = std::unordered_map<
    std::string,
    std::shared_ptr<facebook::velox::connector::ColumnHandle>>;

class ParquetConnectorTestBase
    : public facebook::velox::exec::test::OperatorTestBase {
 public:
  ParquetConnectorTestBase();

  void SetUp() override;
  void TearDown() override;

  void resetParquetConnector(
      const std::shared_ptr<const facebook::velox::config::ConfigBase>& config);

  void writeToFile(
      const std::string& filePath,
      RowVectorPtr vector,
      std::string prefix = "c");

  void writeToFile(
      const std::string& filePath,
      const std::vector<RowVectorPtr>& vectors,
      std::string prefix = "c");

  std::vector<RowVectorPtr> makeVectors(
      const RowTypePtr& rowType,
      int32_t numVectors,
      int32_t rowsPerVector);

  using facebook::velox::exec::test::OperatorTestBase::assertQuery;

  /// Assumes plan has a single TableScan node.
  std::shared_ptr<facebook::velox::exec::Task> assertQuery(
      const facebook::velox::core::PlanNodePtr& plan,
      const std::vector<
          std::shared_ptr<facebook::velox::exec::test::TempFilePath>>&
          filePaths,
      const std::string& duckDbSql);

  std::shared_ptr<facebook::velox::exec::Task> assertQuery(
      const facebook::velox::core::PlanNodePtr& plan,
      const std::vector<
          std::shared_ptr<facebook::velox::connector::ConnectorSplit>>& splits,
      const std::string& duckDbSql,
      const int32_t numPrefetchSplit);

  static std::vector<std::shared_ptr<facebook::velox::exec::test::TempFilePath>>
  makeFilePaths(int count);

  static std::shared_ptr<
      facebook::velox::cudf_velox::connector::parquet::ParquetConnectorSplit>
  makeParquetConnectorSplit(
      const std::string& filePath,
      int64_t splitWeight = 0);

  static std::vector<
      std::shared_ptr<facebook::velox::connector::ConnectorSplit>>
  makeParquetConnectorSplits(
      const std::vector<
          std::shared_ptr<facebook::velox::exec::test::TempFilePath>>&
          filePaths);

  static std::vector<std::shared_ptr<connector::parquet::ParquetConnectorSplit>>
  makeParquetConnectorSplits(const std::string& filePath, uint32_t splitCount);

  static std::shared_ptr<connector::parquet::ParquetTableHandle>
  makeTableHandle(
      const std::string& tableName = "parquet_table",
      const RowTypePtr& dataColumns = nullptr,
      bool filterPushdownEnabled = false,
      const core::TypedExprPtr& subfieldFilterExpr = nullptr,
      const core::TypedExprPtr& remainingFilterExpr = nullptr) {
    return std::make_shared<connector::parquet::ParquetTableHandle>(
        kParquetConnectorId,
        tableName,
        filterPushdownEnabled,
        subfieldFilterExpr,
        remainingFilterExpr,
        dataColumns);
  }

  /// @param name Column name.
  /// @param type Column type.
  /// @param Required subfields of this column.
  static std::unique_ptr<connector::parquet::ParquetColumnHandle>
  makeColumnHandle(
      const std::string& name,
      const TypePtr& type,
      const std::vector<connector::parquet::ParquetColumnHandle>& children);

  /// @param name Column name.
  /// @param type Column type.
  /// @param type cudf column type.
  /// @param Required subfields of this column.
  static std::unique_ptr<connector::parquet::ParquetColumnHandle>
  makeColumnHandle(
      const std::string& name,
      const TypePtr& type,
      const cudf::data_type data_type,
      const std::vector<connector::parquet::ParquetColumnHandle>& children);

  /// @param targetDirectory Final directory of the target table.
  /// @param tableType Whether to create a new table.
  static std::shared_ptr<connector::parquet::LocationHandle> makeLocationHandle(
      std::string targetDirectory) {
    return std::make_shared<connector::parquet::LocationHandle>(
        targetDirectory,
        connector::parquet::LocationHandle::TableType::kNew,
        "");
  }

  /// @param targetDirectory Final directory of the target table.
  /// @param tableType Whether to create a new table, insert into an existing
  /// table, or write a temporary table.
  /// @param targetDirectory Final file name of the target table .
  static std::shared_ptr<connector::parquet::LocationHandle> makeLocationHandle(
      std::string targetDirectory,
      connector::parquet::LocationHandle::TableType tableType =
          connector::parquet::LocationHandle::TableType::kNew,
      std::string targetFileName = "") {
    return std::make_shared<connector::parquet::LocationHandle>(
        targetDirectory, tableType, targetFileName);
  }

  /// Build a ParquetInsertTableHandle.
  /// @param tableColumnNames Column names of the target table. Corresponding
  /// type of tableColumnNames[i] is tableColumnTypes[i].
  /// @param tableColumnTypes Column types of the target table. Corresponding
  /// name of tableColumnTypes[i] is tableColumnNames[i].
  /// @param locationHandle Location handle for the table write.
  /// @param compressionKind compression algorithm to use for table write.
  /// @param serdeParameters Table writer configuration parameters.
  static std::shared_ptr<connector::parquet::ParquetInsertTableHandle>
  makeParquetInsertTableHandle(
      const std::vector<std::string>& tableColumnNames,
      const std::vector<TypePtr>& tableColumnTypes,
      std::shared_ptr<connector::parquet::LocationHandle> locationHandle,
      const std::optional<common::CompressionKind> compressionKind = {},
      const std::unordered_map<std::string, std::string>& serdeParameters = {},
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr);
};

/// Same as connector::parquet::ParquetConnectorBuilder, except that this
/// defaults connectorId to kParquetConnectorId.
class ParquetConnectorSplitBuilder
    : public connector::parquet::ParquetConnectorSplitBuilder {
 public:
  explicit ParquetConnectorSplitBuilder(std::string filePath)
      : connector::parquet::ParquetConnectorSplitBuilder(filePath) {
    connectorId(kParquetConnectorId);
  }
};

} // namespace facebook::velox::cudf_velox::exec::test
