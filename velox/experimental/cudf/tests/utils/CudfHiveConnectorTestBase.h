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

#include "velox/experimental/cudf/connectors/hive/CudfHiveConfig.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveDataSink.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveDataSource.h"

#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

namespace facebook::velox::cudf_velox::exec::test {

static const std::string kCudfHiveConnectorId = "test-cudf-hive";

using ColumnHandleMap = std::unordered_map<
    std::string,
    std::shared_ptr<facebook::velox::connector::ColumnHandle>>;

class CudfHiveConnectorTestBase
    : public facebook::velox::exec::test::OperatorTestBase {
 public:
  CudfHiveConnectorTestBase();

  void SetUp() override;
  void TearDown() override;

  void resetCudfHiveConnector(
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

  static std::shared_ptr<facebook::velox::connector::hive::HiveConnectorSplit>
  makeCudfHiveConnectorSplit(
      const std::string& filePath,
      int64_t splitWeight = 0);

  static std::vector<
      std::shared_ptr<facebook::velox::connector::ConnectorSplit>>
  makeCudfHiveConnectorSplits(
      const std::vector<
          std::shared_ptr<facebook::velox::exec::test::TempFilePath>>&
          filePaths);

  static std::vector<
      std::shared_ptr<facebook::velox::connector::hive::HiveConnectorSplit>>
  makeCudfHiveConnectorSplits(const std::string& filePath, uint32_t splitCount);

  static std::shared_ptr<facebook::velox::connector::hive::HiveTableHandle>
  makeTableHandle(
      const std::string& tableName = "parquet_table",
      const RowTypePtr& dataColumns = nullptr,
      bool filterPushdownEnabled = false,
      common::SubfieldFilters subfieldFilters = {},
      const core::TypedExprPtr& remainingFilterExpr = nullptr) {
    return std::make_shared<facebook::velox::connector::hive::HiveTableHandle>(
        kCudfHiveConnectorId,
        tableName,
        filterPushdownEnabled,
        std::move(subfieldFilters),
        remainingFilterExpr,
        dataColumns);
  }

  /// @param name Column name.
  /// @param type Column type.
  /// @param Required subfields of this column.
  static std::shared_ptr<facebook::velox::connector::hive::HiveColumnHandle>
  makeColumnHandle(
      const std::string& name,
      const TypePtr& type,
      facebook::velox::connector::hive::HiveColumnHandle::ColumnType
          columnType = facebook::velox::connector::hive::HiveColumnHandle::
              ColumnType::kRegular,
      const std::vector<facebook::velox::common::Subfield>& requiredSubfields =
          {}) {
    return std::make_shared<facebook::velox::connector::hive::HiveColumnHandle>(
        name,
        columnType,
        type,
        type,
        std::vector<facebook::velox::common::Subfield>{});
  }

  /// @param name Column name.
  /// @param type Column type.
  /// @param type cudf column type.
  /// @param Required subfields of this column.
  static std::unique_ptr<connector::hive::CudfHiveColumnHandle>
  makeColumnHandle(
      const std::string& name,
      const TypePtr& type,
      const cudf::data_type data_type,
      const std::vector<connector::hive::CudfHiveColumnHandle>& children);

  /// @param targetDirectory Final directory of the target table.
  /// @param tableType Whether to create a new table.
  static std::shared_ptr<connector::hive::LocationHandle> makeLocationHandle(
      std::string targetDirectory) {
    return std::make_shared<connector::hive::LocationHandle>(
        targetDirectory, connector::hive::LocationHandle::TableType::kNew, "");
  }

  /// @param targetDirectory Final directory of the target table.
  /// @param tableType Whether to create a new table, insert into an existing
  /// table, or write a temporary table.
  /// @param targetDirectory Final file name of the target table .
  static std::shared_ptr<connector::hive::LocationHandle> makeLocationHandle(
      std::string targetDirectory,
      connector::hive::LocationHandle::TableType tableType =
          connector::hive::LocationHandle::TableType::kNew,
      std::string targetFileName = "") {
    return std::make_shared<connector::hive::LocationHandle>(
        targetDirectory, tableType, targetFileName);
  }

  /// Build a CudfHiveInsertTableHandle.
  /// @param tableColumnNames Column names of the target table. Corresponding
  /// type of tableColumnNames[i] is tableColumnTypes[i].
  /// @param tableColumnTypes Column types of the target table. Corresponding
  /// name of tableColumnTypes[i] is tableColumnNames[i].
  /// @param locationHandle Location handle for the table write.
  /// @param compressionKind compression algorithm to use for table write.
  /// @param serdeParameters Table writer configuration parameters.
  static std::shared_ptr<connector::hive::CudfHiveInsertTableHandle>
  makeCudfHiveInsertTableHandle(
      const std::vector<std::string>& tableColumnNames,
      const std::vector<TypePtr>& tableColumnTypes,
      std::shared_ptr<connector::hive::LocationHandle> locationHandle,
      const std::optional<common::CompressionKind> compressionKind = {},
      const std::unordered_map<std::string, std::string>& serdeParameters = {},
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr);
};

/// Same as connector::hive::CudfHiveConnectorBuilder, except that this
/// defaults connectorId to kCudfHiveConnectorId.
class CudfHiveConnectorSplitBuilder
    : public connector::hive::CudfHiveConnectorSplitBuilder {
 public:
  explicit CudfHiveConnectorSplitBuilder(std::string filePath)
      : connector::hive::CudfHiveConnectorSplitBuilder(filePath) {
    connectorId(kCudfHiveConnectorId);
  }
};

} // namespace facebook::velox::cudf_velox::exec::test
