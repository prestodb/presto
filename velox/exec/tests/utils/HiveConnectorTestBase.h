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

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

namespace facebook::velox::exec::test {

static const std::string kHiveConnectorId = "test-hive";

using ColumnHandleMap =
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>;

class HiveConnectorTestBase : public OperatorTestBase {
 public:
  HiveConnectorTestBase();

  void SetUp() override;
  void TearDown() override;

  void resetHiveConnector(
      const std::shared_ptr<const config::ConfigBase>& config);

  void writeToFile(const std::string& filePath, RowVectorPtr vector);

  void writeToFile(
      const std::string& filePath,
      const std::vector<RowVectorPtr>& vectors,
      std::shared_ptr<dwrf::Config> config =
          std::make_shared<facebook::velox::dwrf::Config>(),
      const std::function<std::unique_ptr<dwrf::DWRFFlushPolicy>()>&
          flushPolicyFactory = nullptr);

  void writeToFile(
      const std::string& filePath,
      const std::vector<RowVectorPtr>& vectors,
      std::shared_ptr<dwrf::Config> config,
      const TypePtr& schema,
      const std::function<std::unique_ptr<dwrf::DWRFFlushPolicy>()>&
          flushPolicyFactory = nullptr);

  std::vector<RowVectorPtr> makeVectors(
      const RowTypePtr& rowType,
      int32_t numVectors,
      int32_t rowsPerVector);

  using OperatorTestBase::assertQuery;

  /// Assumes plan has a single TableScan node.
  std::shared_ptr<exec::Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql);

  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<connector::ConnectorSplit>>& splits,
      const std::string& duckDbSql,
      const int32_t numPrefetchSplit);

  static std::vector<std::shared_ptr<TempFilePath>> makeFilePaths(int count);

  static std::vector<std::shared_ptr<connector::ConnectorSplit>>
  makeHiveConnectorSplits(
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths);

  static std::shared_ptr<connector::hive::HiveConnectorSplit>
  makeHiveConnectorSplit(
      const std::string& filePath,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max(),
      int64_t splitWeight = 0);

  static std::shared_ptr<connector::hive::HiveConnectorSplit>
  makeHiveConnectorSplit(
      const std::string& filePath,
      int64_t fileSize,
      int64_t fileModifiedTime,
      uint64_t start,
      uint64_t length);

  /// Split file at path 'filePath' into 'splitCount' splits. If not local file,
  /// file size can be given as 'externalSize'.
  static std::vector<std::shared_ptr<connector::hive::HiveConnectorSplit>>
  makeHiveConnectorSplits(
      const std::string& filePath,
      uint32_t splitCount,
      dwio::common::FileFormat format,
      const std::optional<
          std::unordered_map<std::string, std::optional<std::string>>>&
          partitionKeys = {},
      const std::optional<std::unordered_map<std::string, std::string>>&
          infoColumns = {});

  static std::shared_ptr<connector::hive::HiveTableHandle> makeTableHandle(
      common::test::SubfieldFilters subfieldFilters = {},
      const core::TypedExprPtr& remainingFilter = nullptr,
      const std::string& tableName = "hive_table",
      const RowTypePtr& dataColumns = nullptr,
      bool filterPushdownEnabled = true) {
    return std::make_shared<connector::hive::HiveTableHandle>(
        kHiveConnectorId,
        tableName,
        filterPushdownEnabled,
        std::move(subfieldFilters),
        remainingFilter,
        dataColumns);
  }

  /// @param name Column name.
  /// @param type Column type.
  /// @param Required subfields of this column.
  static std::unique_ptr<connector::hive::HiveColumnHandle> makeColumnHandle(
      const std::string& name,
      const TypePtr& type,
      const std::vector<std::string>& requiredSubfields);

  /// @param name Column name.
  /// @param type Column type.
  /// @param type Hive type.
  /// @param Required subfields of this column.
  static std::unique_ptr<connector::hive::HiveColumnHandle> makeColumnHandle(
      const std::string& name,
      const TypePtr& dataType,
      const TypePtr& hiveType,
      const std::vector<std::string>& requiredSubfields,
      connector::hive::HiveColumnHandle::ColumnType columnType =
          connector::hive::HiveColumnHandle::ColumnType::kRegular);

  /// @param targetDirectory Final directory of the target table after commit.
  /// @param writeDirectory Write directory of the target table before commit.
  /// @param tableType Whether to create a new table, insert into an existing
  /// table, or write a temporary table.
  /// @param writeMode How to write to the target directory.
  static std::shared_ptr<connector::hive::LocationHandle> makeLocationHandle(
      std::string targetDirectory,
      std::optional<std::string> writeDirectory = std::nullopt,
      connector::hive::LocationHandle::TableType tableType =
          connector::hive::LocationHandle::TableType::kNew) {
    return std::make_shared<connector::hive::LocationHandle>(
        targetDirectory, writeDirectory.value_or(targetDirectory), tableType);
  }

  /// Build a HiveInsertTableHandle.
  /// @param tableColumnNames Column names of the target table. Corresponding
  /// type of tableColumnNames[i] is tableColumnTypes[i].
  /// @param tableColumnTypes Column types of the target table. Corresponding
  /// name of tableColumnTypes[i] is tableColumnNames[i].
  /// @param partitionedBy A list of partition columns of the target table.
  /// @param bucketProperty if not nulll, specifies the property for a bucket
  /// table.
  /// @param locationHandle Location handle for the table write.
  /// @param compressionKind compression algorithm to use for table write.
  static std::shared_ptr<connector::hive::HiveInsertTableHandle>
  makeHiveInsertTableHandle(
      const std::vector<std::string>& tableColumnNames,
      const std::vector<TypePtr>& tableColumnTypes,
      const std::vector<std::string>& partitionedBy,
      std::shared_ptr<connector::hive::HiveBucketProperty> bucketProperty,
      std::shared_ptr<connector::hive::LocationHandle> locationHandle,
      const dwio::common::FileFormat tableStorageFormat =
          dwio::common::FileFormat::DWRF,
      const std::optional<common::CompressionKind> compressionKind = {},
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr);

  static std::shared_ptr<connector::hive::HiveInsertTableHandle>
  makeHiveInsertTableHandle(
      const std::vector<std::string>& tableColumnNames,
      const std::vector<TypePtr>& tableColumnTypes,
      const std::vector<std::string>& partitionedBy,
      std::shared_ptr<connector::hive::LocationHandle> locationHandle,
      const dwio::common::FileFormat tableStorageFormat =
          dwio::common::FileFormat::DWRF,
      const std::optional<common::CompressionKind> compressionKind = {},
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr);

  static std::shared_ptr<connector::hive::HiveColumnHandle> regularColumn(
      const std::string& name,
      const TypePtr& type);

  static std::shared_ptr<connector::hive::HiveColumnHandle> partitionKey(
      const std::string& name,
      const TypePtr& type);

  static std::shared_ptr<connector::hive::HiveColumnHandle> synthesizedColumn(
      const std::string& name,
      const TypePtr& type);

  static ColumnHandleMap allRegularColumns(const RowTypePtr& rowType) {
    ColumnHandleMap assignments;
    assignments.reserve(rowType->size());
    for (uint32_t i = 0; i < rowType->size(); ++i) {
      const auto& name = rowType->nameOf(i);
      assignments[name] = regularColumn(name, rowType->childAt(i));
    }
    return assignments;
  }
};

class HiveConnectorSplitBuilder {
 public:
  HiveConnectorSplitBuilder(std::string filePath)
      : filePath_{std::move(filePath)} {}

  HiveConnectorSplitBuilder& start(uint64_t start) {
    start_ = start;
    return *this;
  }

  HiveConnectorSplitBuilder& length(uint64_t length) {
    length_ = length;
    return *this;
  }

  HiveConnectorSplitBuilder& splitWeight(int64_t splitWeight) {
    splitWeight_ = splitWeight;
    return *this;
  }

  HiveConnectorSplitBuilder& fileFormat(dwio::common::FileFormat format) {
    fileFormat_ = format;
    return *this;
  }

  HiveConnectorSplitBuilder& infoColumn(
      const std::string& name,
      const std::string& value) {
    infoColumns_.emplace(std::move(name), std::move(value));
    return *this;
  }

  HiveConnectorSplitBuilder& partitionKey(
      std::string name,
      std::optional<std::string> value) {
    partitionKeys_.emplace(std::move(name), std::move(value));
    return *this;
  }

  HiveConnectorSplitBuilder& tableBucketNumber(int32_t bucket) {
    tableBucketNumber_ = bucket;
    return *this;
  }

  HiveConnectorSplitBuilder& customSplitInfo(
      const std::unordered_map<std::string, std::string>& customSplitInfo) {
    customSplitInfo_ = customSplitInfo;
    return *this;
  }

  HiveConnectorSplitBuilder& extraFileInfo(
      const std::shared_ptr<std::string>& extraFileInfo) {
    extraFileInfo_ = extraFileInfo;
    return *this;
  }

  HiveConnectorSplitBuilder& serdeParameters(
      const std::unordered_map<std::string, std::string>& serdeParameters) {
    serdeParameters_ = serdeParameters;
    return *this;
  }

  HiveConnectorSplitBuilder& connectorId(const std::string& connectorId) {
    connectorId_ = connectorId;
    return *this;
  }

  std::shared_ptr<connector::hive::HiveConnectorSplit> build() const {
    static const std::unordered_map<std::string, std::string> customSplitInfo;
    static const std::shared_ptr<std::string> extraFileInfo;
    static const std::unordered_map<std::string, std::string> serdeParameters;
    return std::make_shared<connector::hive::HiveConnectorSplit>(
        connectorId_,
        filePath_.find("/") == 0 ? "file:" + filePath_ : filePath_,
        fileFormat_,
        start_,
        length_,
        partitionKeys_,
        tableBucketNumber_,
        customSplitInfo,
        extraFileInfo,
        serdeParameters,
        splitWeight_,
        infoColumns_,
        std::nullopt);
  }

 private:
  const std::string filePath_;
  dwio::common::FileFormat fileFormat_{dwio::common::FileFormat::DWRF};
  uint64_t start_{0};
  uint64_t length_{std::numeric_limits<uint64_t>::max()};
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys_;
  std::optional<int32_t> tableBucketNumber_;
  std::unordered_map<std::string, std::string> customSplitInfo_ = {};
  std::shared_ptr<std::string> extraFileInfo_ = {};
  std::unordered_map<std::string, std::string> serdeParameters_ = {};
  std::unordered_map<std::string, std::string> infoColumns_ = {};
  std::string connectorId_ = kHiveConnectorId;
  int64_t splitWeight_{0};
};

} // namespace facebook::velox::exec::test
