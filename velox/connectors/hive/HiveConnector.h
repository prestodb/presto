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

#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/IoStatistics.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/expression/Expr.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"

namespace facebook::velox::connector::hive {

class HiveColumnHandle : public ColumnHandle {
 public:
  enum class ColumnType { kPartitionKey, kRegular, kSynthesized };

  HiveColumnHandle(
      const std::string& name,
      ColumnType columnType,
      TypePtr dataType)
      : name_(name), columnType_(columnType), dataType_(std::move(dataType)) {}

  const std::string& name() const {
    return name_;
  }

  ColumnType columnType() const {
    return columnType_;
  }

  const TypePtr& dataType() const {
    return dataType_;
  }

  bool isPartitionKey() const {
    return columnType_ == ColumnType::kPartitionKey;
  }

 private:
  const std::string name_;
  const ColumnType columnType_;
  const TypePtr dataType_;
};

using SubfieldFilters =
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>;

class HiveTableHandle : public ConnectorTableHandle {
 public:
  HiveTableHandle(
      std::string connectorId,
      const std::string& tableName,
      bool filterPushdownEnabled,
      SubfieldFilters subfieldFilters,
      const core::TypedExprPtr& remainingFilter);

  ~HiveTableHandle() override;

  bool isFilterPushdownEnabled() const {
    return filterPushdownEnabled_;
  }

  const SubfieldFilters& subfieldFilters() const {
    return subfieldFilters_;
  }

  const core::TypedExprPtr& remainingFilter() const {
    return remainingFilter_;
  }

  std::string toString() const override;

 private:
  const std::string tableName_;
  const bool filterPushdownEnabled_;
  const SubfieldFilters subfieldFilters_;
  const core::TypedExprPtr remainingFilter_;
};

/// Location related properties of the Hive table to be written
class LocationHandle {
 public:
  enum class TableType {
    kNew, // Write to a new table to be created.
    kExisting, // Write to an existing table.
    kTemporary, // Write to a temporary table.
  };

  enum class WriteMode {
    // Write to a staging directory and then move to the target directory
    // after write finishes.
    kStageAndMoveToTargetDirectory,
    // Directly write to the target directory to be created.
    kDirectToTargetNewDirectory,
    // Directly write to the existing target directory.
    kDirectToTargetExistingDirectory,
  };

  LocationHandle(
      std::string targetPath,
      std::string writePath,
      TableType tableType,
      WriteMode writeMode)
      : targetPath_(std::move(targetPath)),
        writePath_(std::move(writePath)),
        tableType_(tableType),
        writeMode_(writeMode) {}

  const std::string& targetPath() const {
    return targetPath_;
  }

  const std::string& writePath() const {
    return writePath_;
  }

  TableType tableType() const {
    return tableType_;
  }

  WriteMode writeMode() const {
    return writeMode_;
  }

 private:
  // Target directory path.
  const std::string targetPath_;
  // Staging directory path.
  const std::string writePath_;
  // Whether the table to be written is new, already existing or temporary.
  const TableType tableType_;
  // How the target path and directory path could be used.
  const WriteMode writeMode_;
};

/**
 * Represents a request for Hive write
 */
class HiveInsertTableHandle : public ConnectorInsertTableHandle {
 public:
  HiveInsertTableHandle(
      std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns,
      std::shared_ptr<const LocationHandle> locationHandle)
      : inputColumns_(std::move(inputColumns)),
        locationHandle_(std::move(locationHandle)) {}

  virtual ~HiveInsertTableHandle() = default;

  const std::vector<std::shared_ptr<const HiveColumnHandle>>& inputColumns()
      const {
    return inputColumns_;
  }

  const std::shared_ptr<const LocationHandle>& locationHandle() const {
    return locationHandle_;
  }

  bool isPartitioned() const {
    return std::any_of(
        inputColumns_.begin(), inputColumns_.end(), [](auto column) {
          return column->isPartitionKey();
        });
  }

  bool isCreateTable() const {
    return locationHandle_->tableType() == LocationHandle::TableType::kNew;
  }

  bool isInsertTable() const {
    return locationHandle_->tableType() == LocationHandle::TableType::kExisting;
  }

 private:
  const std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns_;
  const std::shared_ptr<const LocationHandle> locationHandle_;
};

class HiveDataSink : public DataSink {
 public:
  explicit HiveDataSink(
      RowTypePtr inputType,
      std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx);

  void appendData(VectorPtr input) override;

  void close() override;

 private:
  std::unique_ptr<dwrf::Writer> createWriter();

  const RowTypePtr inputType_;
  const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle_;
  const ConnectorQueryCtx* connectorQueryCtx_;
  std::vector<std::unique_ptr<dwrf::Writer>> writers_;
};

class HiveConnector;

class HiveDataSource : public DataSource {
 public:
  HiveDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      FileHandleFactory* FOLLY_NONNULL fileHandleFactory,
      velox::memory::MemoryPool* FOLLY_NONNULL pool,
      ExpressionEvaluator* FOLLY_NONNULL expressionEvaluator,
      memory::MappedMemory* FOLLY_NONNULL mappedMemory,
      const std::string& scanId,
      folly::Executor* FOLLY_NULLABLE executor);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  uint64_t getCompletedBytes() override {
    return ioStats_->rawBytesRead();
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override;

  int64_t estimatedRowSize() override;

 private:
  // Evaluates remainingFilter_ on the specified vector. Returns number of rows
  // passed. Populates filterEvalCtx_.selectedIndices and selectedBits if only
  // some rows passed the filter. If none or all rows passed
  // filterEvalCtx_.selectedIndices and selectedBits are not updated.
  vector_size_t evaluateRemainingFilter(RowVectorPtr& rowVector);

  void setConstantValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const velox::variant& value) const;

  void setNullConstantValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const TypePtr& type) const;

  void setPartitionValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const std::string& partitionKey,
      const std::optional<std::string>& value) const;

  /// Clear split_, reader_ and rowReader_ after split has been fully processed.
  void resetSplit();

  const RowTypePtr outputType_;
  // Column handles for the partition key columns keyed on partition key column
  // name.
  std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>
      partitionKeys_;
  FileHandleFactory* FOLLY_NONNULL fileHandleFactory_;
  velox::memory::MemoryPool* FOLLY_NONNULL pool_;
  std::shared_ptr<dwio::common::IoStatistics> ioStats_;
  std::shared_ptr<dwio::common::BufferedInputFactory> bufferedInputFactory_;
  std::shared_ptr<common::ScanSpec> scanSpec_;
  std::shared_ptr<HiveConnectorSplit> split_;
  dwio::common::ReaderOptions readerOpts_;
  dwio::common::RowReaderOptions rowReaderOpts_;
  std::unique_ptr<dwio::common::Reader> reader_;
  std::unique_ptr<dwio::common::RowReader> rowReader_;
  std::unique_ptr<exec::ExprSet> remainingFilterExprSet_;
  RowTypePtr readerOutputType_;
  bool emptySplit_;

  dwio::common::RuntimeStatistics runtimeStats_;

  VectorPtr output_;
  FileHandleCachedPtr fileHandle_;
  ExpressionEvaluator* FOLLY_NONNULL expressionEvaluator_;
  uint64_t completedRows_ = 0;

  // Reusable memory for remaining filter evaluation
  VectorPtr filterResult_;
  SelectivityVector filterRows_;
  exec::FilterEvalCtx filterEvalCtx_;

  memory::MappedMemory* const FOLLY_NONNULL mappedMemory_;
  const std::string& scanId_;
  folly::Executor* FOLLY_NULLABLE executor_;
};

/// Hive connector configs
class HiveConfig {
 public:
  /// Can new data be inserted into existing partitions or existing
  /// unpartitioned tables
  static constexpr const char* kImmutablePartitions =
      "hive.immutable-partitions";

  static bool isImmutablePartitions(const Config* baseConfig) {
    return baseConfig->get<bool>(kImmutablePartitions, true);
  }
};

class HiveConnector final : public Connector {
 public:
  explicit HiveConnector(
      const std::string& id,
      std::shared_ptr<const Config> properties,
      folly::Executor* FOLLY_NULLABLE executor);

  bool canAddDynamicFilter() const override {
    return true;
  }

  std::shared_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx) override final {
    return std::make_shared<HiveDataSource>(
        outputType,
        tableHandle,
        columnHandles,
        &fileHandleFactory_,
        connectorQueryCtx->memoryPool(),
        connectorQueryCtx->expressionEvaluator(),
        connectorQueryCtx->mappedMemory(),
        connectorQueryCtx->scanId(),
        executor_);
  }

  std::shared_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx) override final {
    auto hiveInsertHandle = std::dynamic_pointer_cast<HiveInsertTableHandle>(
        connectorInsertTableHandle);
    VELOX_CHECK_NOT_NULL(
        hiveInsertHandle, "Hive connector expecting hive write handle!");
    return std::make_shared<HiveDataSink>(
        inputType, hiveInsertHandle, connectorQueryCtx);
  }

  folly::Executor* FOLLY_NULLABLE executor() {
    return executor_;
  }

 private:
  FileHandleFactory fileHandleFactory_;
  folly::Executor* FOLLY_NULLABLE executor_;

  static constexpr const char* FOLLY_NONNULL kNodeSelectionStrategy =
      "node_selection_strategy";
  static constexpr const char* FOLLY_NONNULL
      kNodeSelectionStrategyNoPreference = "NO_PREFERENCE";
  static constexpr const char* FOLLY_NONNULL
      kNodeSelectionStrategySoftAffinity = "SOFT_AFFINITY";
};

class HiveConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* FOLLY_NONNULL kHiveConnectorName = "hive";
  static constexpr const char* FOLLY_NONNULL kHiveHadoop2ConnectorName =
      "hive-hadoop2";

  HiveConnectorFactory() : ConnectorFactory(kHiveConnectorName) {
    dwio::common::FileSink::registerFactory();
  }

  HiveConnectorFactory(const char* FOLLY_NONNULL connectorName)
      : ConnectorFactory(connectorName) {
    dwio::common::FileSink::registerFactory();
  }

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> properties,
      folly::Executor* FOLLY_NULLABLE executor = nullptr) override {
    return std::make_shared<HiveConnector>(id, properties, executor);
  }
};

class HiveHadoop2ConnectorFactory : public HiveConnectorFactory {
 public:
  HiveHadoop2ConnectorFactory()
      : HiveConnectorFactory(kHiveHadoop2ConnectorName) {}
};

} // namespace facebook::velox::connector::hive
