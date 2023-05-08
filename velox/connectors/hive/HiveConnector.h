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
#include "velox/connectors/hive/HiveDataSink.h"
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
      TypePtr dataType,
      std::vector<common::Subfield> requiredSubfields = {})
      : name_(name),
        columnType_(columnType),
        dataType_(std::move(dataType)),
        requiredSubfields_(std::move(requiredSubfields)) {}

  const std::string& name() const {
    return name_;
  }

  ColumnType columnType() const {
    return columnType_;
  }

  const TypePtr& dataType() const {
    return dataType_;
  }

  // Applies to columns of complex types: arrays, maps and structs.  When a
  // query uses only some of the subfields, the engine provides the complete
  // list of required subfields and the connector is free to prune the rest.
  //
  // Examples:
  //  - SELECT a[1], b['x'], x.y FROM t
  //  - SELECT a FROM t WHERE b['y'] > 10
  //
  // Pruning a struct means populating some of the members with null values.
  //
  // Pruning a map means dropping keys not listed in the required subfields.
  //
  // Pruning arrays means dropping values with indices larger than maximum
  // required index.
  const std::vector<common::Subfield>& requiredSubfields() const {
    return requiredSubfields_;
  }

  bool isPartitionKey() const {
    return columnType_ == ColumnType::kPartitionKey;
  }

 private:
  const std::string name_;
  const ColumnType columnType_;
  const TypePtr dataType_;
  const std::vector<common::Subfield> requiredSubfields_;
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

class HiveConnector;

class HiveDataSource : public DataSource {
 public:
  HiveDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      FileHandleFactory* fileHandleFactory,
      velox::memory::MemoryPool* pool,
      ExpressionEvaluator* expressionEvaluator,
      memory::MemoryAllocator* allocator,
      const std::string& scanId,
      folly::Executor* executor);

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

  bool allPrefetchIssued() const override {
    return rowReader_ && rowReader_->allPrefetchIssued();
  }

  void setFromDataSource(std::shared_ptr<DataSource> source) override;

  int64_t estimatedRowSize() override;

  // Internal API, made public to be accessible in unit tests.  Do not use in
  // other places.
  static std::shared_ptr<common::ScanSpec> makeScanSpec(
      const SubfieldFilters& filters,
      const RowTypePtr& rowType,
      const std::vector<const HiveColumnHandle*>& columnHandles,
      memory::MemoryPool* pool);

 protected:
  virtual uint64_t readNext(uint64_t size) {
    return rowReader_->next(size, output_);
  }

  std::unique_ptr<dwio::common::BufferedInput> createBufferedInput(
      const FileHandle&,
      const dwio::common::ReaderOptions&);

  virtual std::unique_ptr<dwio::common::RowReader> createRowReader(
      dwio::common::RowReaderOptions& options) {
    return reader_->createRowReader(options);
  }

  std::shared_ptr<HiveConnectorSplit> split_;
  FileHandleFactory* fileHandleFactory_;
  dwio::common::ReaderOptions readerOpts_;
  memory::MemoryPool* pool_;
  VectorPtr output_;
  RowTypePtr readerOutputType_;
  std::unique_ptr<dwio::common::RowReader> rowReader_;

 private:
  // Evaluates remainingFilter_ on the specified vector. Returns number of rows
  // passed. Populates filterEvalCtx_.selectedIndices and selectedBits if only
  // some rows passed the filter. If none or all rows passed
  // filterEvalCtx_.selectedIndices and selectedBits are not updated.
  vector_size_t evaluateRemainingFilter(RowVectorPtr& rowVector);

  void setConstantValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const TypePtr& type,
      const velox::variant& value) const;

  void setNullConstantValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const TypePtr& type) const;

  void setPartitionValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const std::string& partitionKey,
      const std::optional<std::string>& value) const;

  // Clear split_ after split has been fully processed.  Keep readers around to
  // hold adaptation.
  void resetSplit();

  void configureRowReaderOptions(dwio::common::RowReaderOptions&) const;

  const RowTypePtr outputType_;
  // Column handles for the partition key columns keyed on partition key column
  // name.
  std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>
      partitionKeys_;
  std::shared_ptr<dwio::common::IoStatistics> ioStats_;
  std::shared_ptr<common::ScanSpec> scanSpec_;
  std::shared_ptr<common::MetadataFilter> metadataFilter_;
  dwio::common::RowReaderOptions rowReaderOpts_;
  std::unique_ptr<dwio::common::Reader> reader_;
  std::unique_ptr<exec::ExprSet> remainingFilterExprSet_;
  bool emptySplit_;

  dwio::common::RuntimeStatistics runtimeStats_;

  FileHandleCachedPtr fileHandle_;
  ExpressionEvaluator* expressionEvaluator_;
  uint64_t completedRows_ = 0;

  // Reusable memory for remaining filter evaluation.
  VectorPtr filterResult_;
  SelectivityVector filterRows_;
  exec::FilterEvalCtx filterEvalCtx_;

  memory::MemoryAllocator* const allocator_;
  const std::string& scanId_;
  folly::Executor* executor_;
};

class HiveConnector : public Connector {
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
      ConnectorQueryCtx* connectorQueryCtx) override {
    return std::make_shared<HiveDataSource>(
        outputType,
        tableHandle,
        columnHandles,
        &fileHandleFactory_,
        connectorQueryCtx->memoryPool(),
        connectorQueryCtx->expressionEvaluator(),
        connectorQueryCtx->allocator(),
        connectorQueryCtx->scanId(),
        executor_);
  }

  bool supportsSplitPreload() override {
    return true;
  }

  std::shared_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) override final {
    auto hiveInsertHandle = std::dynamic_pointer_cast<HiveInsertTableHandle>(
        connectorInsertTableHandle);
    VELOX_CHECK_NOT_NULL(
        hiveInsertHandle, "Hive connector expecting hive write handle!");
    return std::make_shared<HiveDataSink>(
        inputType, hiveInsertHandle, connectorQueryCtx, commitStrategy);
  }

  folly::Executor* FOLLY_NULLABLE executor() const override {
    return executor_;
  }

  FileHandleCacheStats fileHandleCacheStats() {
    return fileHandleFactory_.cacheStats();
  }

  // NOTE: this is to clear file handle cache which might affect performance,
  // and is only used for operational purposes.
  FileHandleCacheStats clearFileHandleCache() {
    return fileHandleFactory_.clearCache();
  }

 protected:
  FileHandleFactory fileHandleFactory_;
  folly::Executor* FOLLY_NULLABLE executor_;
};

class HiveConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* FOLLY_NONNULL kHiveConnectorName = "hive";
  static constexpr const char* FOLLY_NONNULL kHiveHadoop2ConnectorName =
      "hive-hadoop2";

  HiveConnectorFactory() : ConnectorFactory(kHiveConnectorName) {
    dwio::common::LocalFileSink::registerFactory();
  }

  HiveConnectorFactory(const char* FOLLY_NONNULL connectorName)
      : ConnectorFactory(connectorName) {
    dwio::common::LocalFileSink::registerFactory();
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

class HivePartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  HivePartitionFunctionSpec(
      int numBuckets,
      std::vector<int> bucketToPartition,
      std::vector<column_index_t> channels,
      std::vector<VectorPtr> constValues)
      : numBuckets_(numBuckets),
        bucketToPartition_(std::move(bucketToPartition)),
        channels_(std::move(channels)),
        constValues_(std::move(constValues)) {}

  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions) const override;

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& obj,
      void* context);

 private:
  const int numBuckets_;
  const std::vector<int> bucketToPartition_;
  const std::vector<column_index_t> channels_;
  const std::vector<VectorPtr> constValues_;
};

void registerHivePartitionFunctionSerDe();

} // namespace facebook::velox::connector::hive
