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

#include "velox/common/caching/SsdFile.h" // Needed by presto_cpp
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/HiveDataSource.h"
#include "velox/dwio/common/DataSink.h"

namespace facebook::velox::connector::hive {

class HiveConnector : public Connector {
 public:
  explicit HiveConnector(
      const std::string& id,
      std::shared_ptr<const Config> properties,
      folly::Executor* FOLLY_NULLABLE executor);

  bool canAddDynamicFilter() const override {
    return true;
  }

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override {
    return std::make_unique<HiveDataSource>(
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

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) override final {
    auto hiveInsertHandle = std::dynamic_pointer_cast<HiveInsertTableHandle>(
        connectorInsertTableHandle);
    VELOX_CHECK_NOT_NULL(
        hiveInsertHandle, "Hive connector expecting hive write handle!");
    return std::make_unique<HiveDataSink>(
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
