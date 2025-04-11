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

#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/core/PlanNode.h"

namespace facebook::velox::dwio::common {
class DataSink;
class DataSource;
} // namespace facebook::velox::dwio::common

namespace facebook::velox::connector::hive {

class HiveConnector : public Connector {
 public:
  HiveConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor);

  const std::shared_ptr<const config::ConfigBase>& connectorConfig()
      const override {
    return hiveConfig_->config();
  }

  bool canAddDynamicFilter() const override {
    return true;
  }

  ConnectorMetadata* metadata() const override {
    VELOX_CHECK_NOT_NULL(metadata_);
    return metadata_.get();
  }

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override;

  bool supportsSplitPreload() override {
    return true;
  }

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) override;

  folly::Executor* executor() const override {
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
  const std::shared_ptr<HiveConfig> hiveConfig_;
  FileHandleFactory fileHandleFactory_;
  folly::Executor* executor_;
  std::shared_ptr<ConnectorMetadata> metadata_;
};

class HiveConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kHiveConnectorName = "hive";

  HiveConnectorFactory() : ConnectorFactory(kHiveConnectorName) {}

  explicit HiveConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<HiveConnector>(id, config, ioExecutor);
  }
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

  /// The constructor without 'bucketToPartition' input is used in case that
  /// we don't know the actual number of partitions until we create the
  /// partition function instance. The hive partition function spec then builds
  /// a bucket to partition map based on the actual number of partitions with
  /// round-robin partitioning scheme to create the function instance. For
  /// instance, when we create the local partition node with hive bucket
  /// function to support multiple table writer drivers, we don't know the the
  /// actual number of table writer drivers until start the task.
  HivePartitionFunctionSpec(
      int numBuckets,
      std::vector<column_index_t> channels,
      std::vector<VectorPtr> constValues)
      : HivePartitionFunctionSpec(
            numBuckets,
            {},
            std::move(channels),
            std::move(constValues)) {}

  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions,
      bool localExchange) const override;

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

/// Hook for connecting metadata functions to a HiveConnector. Each registered
/// factory is called after initializing a HiveConnector until one of these
/// returns a ConnectorMetadata instance.
class HiveConnectorMetadataFactory {
 public:
  virtual ~HiveConnectorMetadataFactory() = default;

  /// Returns a ConnectorMetadata to complete'hiveConnector' if 'this'
  /// recognizes a data source, e.g. local file system or remote metadata
  /// service associated to configs in 'hiveConnector'.
  virtual std::shared_ptr<ConnectorMetadata> create(
      HiveConnector* connector) = 0;
};

bool registerHiveConnectorMetadataFactory(
    std::unique_ptr<HiveConnectorMetadataFactory>);

} // namespace facebook::velox::connector::hive
