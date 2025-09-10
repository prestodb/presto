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

#include "velox/connectors/hive/HiveConnector.h"

#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/HiveDataSource.h"
#include "velox/connectors/hive/HivePartitionFunction.h"

#include <boost/lexical_cast.hpp>
#include <memory>

using namespace facebook::velox::exec;

namespace facebook::velox::connector::hive {

HiveConnector::HiveConnector(
    const std::string& id,
    std::shared_ptr<const config::ConfigBase> config,
    folly::Executor* ioExecutor)
    : Connector(id, std::move(config)),
      hiveConfig_(std::make_shared<HiveConfig>(connectorConfig())),
      fileHandleFactory_(
          hiveConfig_->isFileHandleCacheEnabled()
              ? std::make_unique<SimpleLRUCache<FileHandleKey, FileHandle>>(
                    hiveConfig_->numCacheFileHandles())
              : nullptr,
          std::make_unique<FileHandleGenerator>(hiveConfig_->config())),
      ioExecutor_(ioExecutor) {
  if (hiveConfig_->isFileHandleCacheEnabled()) {
    LOG(INFO) << "Hive connector " << connectorId()
              << " created with maximum of "
              << hiveConfig_->numCacheFileHandles()
              << " cached file handles with expiration of "
              << hiveConfig_->fileHandleExpirationDurationMs() << "ms.";
  } else {
    LOG(INFO) << "Hive connector " << connectorId()
              << " created with file handle cache disabled";
  }
}

std::unique_ptr<DataSource> HiveConnector::createDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const std::unordered_map<std::string, ColumnHandlePtr>& columnHandles,
    ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<HiveDataSource>(
      outputType,
      tableHandle,
      columnHandles,
      &fileHandleFactory_,
      ioExecutor_,
      connectorQueryCtx,
      hiveConfig_);
}

std::unique_ptr<DataSink> HiveConnector::createDataSink(
    RowTypePtr inputType,
    ConnectorInsertTableHandlePtr connectorInsertTableHandle,
    ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy) {
  auto hiveInsertHandle =
      std::dynamic_pointer_cast<const HiveInsertTableHandle>(
          connectorInsertTableHandle);
  VELOX_CHECK_NOT_NULL(
      hiveInsertHandle, "Hive connector expecting hive write handle!");
  return std::make_unique<HiveDataSink>(
      inputType,
      hiveInsertHandle,
      connectorQueryCtx,
      commitStrategy,
      hiveConfig_);
}

// static
void HiveConnector::registerSerDe() {
  HiveTableHandle::registerSerDe();
  HiveColumnHandle::registerSerDe();
  HiveConnectorSplit::registerSerDe();
  HiveInsertTableHandle::registerSerDe();
  HiveInsertFileNameGenerator::registerSerDe();
  LocationHandle::registerSerDe();
  HiveBucketProperty::registerSerDe();
  HiveSortingColumn::registerSerDe();
  HivePartitionFunctionSpec::registerSerDe();
}

std::unique_ptr<core::PartitionFunction> HivePartitionFunctionSpec::create(
    int numPartitions,
    bool localExchange) const {
  std::vector<int> bucketToPartitions;
  if (bucketToPartition_.empty()) {
    // NOTE: if hive partition function spec doesn't specify bucket to partition
    // mapping, then we do round-robin mapping based on the actual number of
    // partitions.
    bucketToPartitions.resize(numBuckets_);
    for (int bucket = 0; bucket < numBuckets_; ++bucket) {
      bucketToPartitions[bucket] = bucket % numPartitions;
    }
    if (localExchange) {
      // Shuffle the map from bucket to partition for local exchange so we don't
      // use the same map for remote shuffle.
      std::shuffle(
          bucketToPartitions.begin(),
          bucketToPartitions.end(),
          std::mt19937{0});
    }
  }
  return std::make_unique<HivePartitionFunction>(
      numBuckets_,
      bucketToPartition_.empty() ? std::move(bucketToPartitions)
                                 : bucketToPartition_,
      channels_,
      constValues_);
}

std::string HivePartitionFunctionSpec::toString() const {
  std::ostringstream keys;
  size_t constIndex = 0;
  for (auto i = 0; i < channels_.size(); ++i) {
    if (i > 0) {
      keys << ", ";
    }
    auto channel = channels_[i];
    if (channel == kConstantChannel) {
      keys << "\"" << constValues_[constIndex++]->toString(0) << "\"";
    } else {
      keys << channel;
    }
  }

  return fmt::format("HIVE(({}) buckets: {})", keys.str(), numBuckets_);
}

folly::dynamic HivePartitionFunctionSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HivePartitionFunctionSpec";
  obj["numBuckets"] = ISerializable::serialize(numBuckets_);
  obj["bucketToPartition"] = ISerializable::serialize(bucketToPartition_);
  obj["keys"] = ISerializable::serialize(channels_);
  std::vector<velox::core::ConstantTypedExpr> constValueExprs;
  constValueExprs.reserve(constValues_.size());
  for (const auto& value : constValues_) {
    constValueExprs.emplace_back(value);
  }
  obj["constants"] = ISerializable::serialize(constValueExprs);
  return obj;
}

// static
core::PartitionFunctionSpecPtr HivePartitionFunctionSpec::deserialize(
    const folly::dynamic& obj,
    void* context) {
  std::vector<column_index_t> channels =
      ISerializable::deserialize<std::vector<column_index_t>>(
          obj["keys"], context);
  const auto constTypedValues =
      ISerializable::deserialize<std::vector<velox::core::ConstantTypedExpr>>(
          obj["constants"], context);
  std::vector<VectorPtr> constValues;
  constValues.reserve(constTypedValues.size());
  auto* pool = static_cast<memory::MemoryPool*>(context);
  for (const auto& value : constTypedValues) {
    constValues.emplace_back(value->toConstantVector(pool));
  }
  return std::make_shared<HivePartitionFunctionSpec>(
      ISerializable::deserialize<int>(obj["numBuckets"], context),
      ISerializable::deserialize<std::vector<int>>(
          obj["bucketToPartition"], context),
      std::move(channels),
      std::move(constValues));
}

// static
void HivePartitionFunctionSpec::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register(
      "HivePartitionFunctionSpec", HivePartitionFunctionSpec::deserialize);
}

} // namespace facebook::velox::connector::hive
