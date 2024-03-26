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

#include "velox/common/base/Fs.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/HiveDataSource.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
// Meta's buck build system needs this check.
#ifdef VELOX_ENABLE_GCS
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGCSFileSystem.h" // @manual
#endif
#ifdef VELOX_ENABLE_HDFS3
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h" // @manual
#endif
#ifdef VELOX_ENABLE_S3
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h" // @manual
#endif
#ifdef VELOX_ENABLE_ABFS
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h" // @manual
#endif
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
// Meta's buck build system needs this check.
#ifdef VELOX_ENABLE_PARQUET
#include "velox/dwio/parquet/RegisterParquetReader.h" // @manual
#include "velox/dwio/parquet/RegisterParquetWriter.h" // @manual
#endif
#include "velox/expression/FieldReference.h"

#include <boost/lexical_cast.hpp>
#include <memory>

using namespace facebook::velox::exec;
using namespace facebook::velox::dwrf;

namespace facebook::velox::connector::hive {

HiveConnector::HiveConnector(
    const std::string& id,
    std::shared_ptr<const Config> config,
    folly::Executor* executor)
    : Connector(id),
      hiveConfig_(std::make_shared<HiveConfig>(config)),
      fileHandleFactory_(
          hiveConfig_->isFileHandleCacheEnabled()
              ? std::make_unique<
                    SimpleLRUCache<std::string, std::shared_ptr<FileHandle>>>(
                    hiveConfig_->numCacheFileHandles())
              : nullptr,
          std::make_unique<FileHandleGenerator>(config)),
      executor_(executor) {
  if (hiveConfig_->isFileHandleCacheEnabled()) {
    LOG(INFO) << "Hive connector " << connectorId()
              << " created with maximum of "
              << hiveConfig_->numCacheFileHandles() << " cached file handles.";
  } else {
    LOG(INFO) << "Hive connector " << connectorId()
              << " created with file handle cache disabled";
  }
}

std::unique_ptr<DataSource> HiveConnector::createDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<HiveDataSource>(
      outputType,
      tableHandle,
      columnHandles,
      &fileHandleFactory_,
      executor_,
      connectorQueryCtx,
      hiveConfig_);
}

std::unique_ptr<DataSink> HiveConnector::createDataSink(
    RowTypePtr inputType,
    std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
    ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy) {
  auto hiveInsertHandle = std::dynamic_pointer_cast<HiveInsertTableHandle>(
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

std::unique_ptr<core::PartitionFunction> HivePartitionFunctionSpec::create(
    int numPartitions) const {
  std::vector<int> bucketToPartitions;
  if (bucketToPartition_.empty()) {
    // NOTE: if hive partition function spec doesn't specify bucket to partition
    // mapping, then we do round-robin mapping based on the actual number of
    // partitions.
    bucketToPartitions.resize(numBuckets_);
    for (int bucket = 0; bucket < numBuckets_; ++bucket) {
      bucketToPartitions[bucket] = bucket % numPartitions;
    }
  }
  return std::make_unique<velox::connector::hive::HivePartitionFunction>(
      numBuckets_,
      bucketToPartition_.empty() ? std::move(bucketToPartitions)
                                 : bucketToPartition_,
      channels_,
      constValues_);
}

void HiveConnectorFactory::initialize() {
  [[maybe_unused]] static bool once = []() {
    dwio::common::registerFileSinks();
    dwrf::registerDwrfReaderFactory();
    dwrf::registerDwrfWriterFactory();
// Meta's buck build system needs this check.
#ifdef VELOX_ENABLE_PARQUET
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();
#endif
// Meta's buck build system needs this check.
#ifdef VELOX_ENABLE_S3
    filesystems::registerS3FileSystem();
#endif
#ifdef VELOX_ENABLE_HDFS3
    filesystems::registerHdfsFileSystem();
#endif
#ifdef VELOX_ENABLE_GCS
    filesystems::registerGCSFileSystem();
#endif
#ifdef VELOX_ENABLE_ABFS
    filesystems::abfs::registerAbfsFileSystem();
#endif
    return true;
  }();
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

void registerHivePartitionFunctionSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register(
      "HivePartitionFunctionSpec", HivePartitionFunctionSpec::deserialize);
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<HiveConnectorFactory>())
VELOX_REGISTER_CONNECTOR_FACTORY(
    std::make_shared<HiveHadoop2ConnectorFactory>())
} // namespace facebook::velox::connector::hive
