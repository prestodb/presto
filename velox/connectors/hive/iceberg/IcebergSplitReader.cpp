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

#include "velox/connectors/hive/iceberg/IcebergSplitReader.h"

#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/common/Mutation.h"
#include "velox/dwio/common/Reader.h"

using namespace facebook::velox::dwio::common;

namespace facebook::velox::connector::hive::iceberg {

IcebergSplitReader::IcebergSplitReader(
    std::shared_ptr<velox::connector::hive::HiveConnectorSplit> hiveSplit,
    std::shared_ptr<HiveTableHandle> hiveTableHandle,
    std::shared_ptr<common::ScanSpec> scanSpec,
    const RowTypePtr readerOutputType,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig> hiveConfig,
    std::shared_ptr<io::IoStatistics> ioStats)
    : SplitReader(
          hiveSplit,
          hiveTableHandle,
          scanSpec,
          readerOutputType,
          partitionKeys,
          fileHandleFactory,
          executor,
          connectorQueryCtx,
          hiveConfig,
          ioStats) {}

void IcebergSplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats) {
  SplitReader::prepareSplit(metadataFilter, runtimeStats);
  baseReadOffset_ = 0;
  positionalDeleteFileReaders_.clear();
  splitOffset_ = baseRowReader_->nextRowNumber();

  // TODO: Deserialize the std::vector<IcebergDeleteFile> deleteFiles. For now
  // we assume it's already deserialized.
  std::shared_ptr<HiveIcebergSplit> icebergSplit =
      std::dynamic_pointer_cast<HiveIcebergSplit>(hiveSplit_);

  const auto& deleteFiles = icebergSplit->deleteFiles;
  for (const auto& deleteFile : deleteFiles) {
    positionalDeleteFileReaders_.push_back(
        std::make_unique<PositionalDeleteFileReader>(
            deleteFile,
            hiveSplit_->filePath,
            fileHandleFactory_,
            connectorQueryCtx_,
            executor_,
            hiveConfig_,
            ioStats_,
            runtimeStats,
            splitOffset_,
            hiveSplit_->connectorId));
  }
}

uint64_t IcebergSplitReader::next(int64_t size, VectorPtr& output) {
  Mutation mutation;
  mutation.deletedRows = nullptr;

  if (!positionalDeleteFileReaders_.empty()) {
    auto numBytes = bits::nbytes(size);
    dwio::common::ensureCapacity<int8_t>(
        deleteBitmap_, numBytes, connectorQueryCtx_->memoryPool());
    std::memset((void*)deleteBitmap_->as<int8_t>(), 0L, numBytes);

    for (auto iter = positionalDeleteFileReaders_.begin();
         iter != positionalDeleteFileReaders_.end();
         iter++) {
      (*iter)->readDeletePositions(
          baseReadOffset_, size, deleteBitmap_->asMutable<int8_t>());
      if ((*iter)->endOfFile()) {
        iter = positionalDeleteFileReaders_.erase(iter);
      }
    }

    deleteBitmap_->setSize(numBytes);
    mutation.deletedRows = deleteBitmap_->as<uint64_t>();
  }

  auto rowsScanned = baseRowReader_->next(size, output, &mutation);
  baseReadOffset_ += rowsScanned;

  return rowsScanned;
}

} // namespace facebook::velox::connector::hive::iceberg
