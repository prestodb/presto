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

using namespace facebook::velox::dwio::common;

namespace facebook::velox::connector::hive::iceberg {

IcebergSplitReader::IcebergSplitReader(
    const std::shared_ptr<const hive::HiveConnectorSplit>& hiveSplit,
    const std::shared_ptr<const HiveTableHandle>& hiveTableHandle,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    FileHandleFactory* const fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<common::ScanSpec>& scanSpec)
    : SplitReader(
          hiveSplit,
          hiveTableHandle,
          partitionKeys,
          connectorQueryCtx,
          hiveConfig,
          readerOutputType,
          ioStats,
          fileHandleFactory,
          executor,
          scanSpec),
      baseReadOffset_(0),
      splitOffset_(0) {}

void IcebergSplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats) {
  createReader();

  if (checkIfSplitIsEmpty(runtimeStats)) {
    VELOX_CHECK(emptySplit_);
    return;
  }

  createRowReader(metadataFilter);

  std::shared_ptr<const HiveIcebergSplit> icebergSplit =
      std::dynamic_pointer_cast<const HiveIcebergSplit>(hiveSplit_);
  baseReadOffset_ = 0;
  splitOffset_ = baseRowReader_->nextRowNumber();
  positionalDeleteFileReaders_.clear();

  const auto& deleteFiles = icebergSplit->deleteFiles;
  for (const auto& deleteFile : deleteFiles) {
    if (deleteFile.content == FileContent::kPositionalDeletes) {
      if (deleteFile.recordCount > 0) {
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
    } else {
      VELOX_NYI();
    }
  }
}

uint64_t IcebergSplitReader::next(uint64_t size, VectorPtr& output) {
  Mutation mutation;
  mutation.randomSkip = baseReaderOpts_.randomSkip().get();
  mutation.deletedRows = nullptr;

  if (!positionalDeleteFileReaders_.empty()) {
    auto numBytes = bits::nbytes(size);
    dwio::common::ensureCapacity<int8_t>(
        deleteBitmap_, numBytes, connectorQueryCtx_->memoryPool());
    std::memset((void*)deleteBitmap_->as<int8_t>(), 0L, numBytes);

    for (auto iter = positionalDeleteFileReaders_.begin();
         iter != positionalDeleteFileReaders_.end();) {
      (*iter)->readDeletePositions(
          baseReadOffset_, size, deleteBitmap_->asMutable<int8_t>());
      if ((*iter)->endOfFile()) {
        iter = positionalDeleteFileReaders_.erase(iter);
      } else {
        ++iter;
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
