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

#include <folly/Executor.h>
#include <memory>

#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/Reader.h"

namespace facebook::velox::connector::hive::iceberg {

struct IcebergDeleteFile;
struct IcebergMetadataColumn;

class PositionalDeleteFileReader {
 public:
  PositionalDeleteFileReader(
      const IcebergDeleteFile& deleteFile,
      const std::string& baseFilePath,
      FileHandleFactory* fileHandleFactory,
      const ConnectorQueryCtx* connectorQueryCtx,
      folly::Executor* executor,
      const std::shared_ptr<const HiveConfig>& hiveConfig,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const std::shared_ptr<filesystems::File::IoStats>& fsStats,
      dwio::common::RuntimeStatistics& runtimeStats,
      uint64_t splitOffset,
      const std::string& connectorId);

  void readDeletePositions(
      uint64_t baseReadOffset,
      uint64_t size,
      BufferPtr deleteBitmap);

  bool noMoreData();

 private:
  void updateDeleteBitmap(
      VectorPtr deletePositionsVector,
      uint64_t baseReadOffset,
      int64_t rowNumberUpperBound,
      BufferPtr deleteBitmapBuffer);

  bool readFinishedForBatch(int64_t rowNumberUpperBound);

  const IcebergDeleteFile& deleteFile_;
  const std::string& baseFilePath_;
  FileHandleFactory* const fileHandleFactory_;
  folly::Executor* const executor_;
  const ConnectorQueryCtx* connectorQueryCtx_;
  const std::shared_ptr<const HiveConfig> hiveConfig_;
  const std::shared_ptr<io::IoStatistics> ioStats_;
  const std::shared_ptr<filesystems::File::IoStats> fsStats_;
  const std::shared_ptr<filesystems::File::IoStats> fsStats;
  memory::MemoryPool* const pool_;

  std::shared_ptr<IcebergMetadataColumn> filePathColumn_;
  std::shared_ptr<IcebergMetadataColumn> posColumn_;
  uint64_t splitOffset_;

  std::shared_ptr<HiveConnectorSplit> deleteSplit_;
  std::unique_ptr<dwio::common::RowReader> deleteRowReader_;
  // The vector to hold the delete positions read from the positional delete
  // file. These positions are relative to the start of the whole base data
  // file.
  VectorPtr deletePositionsOutput_;
  // The index of deletePositionsOutput_ that indicates up to where the delete
  // positions have been converted into the bitmap
  uint64_t deletePositionsOffset_;
  // Total number of rows read from this positional delete file reader,
  // including the rows filtered out from filters on both filePathColumn_ and
  // posColumn_.
  uint64_t totalNumRowsScanned_;
};

} // namespace facebook::velox::connector::hive::iceberg
