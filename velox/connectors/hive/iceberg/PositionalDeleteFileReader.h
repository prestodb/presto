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

using SubfieldFilters =
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>;

class PositionalDeleteFileReader {
 public:
  PositionalDeleteFileReader(
      const IcebergDeleteFile& deleteFile,
      const std::string& baseFilePath,
      FileHandleFactory* fileHandleFactory,
      const ConnectorQueryCtx* connectorQueryCtx,
      folly::Executor* executor,
      const std::shared_ptr<HiveConfig> hiveConfig,
      std::shared_ptr<io::IoStatistics> ioStats,
      dwio::common::RuntimeStatistics& runtimeStats,
      uint64_t splitOffset,
      const std::string& connectorId);

  void readDeletePositions(
      uint64_t baseReadOffset,
      uint64_t size,
      int8_t* deleteBitmap);

  bool endOfFile();

 private:
  void updateDeleteBitmap(
      VectorPtr deletePositionsVector,
      uint64_t baseReadOffset,
      int64_t rowNumberUpperBound,
      int8_t* deleteBitmap);

  bool readFinishedForBatch(int64_t rowNumberUpperBound);

  const IcebergDeleteFile& deleteFile_;
  const std::string& baseFilePath_;
  FileHandleFactory* const fileHandleFactory_;
  folly::Executor* const executor_;
  const ConnectorQueryCtx* const connectorQueryCtx_;
  const std::shared_ptr<HiveConfig> hiveConfig_;
  std::shared_ptr<io::IoStatistics> ioStats_;
  memory::MemoryPool* const pool_;

  std::shared_ptr<IcebergMetadataColumn> filePathColumn_;
  std::shared_ptr<IcebergMetadataColumn> posColumn_;
  uint64_t splitOffset_;

  std::shared_ptr<HiveConnectorSplit> deleteSplit_;
  std::unique_ptr<dwio::common::RowReader> deleteRowReader_;
  VectorPtr deletePositionsOutput_;
  uint64_t deletePositionsOffset_;
  bool endOfFile_;
};

} // namespace facebook::velox::connector::hive::iceberg
