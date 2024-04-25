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

#include "velox/common/base/RandomUtil.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/dwio/common/Options.h"

namespace facebook::velox {
class BaseVector;
class variant;
using VectorPtr = std::shared_ptr<BaseVector>;
} // namespace facebook::velox

namespace facebook::velox::common {
class MetadataFilter;
class ScanSpec;
} // namespace facebook::velox::common

namespace facebook::velox::connector {
class ConnectorQueryCtx;
} // namespace facebook::velox::connector

namespace facebook::velox::dwio::common {
class Reader;
class RowReader;
struct RuntimeStatistics;
} // namespace facebook::velox::dwio::common

namespace facebook::velox::memory {
class MemoryPool;
}

namespace facebook::velox::connector::hive {

struct HiveConnectorSplit;
class HiveTableHandle;
class HiveColumnHandle;
class HiveConfig;

class SplitReader {
 public:
  static std::unique_ptr<SplitReader> create(
      const std::shared_ptr<hive::HiveConnectorSplit>& hiveSplit,
      const std::shared_ptr<const HiveTableHandle>& hiveTableHandle,
      const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
          partitionKeys,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<const HiveConfig>& hiveConfig,
      const RowTypePtr& readerOutputType,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const std::shared_ptr<common::ScanSpec>& scanSpec);

  SplitReader(
      const std::shared_ptr<const hive::HiveConnectorSplit>& hiveSplit,
      const std::shared_ptr<const HiveTableHandle>& hiveTableHandle,
      const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
          partitionKeys,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<const HiveConfig>& hiveConfig,
      const RowTypePtr& readerOutputType,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const std::shared_ptr<common::ScanSpec>& scanSpec);

  virtual ~SplitReader() = default;

  void configureReaderOptions(
      std::shared_ptr<random::RandomSkipTracker> randomSkip);

  /// This function is used by different table formats like Iceberg and Hudi to
  /// do additional preparations before reading the split, e.g. Open delete
  /// files or log files, and add column adapatations for metadata columns. It
  /// would be called only once per incoming split
  virtual void prepareSplit(
      std::shared_ptr<common::MetadataFilter> metadataFilter,
      dwio::common::RuntimeStatistics& runtimeStats);

  virtual uint64_t next(uint64_t size, VectorPtr& output);

  void resetFilterCaches();

  bool emptySplit() const;

  void resetSplit();

  int64_t estimatedRowSize() const;

  void updateRuntimeStats(dwio::common::RuntimeStatistics& stats) const;

  bool allPrefetchIssued() const;

  void setConnectorQueryCtx(const ConnectorQueryCtx* connectorQueryCtx);

  std::string toString() const;

 protected:
  /// Create the dwio::common::Reader object baseReader_, which will be used to
  /// read the data file's metadata and schema
  void createReader();

  /// Check if the hiveSplit_ is empty. The split is considered empty when
  ///   1) The data file is missing but the user chooses to ignore it
  ///   2) The file does not contain any rows
  ///   3) The data in the file does not pass the filters. The test is based on
  ///      the file metadata and partition key values
  /// This function needs to be called after baseReader_ is created.
  bool checkIfSplitIsEmpty(dwio::common::RuntimeStatistics& runtimeStats);

  /// Create the dwio::common::RowReader object baseRowReader_, which owns the
  /// ColumnReaders that will be used to read the data
  void createRowReader(std::shared_ptr<common::MetadataFilter> metadataFilter);

  /// Different table formats may have different meatadata columns.
  /// This function will be used to update the scanSpec for these columns.
  virtual std::vector<TypePtr> adaptColumns(
      const RowTypePtr& fileType,
      const std::shared_ptr<const velox::RowType>& tableSchema);

  void setPartitionValue(
      common::ScanSpec* spec,
      const std::string& partitionKey,
      const std::optional<std::string>& value) const;

  std::shared_ptr<const HiveConnectorSplit> hiveSplit_;
  const std::shared_ptr<const HiveTableHandle> hiveTableHandle_;
  const std::unordered_map<
      std::string,
      std::shared_ptr<HiveColumnHandle>>* const partitionKeys_;
  const ConnectorQueryCtx* connectorQueryCtx_;
  const std::shared_ptr<const HiveConfig> hiveConfig_;

  const RowTypePtr readerOutputType_;
  const std::shared_ptr<io::IoStatistics> ioStats_;
  FileHandleFactory* const fileHandleFactory_;
  folly::Executor* const executor_;
  memory::MemoryPool* const pool_;

  std::shared_ptr<common::ScanSpec> scanSpec_;
  std::unique_ptr<dwio::common::Reader> baseReader_;
  std::unique_ptr<dwio::common::RowReader> baseRowReader_;
  dwio::common::ReaderOptions baseReaderOpts_;
  dwio::common::RowReaderOptions baseRowReaderOpts_;
  bool emptySplit_;
};

} // namespace facebook::velox::connector::hive
