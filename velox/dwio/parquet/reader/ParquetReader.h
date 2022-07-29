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

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"
#include "velox/dwio/parquet/thrift/ParquetThriftTypes.h"

namespace facebook::velox::parquet {

constexpr uint64_t DIRECTORY_SIZE_GUESS = 1024 * 1024;
constexpr uint64_t FILE_PRELOAD_THRESHOLD = 1024 * 1024 * 8;

enum class ParquetMetricsType { HEADER, FILE_METADATA, FILE, BLOCK, TEST };

class StructColumnReader;

/// Metadata and options for reading Parquet.
class ReaderBase {
 public:
  ReaderBase(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options);

  virtual ~ReaderBase() = default;

  memory::MemoryPool& getMemoryPool() const {
    return pool_;
  }

  dwio::common::BufferedInput& bufferedInput() const {
    return *input_;
  }

  dwio::common::InputStream& stream() const {
    return *stream_;
  }

  uint64_t fileLength() const {
    return fileLength_;
  }

  uint64_t fileNumRows() const {
    return fileMetaData_->num_rows;
  }

  const thrift::FileMetaData& fileMetaData() const {
    return *fileMetaData_;
  }

  const std::shared_ptr<const RowType>& schema() const {
    return schema_;
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& schemaWithId() {
    return schemaWithId_;
  }

  /// Ensures that streams are enqueued and loading for the row group at
  /// 'currentGroup'. May start loading one or more subsequent groups.
  void scheduleRowGroups(
      const std::vector<uint32_t>& groups,
      int32_t currentGroup,
      StructColumnReader& reader);

  /// Returns the uncompressed size for columns in 'type' and its children in
  /// row
  /// group.
  int64_t rowGroupUncompressedSize(
      int32_t rowGroupIndex,
      const dwio::common::TypeWithId& type) const;

 private:
  // Reads and parses file footer.
  void loadFileMetaData();

  void initializeSchema();

  std::shared_ptr<const ParquetTypeWithId> getParquetColumnInfo(
      uint32_t maxSchemaElementIdx,
      uint32_t maxRepeat,
      uint32_t maxDefine,
      uint32_t& schemaIdx,
      uint32_t& columnIdx) const;

  TypePtr convertType(const thrift::SchemaElement& schemaElement) const;

  static std::shared_ptr<const RowType> createRowType(
      std::vector<std::shared_ptr<const ParquetTypeWithId::TypeWithId>>
          children);

  memory::MemoryPool& pool_;
  const dwio::common::ReaderOptions& options_;
  const std::unique_ptr<dwio::common::InputStream> stream_;
  std::shared_ptr<dwio::common::BufferedInputFactory> bufferedInputFactory_;
  std::shared_ptr<velox::dwio::common::BufferedInput> input_;
  uint64_t fileLength_;
  std::unique_ptr<thrift::FileMetaData> fileMetaData_;
  RowTypePtr schema_;
  std::shared_ptr<const dwio::common::TypeWithId> schemaWithId_;

  const bool binaryAsString = false;

  // Map from row group index to pre-created loading BufferedInput.
  std::unordered_map<uint32_t, std::unique_ptr<dwio::common::BufferedInput>>
      inputs_;
};

/// Implements the RowReader interface for Parquet.
class ParquetRowReader : public dwio::common::RowReader {
 public:
  ParquetRowReader(
      const std::shared_ptr<ReaderBase>& readerBase,
      const dwio::common::RowReaderOptions& options);
  ~ParquetRowReader() override = default;

  uint64_t next(uint64_t size, velox::VectorPtr& result) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

  const dwio::common::RowReaderOptions& getOptions() {
    return options_;
  }

 private:
  // Compares row group  metadata to filters in ScanSpec in options of
  // ReaderBase and determines the set of row groups to scan.
  void filterRowGroups();

  // Positions the reader tre at the start of the next row group, as determined
  // by filterRowGroups().
  bool advanceToNextRowGroup();

  memory::MemoryPool& pool_;
  const std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::RowReaderOptions& options_;

  // All row groups from file metadata.
  const std::vector<thrift::RowGroup>& rowGroups_;

  // Indices of row groups where stats match filters.
  std::vector<uint32_t> rowGroupIds_;
  uint32_t currentRowGroupIdsIdx_;
  const thrift::RowGroup* FOLLY_NULLABLE currentRowGroupPtr_{nullptr};
  uint64_t rowsInCurrentRowGroup_;
  uint64_t currentRowInGroup_;

  // Number of row groups skipped based on stats.
  int32_t skippedRowGroups_{0};

  std::unique_ptr<dwio::common::SelectiveColumnReader> columnReader_;

  RowTypePtr requestedType_;
};

/// Implements the reader interface for Parquet.
class ParquetReader : public dwio::common::Reader {
 public:
  ParquetReader(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options);

  ~ParquetReader() override = default;

  std::optional<uint64_t> numberOfRows() const override {
    return readerBase_->fileNumRows();
  }

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override {
    return nullptr;
  }

  const velox::RowTypePtr& rowType() const override {
    return readerBase_->schema();
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override {
    return readerBase_->schemaWithId();
  }

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;

 private:
  std::shared_ptr<ReaderBase> readerBase_;
};

class ParquetReaderFactory : public dwio::common::ReaderFactory {
 public:
  ParquetReaderFactory() : ReaderFactory(dwio::common::FileFormat::PARQUET) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options) override {
    return std::make_unique<ParquetReader>(std::move(stream), options);
  }
};

void registerParquetReaderFactory();

void unregisterParquetReaderFactory();

} // namespace facebook::velox::parquet
