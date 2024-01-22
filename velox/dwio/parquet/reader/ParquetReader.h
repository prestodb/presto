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

#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"

namespace facebook::velox::dwio::common {

class SelectiveColumnReader;
class BufferedInput;

} // namespace facebook::velox::dwio::common

namespace facebook::velox::parquet {

enum class ParquetMetricsType { HEADER, FILE_METADATA, FILE, BLOCK, TEST };

class StructColumnReader;

class ReaderBase;

/// Implements the RowReader interface for Parquet.
class ParquetRowReader : public dwio::common::RowReader {
 public:
  ParquetRowReader(
      const std::shared_ptr<ReaderBase>& readerBase,
      const dwio::common::RowReaderOptions& options);
  ~ParquetRowReader() override = default;

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  uint64_t next(
      uint64_t size,
      velox::VectorPtr& result,
      const dwio::common::Mutation* = nullptr) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

  const dwio::common::RowReaderOptions& getOptions() {
    return options_;
  }

  bool allPrefetchIssued() const override {
    //  Allow opening the next split while this is reading.
    return true;
  }

  // Checks if the specific row group is buffered.
  // Returns false if the row group is not loaded into buffer
  // or the buffered data has been evicted.
  bool isRowGroupBuffered(int32_t rowGroupIndex) const;

 private:
  // Compares row group  metadata to filters in ScanSpec in options of
  // ReaderBase and determines the set of row groups to scan.
  void filterRowGroups();

  // Positions the reader tre at the start of the next row group, as determined
  // by filterRowGroups().
  bool advanceToNextRowGroup();

  memory::MemoryPool& pool_;
  const std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::RowReaderOptions options_;

  // All row groups from file metadata.
  const std::vector<thrift::RowGroup>& rowGroups_;

  // Indices of row groups where stats match filters.
  std::vector<uint32_t> rowGroupIds_;
  std::vector<uint64_t> firstRowOfRowGroup_;
  uint32_t nextRowGroupIdsIdx_;
  const thrift::RowGroup* FOLLY_NULLABLE currentRowGroupPtr_{nullptr};
  uint64_t rowsInCurrentRowGroup_;
  uint64_t currentRowInGroup_;

  std::unique_ptr<dwio::common::SelectiveColumnReader> columnReader_;

  RowTypePtr requestedType_;

  dwio::common::ColumnReaderStatistics columnReaderStats_;
};

/// Implements the reader interface for Parquet.
class ParquetReader : public dwio::common::Reader {
 public:
  ParquetReader(
      std::unique_ptr<dwio::common::BufferedInput>,
      const dwio::common::ReaderOptions& options);

  ~ParquetReader() override = default;

  std::optional<uint64_t> numberOfRows() const override;

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override {
    return nullptr;
  }

  const velox::RowTypePtr& rowType() const override;

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override;

  size_t numberOfRowGroups() const;

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;

 private:
  std::shared_ptr<ReaderBase> readerBase_;
};

class ParquetReaderFactory : public dwio::common::ReaderFactory {
 public:
  ParquetReaderFactory() : ReaderFactory(dwio::common::FileFormat::PARQUET) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options) override {
    return std::make_unique<ParquetReader>(std::move(input), options);
  }
};

void registerParquetReaderFactory();

void unregisterParquetReaderFactory();

} // namespace facebook::velox::parquet
