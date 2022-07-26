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
#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"
#include "velox/dwio/parquet/thrift/ParquetThriftTypes.h"

namespace facebook::velox::parquet {

using TypePtr = std::shared_ptr<const velox::Type>;

constexpr uint64_t DIRECTORY_SIZE_GUESS = 1024 * 1024;
constexpr uint64_t FILE_PRELOAD_THRESHOLD = 1024 * 1024 * 8;

class ReaderBase {
 public:
  ReaderBase(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options);

  virtual ~ReaderBase() = default;

  dwio::common::BufferedInput& getBufferedInput() const;
  memory::MemoryPool& getMemoryPool() const;

  const dwio::common::InputStream& getStream() const;
  uint64_t getFileLength() const;
  uint64_t getFileNumRows() const;
  const thrift::FileMetaData& getFileMetaData() const;
  const std::shared_ptr<const RowType>& getSchema() const;
  const std::shared_ptr<const dwio::common::TypeWithId>& getSchemaWithId()
      const;

 private:
  void loadFileMetaData();
  void initializeSchema();
  std::shared_ptr<const ParquetTypeWithId> getParquetColumnInfo(
      uint32_t maxSchemaElementIdx,
      uint32_t maxRepeat,
      uint32_t maxDefine,
      uint32_t& schemaIdx,
      uint32_t& columnIdx) const;
  std::shared_ptr<const RowType> createRowType(
      std::vector<std::shared_ptr<const ParquetTypeWithId::TypeWithId>>
          children) const;
  TypePtr convertType(const thrift::SchemaElement& schemaElement) const;

  const bool binaryAsString = false;
  const dwio::common::ReaderOptions& options_;
  const std::unique_ptr<dwio::common::InputStream> stream_;
  const std::shared_ptr<dwio::common::BufferedInputFactory>
      bufferedInputFactory_ =
          dwio::common::BufferedInputFactory::baseFactoryShared();

  std::shared_ptr<dwio::common::BufferedInput> input_;
  memory::MemoryPool& pool_;

  uint64_t fileLength_;
  std::unique_ptr<thrift::FileMetaData> fileMetaData_;
  RowTypePtr schema_;
  std::shared_ptr<const dwio::common::TypeWithId> schemaWithId_;
};

class ParquetReader : public dwio::common::Reader {
 public:
  ParquetReader(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options);
  ~ParquetReader() override = default;

  std::optional<uint64_t> numberOfRows() const override;

  // TODO: Merge the stats of all RowGroups into the stats for the whole column.
  // Unlike DWRF and ORC, Parquet column Statistics is per row group.
  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override {
    VELOX_NYI(
        "columnStatistics for native Parquet reader is not implemented yet");
  }

  const velox::RowTypePtr& rowType() const override;
  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override;

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override {
    VELOX_NYI("ParquetRowReader will be merged later");
  }

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

} // namespace facebook::velox::parquet
