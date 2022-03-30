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

#include "velox/common/base/Macros.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/parquet/reader/duckdb/Allocator.h"
#include "velox/dwio/parquet/reader/duckdb/InputStreamFileSystem.h"
VELOX_SUPPRESS_DEPRECATION_WARNING
#include "velox/external/duckdb/parquet-amalgamation.hpp"
VELOX_UNSUPPRESS_DEPRECATION_WARNING

namespace facebook::velox::parquet {

class ParquetRowReader : public dwio::common::RowReader {
 public:
  ParquetRowReader(
      std::shared_ptr<::duckdb::ParquetReader> reader,
      const dwio::common::RowReaderOptions& options,
      memory::MemoryPool& pool);
  ~ParquetRowReader() override = default;

  uint64_t next(uint64_t size, velox::VectorPtr& result) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

 private:
  ::duckdb::TableFilterSet filters_;
  std::shared_ptr<::duckdb::ParquetReader> reader_;
  ::duckdb::ParquetReaderScanState state_;
  memory::MemoryPool& pool_;
  RowTypePtr rowType_;
  std::vector<::duckdb::LogicalType> duckdbRowType_;
};

class ParquetReader : public dwio::common::Reader {
 public:
  ParquetReader(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options);
  ~ParquetReader() override = default;

  std::optional<uint64_t> numberOfRows() const override;

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override;

  const velox::RowTypePtr& rowType() const override;

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override;

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;

 private:
  duckdb::VeloxPoolAllocator allocator_;
  std::unique_ptr<duckdb::InputStreamFileSystem> fileSystem_;
  std::shared_ptr<::duckdb::ParquetReader> reader_;
  memory::MemoryPool& pool_;

  RowTypePtr type_;
  mutable std::shared_ptr<const dwio::common::TypeWithId> typeWithId_;
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
