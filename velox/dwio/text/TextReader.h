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

#include "velox/dwio/common/ReaderFactory.h"

namespace facebook::velox::text {

using dwio::common::ColumnStatistics;
using dwio::common::Mutation;
using dwio::common::Reader;
using dwio::common::RowReader;
using dwio::common::RowReaderOptions;
using dwio::common::RuntimeStatistics;
using dwio::common::TypeWithId;

using dwio::common::ReaderOptions;

class TextRowReader : public RowReader {
 public:
  explicit TextRowReader(
      std::unique_ptr<RowReader> rowReader,
      memory::MemoryPool& pool,
      const std::shared_ptr<common::ScanSpec>& scanSpec);

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  uint64_t next(uint64_t size, VectorPtr& result, const Mutation* mutation)
      override;

  void updateRuntimeStats(RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

 private:
  std::unique_ptr<RowReader> rowReader_;
  memory::MemoryPool& pool_;
  std::unique_ptr<BaseVector> batch_;
  std::shared_ptr<common::ScanSpec> scanSpec_;
};

class TextReader : public Reader {
 public:
  TextReader(
      const ReaderOptions& options,
      const std::shared_ptr<ReadFile>& readFile);

  std::optional<uint64_t> numberOfRows() const override;

  std::unique_ptr<ColumnStatistics> columnStatistics(
      uint32_t index) const override;

  const RowTypePtr& rowType() const override;
  const std::shared_ptr<const TypeWithId>& typeWithId() const override;

  std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& options) const override;

 private:
  ReaderOptions options_;
  std::shared_ptr<ReadFile> readFile_;
  mutable std::shared_ptr<const TypeWithId> typeWithId_;
};

class TextReaderFactory : public dwio::common::ReaderFactory {
 public:
  TextReaderFactory() : ReaderFactory(dwio::common::FileFormat::TEXT) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::BufferedInput>,
      const dwio::common::ReaderOptions&) override;
};

void registerTextReaderFactory();

void unregisterTextReaderFactory();

} // namespace facebook::velox::text
