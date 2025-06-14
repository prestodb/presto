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
#include "velox/dwio/text/reader/TextReaderImpl.h"

namespace facebook::velox::dwio::common {

using memory::MemoryPool;
using velox::common::ScanSpec;

class TextRowReader : public RowReader {
 public:
  explicit TextRowReader(
      std::unique_ptr<RowReader> rowReader,
      MemoryPool& pool,
      const std::shared_ptr<ScanSpec>& scanSpec);

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  uint64_t next(uint64_t size, VectorPtr& result, const Mutation* mutation)
      override;

  void updateRuntimeStats(RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

 private:
  MemoryPool& pool_;
  std::unique_ptr<RowReader>
      rowReader_; // creation of TextRowReader dictates rowReader_ to be type of
                  // TextRowReaderImpl but no dynamic cast to avoid runtime
                  // performance penalty, no casting will be needed after
                  // refactoring redundant abstraction layer
  std::unique_ptr<BaseVector> vector_;
  std::shared_ptr<ScanSpec> scanSpec_;
};

class TextReader : public Reader {
 public:
  TextReader(
      const ReaderOptions& options,
      std::unique_ptr<BufferedInput> input);

  std::optional<uint64_t> numberOfRows() const override;

  std::unique_ptr<ColumnStatistics> columnStatistics(
      uint32_t index) const override;

  const RowTypePtr& rowType() const override;

  const std::shared_ptr<const TypeWithId>& typeWithId() const override;

  std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& options) const override;

 private:
  ReaderOptions options_;
  std::unique_ptr<TextReaderImpl> reader_;

  mutable std::shared_ptr<const TypeWithId> typeWithId_;
};

class TextReaderFactory : public ReaderFactory {
 public:
  TextReaderFactory() : ReaderFactory(FileFormat::TEXT) {}

  std::unique_ptr<Reader> createReader(
      std::unique_ptr<BufferedInput>,
      const ReaderOptions&) override;
};

void registerTextReaderFactory();

void unregisterTextReaderFactory();

} // namespace facebook::velox::dwio::common
