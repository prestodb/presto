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

#include "velox/dwio/text/TextReader.h"

using namespace facebook::velox::dwio::common;

namespace facebook::velox::text {

using dwio::common::ReaderOptions; // clarify ambiguity

namespace {

class TextRowReader : public RowReader {
 public:
  /// TODO: Add implementation
  explicit TextRowReader(
      std::unique_ptr<RowReader> /*rowReader*/,
      memory::MemoryPool& pool,
      const std::shared_ptr<common::ScanSpec>& /*scanSpec*/)
      : pool_(pool) {}

  /// TODO: Add implementation
  int64_t nextRowNumber() override {
    return 0;
  }

  /// TODO: Add implementation
  int64_t nextReadSize(uint64_t /*size*/) override {
    return 0;
  }

  /// TODO: Add implementation
  uint64_t next(
      uint64_t /*size*/,
      VectorPtr& /*result*/,
      const Mutation* /*mutation*/) override {
    return 0;
  }

  void updateRuntimeStats(RuntimeStatistics& /*stats*/) const override {
    // No-op for non-selective reader.
    return;
  }

  void resetFilterCaches() override {
    // No-op for non-selective reader.
    return;
  }

  std::optional<size_t> estimatedRowSize() const override {
    return std::nullopt;
  }

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
      const std::shared_ptr<ReadFile>& readFile)
      : options_(options), readFile_(readFile) {
    return;
  }

  std::optional<uint64_t> numberOfRows() const override {
    return std::nullopt;
  }

  std::unique_ptr<ColumnStatistics> columnStatistics(
      uint32_t /*index*/) const override {
    return nullptr;
  }

  const RowTypePtr& rowType() const override {
    static RowTypePtr dummy;
    return dummy;
  }

  const std::shared_ptr<const TypeWithId>& typeWithId() const override {
    static std::shared_ptr<const TypeWithId> dummy;
    return dummy;
  }

  std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& /*options*/) const override {
    return nullptr;
  }

 private:
  ReaderOptions options_;
  std::shared_ptr<ReadFile> readFile_;
  mutable std::shared_ptr<const TypeWithId> typeWithId_;
};

} // namespace

std::unique_ptr<Reader> TextReaderFactory::createReader(
    std::unique_ptr<BufferedInput> input,
    const dwio::common::ReaderOptions& options) {
  return std::make_unique<TextReader>(options, input->getReadFile());
}

void registerTextReaderFactory() {
  registerReaderFactory(std::make_shared<TextReaderFactory>());
}

void unregisterTextReaderFactory() {
  unregisterReaderFactory(FileFormat::TEXT);
}

} // namespace facebook::velox::text
