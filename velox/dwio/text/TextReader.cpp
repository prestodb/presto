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

namespace facebook::velox::dwio::common {

TextRowReader::TextRowReader(
    std::unique_ptr<RowReader> rowReader,
    memory::MemoryPool& pool,
    const std::shared_ptr<ScanSpec>& scanSpec)
    : pool_(pool), rowReader_(std::move(rowReader)), scanSpec_(scanSpec) {}

int64_t TextRowReader::nextRowNumber() {
  return rowReader_->nextRowNumber();
}

int64_t TextRowReader::nextReadSize(uint64_t size) {
  return rowReader_->nextReadSize(size);
}

uint64_t TextRowReader::next(
    uint64_t size,
    VectorPtr& result,
    const Mutation* mutation) {
  return rowReader_->next(size, result, mutation);
}

void TextRowReader::updateRuntimeStats(RuntimeStatistics& stats) const {
  rowReader_->updateRuntimeStats(stats);
}

void TextRowReader::resetFilterCaches() {
  rowReader_->resetFilterCaches();
}

std::optional<size_t> TextRowReader::estimatedRowSize() const {
  return rowReader_->estimatedRowSize();
}

TextReader::TextReader(
    const ReaderOptions& options,
    std::unique_ptr<BufferedInput> input)
    : options_(options),
      reader_(std::make_unique<TextReaderImpl>(std::move(input), options_)) {
  VELOX_USER_CHECK_NOT_NULL(
      options_.fileSchema(), "File schema for TEXT must be set.");
}

std::optional<uint64_t> TextReader::numberOfRows() const {
  return reader_->numberOfRows();
}

std::unique_ptr<ColumnStatistics> TextReader::columnStatistics(
    uint32_t index) const {
  return reader_->columnStatistics(index);
}

const RowTypePtr& TextReader::rowType() const {
  return reader_->rowType();
}

const std::shared_ptr<const TypeWithId>& TextReader::typeWithId() const {
  if (!typeWithId_) {
    typeWithId_ = TypeWithId::create(rowType());
  }
  return typeWithId_;
}

std::unique_ptr<RowReader> TextReader::createRowReader(
    const RowReaderOptions& options) const {
  auto rowReader = reader_->createRowReader(options);
  return std::make_unique<TextRowReader>(
      std::move(rowReader), options_.memoryPool(), options.scanSpec());
}

std::unique_ptr<Reader> TextReaderFactory::createReader(
    std::unique_ptr<BufferedInput> input,
    const ReaderOptions& options) {
  return std::make_unique<TextReader>(options, std::move(input));
}

void registerTextReaderFactory() {
  registerReaderFactory(std::make_shared<TextReaderFactory>());
}

void unregisterTextReaderFactory() {
  unregisterReaderFactory(FileFormat::TEXT);
}

} // namespace facebook::velox::dwio::common
