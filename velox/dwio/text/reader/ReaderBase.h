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

namespace facebook::velox::text {

using dwio::common::BufferedInput;
using dwio::common::ReaderOptions;
using dwio::common::RowReader;
using dwio::common::RowReaderOptions;
using dwio::common::TypeWithId;

struct FileContents {
  const size_t COLUMN_POSITION_INVALID = std::numeric_limits<size_t>::max();
  const std::shared_ptr<const RowType> schema;
  memory::MemoryPool& pool;
  uint64_t fileLength;
  common::CompressionKind compression;
  dwio::common::SerDeOptions serDeOptions;
  std::array<bool, 128> needsEscape;

  FileContents(
      memory::MemoryPool& pool,
      const std::shared_ptr<const RowType>& t);
};

class ReaderBase {
 private:
  std::shared_ptr<FileContents> contents_;
  std::shared_ptr<const TypeWithId> schemaWithId_;
  const ReaderOptions& options_;
  std::shared_ptr<BufferedInput> input_;

  // Usable only for testing.
  std::shared_ptr<const RowType> internalSchema_;

 public:
  ReaderBase(
      std::unique_ptr<BufferedInput> input,
      const ReaderOptions& options);

  common::CompressionKind getCompression() const;

  uint64_t getNumberOfRows() const;

  std::unique_ptr<RowReader> createRowReader() const;

  std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& options) const;

  std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& options,
      bool prestoTextReader) const;

  uint64_t getFileLength() const;

  const std::shared_ptr<const RowType>& getType() const;

  uint64_t getMemoryUse();
};

class TextReaderImpl : public RowReader {
 private:
  const ReaderOptions& options_;

  std::shared_ptr<FileContents> contents_;
  std::shared_ptr<const TypeWithId> schemaWithId_;

  // Usable only for testing.
  std::shared_ptr<const RowType> internalSchema_;

 public:
  TextReaderImpl(
      std::unique_ptr<velox::dwio::common::ReadFileInputStream> stream,
      const ReaderOptions& options);

  common::CompressionKind getCompression() const;

  uint64_t getNumberOfRows() const;

  std::unique_ptr<RowReader> createRowReader() const;

  std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& options) const;

  std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& options,
      bool prestoTextReader) const;

  uint64_t getFileLength() const;

  const std::shared_ptr<const RowType>& getType() const;

  uint64_t getMemoryUse();
};

} // namespace facebook::velox::text
