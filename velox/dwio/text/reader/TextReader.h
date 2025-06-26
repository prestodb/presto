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

#include <array>
#include <limits>
#include <string>

#include "folly/CppAttributes.h"
#include "velox/buffer/StringViewBufferHolder.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/TypeWithId.h"

namespace facebook::velox::dwio::common {

using memory::MemoryPool;
using velox::common::CompressionKind;
using velox::common::ScanSpec;

// Shared state for a file between TextReader and TextRowReader
struct FileContents {
  FileContents(MemoryPool& pool, const std::shared_ptr<const RowType>& t);

  const size_t COLUMN_POSITION_INVALID = std::numeric_limits<size_t>::max();
  const std::shared_ptr<const RowType> schema;

  std::unique_ptr<BufferedInput> input;
  std::unique_ptr<SeekableInputStream> inputStream;
  MemoryPool& pool;
  uint64_t fileLength;
  CompressionKind compression;
  SerDeOptions serDeOptions;
  std::array<bool, 128> needsEscape;
};

using DelimType = uint8_t;
constexpr DelimType DelimTypeNone = 0;
constexpr DelimType DelimTypeEOR = 1;
constexpr DelimType DelimTypeEOE = 2;

class TextReader : public Reader {
 public:
  TextReader(
      const ReaderOptions& options,
      std::unique_ptr<BufferedInput> input);

  std::optional<uint64_t> numberOfRows() const override;

  std::unique_ptr<ColumnStatistics> columnStatistics(
      uint32_t index) const override;

  const RowTypePtr& rowType() const override;

  CompressionKind getCompression() const;

  const std::shared_ptr<const TypeWithId>& typeWithId() const override;

  std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& options) const override;

  uint64_t getFileLength() const;

  uint64_t getMemoryUse();

 private:
  ReaderOptions options_;
  mutable std::shared_ptr<const TypeWithId> typeWithId_;
  std::shared_ptr<FileContents> contents_;
  std::shared_ptr<const TypeWithId> schemaWithId_;
  std::shared_ptr<const RowType> internalSchema_;
};

class TextRowReader : public RowReader {
 public:
  TextRowReader(
      std::shared_ptr<FileContents> fileContents,
      const RowReaderOptions& options);

  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const Mutation* mutation = nullptr) override;

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  void updateRuntimeStats(RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

  const ColumnSelector& getColumnSelector() const;

  std::shared_ptr<const TypeWithId> getSelectedType() const;

  uint64_t getRowNumber() const;

  uint64_t seekToRow(uint64_t rowNumber);

 private:
  const RowReaderOptions& getDefaultOpts();

  const std::shared_ptr<const RowType>& getType() const;

  bool isSelectedField(const std::shared_ptr<const TypeWithId>& t);

  const char* getStreamNameData() const;

  uint64_t getLength();

  uint64_t getStreamLength();

  void setEOF();

  void incrementDepth();

  void decrementDepth(DelimType& delim);

  void setEOE(DelimType& delim);

  void resetEOE(DelimType& delim);

  bool isEOE(DelimType delim);

  void setEOR(DelimType& delim);

  bool isEOR(DelimType delim);

  bool isOuterEOR(DelimType delim);

  bool isEOEorEOR(DelimType delim);

  void setNone(DelimType& delim);

  bool isNone(DelimType delim);

  DelimType getDelimType(uint8_t v);

  template <bool skipLF = false>
  char getByteUnchecked(DelimType& delim);

  template <bool skipLF = false>
  char getByteUncheckedOptimized(DelimType& delim);

  uint8_t getByte(DelimType& delim);
  uint8_t getByteOptimized(DelimType& delim);

  bool getEOR(DelimType& delim, bool& isNull);

  bool skipLine();

  void resetLine();

  static StringView
  getStringView(TextRowReader& th, bool& isNull, DelimType& delim);

  template <typename T>
  static T getInteger(TextRowReader& th, bool& isNull, DelimType& delim);

  static bool getBoolean(TextRowReader& th, bool& isNull, DelimType& delim);

  static float getFloat(TextRowReader& th, bool& isNull, DelimType& delim);

  static double getDouble(TextRowReader& th, bool& isNull, DelimType& delim);

  void readElement(
      const std::shared_ptr<const Type>& t,
      const std::shared_ptr<const Type>& reqT,
      BaseVector* FOLLY_NULLABLE data,
      vector_size_t insertionRow,
      DelimType& delim);

  template <class T, class reqT>
  void putValue(
      std::function<T(TextRowReader& th, bool& isNull, DelimType& delim)> f,
      BaseVector* FOLLY_NULLABLE data,
      vector_size_t insertionRow,
      DelimType& delim);

  const std::shared_ptr<FileContents> contents_;
  const std::shared_ptr<const TypeWithId> schemaWithId_;
  const std::shared_ptr<velox::common::ScanSpec>& scanSpec_;

  mutable std::shared_ptr<const TypeWithId> selectedSchema_;

  RowReaderOptions options_;
  ColumnSelector columnSelector_;
  uint64_t currentRow_;
  uint64_t pos_;
  bool atEOL_;
  bool atEOF_;
  bool atSOL_;
  uint8_t depth_;
  std::string unreadData_;
  int unreadIdx_;
  uint64_t limit_; // lowest offset not in the range
  uint64_t fileLength_;
  std::string ownedString_;
  StringViewBufferHolder stringViewBuffer_;
};

} // namespace facebook::velox::dwio::common
