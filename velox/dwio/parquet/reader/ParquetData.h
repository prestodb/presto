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

#include <thrift/protocol/TCompactProtocol.h> //@manual
#include "velox/common/base/RawVector.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/parquet/reader/PageReader.h"
#include "velox/dwio/parquet/thrift/ParquetThriftTypes.h"
#include "velox/dwio/parquet/thrift/ThriftTransport.h"

namespace facebook::velox::parquet {
class ParquetParams : public dwio::common::FormatParams {
 public:
  ParquetParams(memory::MemoryPool& pool, const thrift::FileMetaData& metaData)
      : FormatParams(pool), metaData_(metaData) {}
  std::unique_ptr<dwio::common::FormatData> toFormatData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const common::ScanSpec& scanSpec) override;

 private:
  const thrift::FileMetaData& metaData_;
};

/// Format-specific data created for each leaf column of a Parquet rowgroup.
class ParquetData : public dwio::common::FormatData {
 public:
  ParquetData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const std::vector<thrift::RowGroup>& rowGroups,
      memory::MemoryPool& pool)
      : pool_(pool),
        type_(std::static_pointer_cast<const ParquetTypeWithId>(type)),
        rowGroups_(rowGroups),
        maxDefine_(type_->maxDefine_),
        maxRepeat_(type_->maxRepeat_),
        rowsInRowGroup_(-1) {}

  /// Prepares to read data for 'index'th row group.
  void enqueueRowGroup(uint32_t index, dwio::common::BufferedInput& input);

  /// Positions 'this' at 'index'th row group. enqueueRowGroup must be called
  /// first. The returned PositionProvider is empty and should not be used.
  /// Other formats may use it.
  dwio::common::PositionProvider seekToRowGroup(uint32_t index) override;

  /// True if 'filter' may have hits for the column of 'this' according to the
  /// stats in 'rowGroup'.
  bool rowGroupMatches(
      uint32_t rowGroupId,
      common::Filter* FOLLY_NULLABLE filter) override;

  std::vector<uint32_t> filterRowGroups(
      const common::ScanSpec& scanSpec,
      uint64_t rowsPerRowGroup,
      const dwio::common::StatsContext& writerContext) override;

  // Reads null flags for 'numValues' next top level rows. The first 'numValues'
  // bits of 'nulls' are set and the reader is advanced by numValues'.
  void readNullsOnly(int32_t numValues, BufferPtr& nulls) {
    reader_->readNullsOnly(numValues, nulls);
  }

  bool hasNulls() const override {
    return maxDefine_ > 0;
  }

  void readNulls(
      vector_size_t numValues,
      const uint64_t* FOLLY_NULLABLE incomingNulls,
      BufferPtr& nulls,
      bool nullsOnly = false) override {
    // If the query accesses only nulls, read the nulls from the pages in range.
    if (nullsOnly) {
      readNullsOnly(numValues, nulls);
      return;
    }
    // There are no column-level nulls in Parquet, only page-level ones, so this
    // is always non-null.
    nulls = nullptr;
  }

  uint64_t skipNulls(uint64_t numValues, bool nullsOnly) override {
    // If we are seeking a column where nulls and data are read, the skip is
    // done in skip(). If we are reading nulls only, this is called with
    // 'nullsOnly' set and is responsible for reading however many nulls or
    // pages it takes to skip 'numValues' top level rows.
    if (nullsOnly) {
      reader_->skipNullsOnly(numValues);
    }
    return numValues;
  }

  uint64_t skip(uint64_t numRows) override {
    reader_->skip(numRows);
    return numRows;
  }

  /// Applies 'visitor' to the data in the column of 'this'. See
  /// PageReader::readWithVisitor().
  template <typename Visitor>
  void readWithVisitor(Visitor visitor) {
    reader_->readWithVisitor(visitor);
  }

  const VectorPtr& dictionaryValues() {
    return reader_->dictionaryValues();
  }

  void clearDictionary() {
    reader_->clearDictionary();
  }

  bool hasDictionary() const {
    return reader_->isDictionary();
  }

 protected:
  memory::MemoryPool& pool_;
  std::shared_ptr<const ParquetTypeWithId> type_;
  const std::vector<thrift::RowGroup>& rowGroups_;
  // Streams for this column in each of 'rowGroups_'. Will be created on or
  // ahead of first use, not at construction.
  std::vector<std::unique_ptr<dwio::common::SeekableInputStream>> streams_;

  const uint32_t maxDefine_;
  const uint32_t maxRepeat_;
  int64_t rowsInRowGroup_;
  std::unique_ptr<PageReader> reader_;
};

} // namespace facebook::velox::parquet
