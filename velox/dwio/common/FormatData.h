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

#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/type/Filter.h"

namespace facebook::velox::dwio::common {

/// Interface base class for format-specific state in common between different
/// file format readers.
class FormatData {
 public:
  virtual ~FormatData() = default;

  template <typename T>
  T& as() {
    return *reinterpret_cast<T*>(this);
  }

  /// Reads nulls if the format has nulls separate from the encoded
  /// data. If there are no nulls, 'nulls' is set to nullptr, else to
  /// a suitable sized and padded Buffer. 'incomingNulls' may be given
  /// if there are enclosing level nulls that should be merged into
  /// the read reasult. If provided, this has 'numValues' bits and
  /// each zero marks an incoming null for which no bit is read from
  /// the nulls stream of 'this'. For Parquet, 'nulls' is always set
  /// to nullptr because nulls are represented by the data pages
  /// themselves. If 'nullsOnly' is true, formats like Parquet will
  /// access the nulls in the pages in range and return the
  /// concatenated nulls in 'nulls'. This is done when only null flags
  /// of a column are of interest, e.g. is null filter.
  virtual void readNulls(
      vector_size_t numValues,
      const uint64_t* FOLLY_NULLABLE incomingNulls,
      BufferPtr& nulls,
      bool nullsOnly = false) = 0;

  /// Reads 'numValues' bits of null flags and returns the number of
  /// non-nulls in the read null flags. If 'nullsOnly' is false this
  /// is a no-op for formats like Parquet where the null flags are
  /// mixed with the data. For those cases, skip deals with the nulls
  /// and the data. If reading nulls only, 'nullsOnly' is true and
  /// then the reader is advanced by 'numValues' null flags without
  /// touching the data also in formats where nulls and data are
  /// mixed. This latter case applies only to queries which do not
  /// touch the data of the column, e.g. a is null filter.
  virtual uint64_t skipNulls(uint64_t numValues, bool nullsOnly = false) = 0;

  /// Skips 'numValues' top level rows and returns the number of
  /// non-null top level rows skipped. For ORC, reverts to
  /// skipNulls. The caller is expected to skip the other streams.  For
  /// Parquet, skips top level rows. In Parquet the caller does not
  /// need to skip anything else since the data is all in one stream.
  virtual uint64_t skip(uint64_t numValues) = 0;

  /// True if 'this' may produce a null. true does not imply the
  /// existence of an actual null, though. For example, if the format
  /// is ORC and there is a nulls decoder for the column this returns
  /// true. False if it is certain there is no null in the range of
  /// 'this'. In ORC this means the column does not add nulls but does
  /// not exclude nulls coming from enclosing null structs.
  virtual bool hasNulls() const = 0;

  /// Seeks the position to the 'index'th row group for the streams
  /// managed by 'this'. Returns a PositionProvider for streams not
  /// managed by 'this'. In a format like Parquet where all the reading
  /// is in FormatData the provider is at end. For ORC/DWRF the type
  /// dependent stream positions are accessed via the provider. The
  /// provider is valid until next call of this.
  virtual dwio::common::PositionProvider seekToRowGroup(uint32_t index) = 0;

  /// Applies 'scanSpec' to the metadata of 'this'. Returns row group
  /// numbers that can be skipped. The interpretation of row group
  /// number is format-dependent. In ORC, these are row groups in the
  /// current stripe, in Parquet these are row group numbers in the
  /// file.
  virtual std::vector<uint32_t> filterRowGroups(
      const velox::common::ScanSpec& scanSpec,
      uint64_t rowsPerRowGroup,
      const StatsContext& writerContext) = 0;

  /// If the format divides data into even-sized runs for purposes of
  /// column statistics and random access, returns the number of rows
  /// in such a group. This is the row group size for ORC and nullopt
  /// for Parquet, where row groups are physically separate runs of
  /// arbitrary nunber of rows.
  virtual std::optional<int64_t> rowsPerRowGroup() const {
    return std::nullopt;
  }

  /// Test if the 'i'th RowGroup can potentially match the 'filter' using the
  /// RowGroup's statistics on all columns. Returns true if all columns in this
  /// RowGroup matches the filter, false otherwise. A column is said to match
  /// the filter if its ColumnStatistics passes the filter, or the filter for
  /// that column is NULL.
  virtual bool rowGroupMatches(
      uint32_t rowGroupId,
      velox::common::Filter* FOLLY_NULLABLE filter) = 0;
};

/// Base class for format-specific reader initialization arguments.
class FormatParams {
 public:
  explicit FormatParams(memory::MemoryPool& pool) : pool_(pool) {}

  virtual ~FormatParams() = default;

  /// Makes format-specific structures for the column given by  'type'.
  /// 'scanSpec' is given as extra context.
  virtual std::unique_ptr<FormatData> toFormatData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const velox::common::ScanSpec& scanSpec) = 0;

  memory::MemoryPool& pool() {
    return pool_;
  }

 private:
  memory::MemoryPool& pool_;
};

} // namespace facebook::velox::dwio::common
