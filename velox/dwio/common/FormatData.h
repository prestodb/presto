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
  /// data. If there are no nulls, 'nulls' is set to nullptr, else to a
  /// suitable sized and padded Buffer. 'incomingNulls' may be given if
  /// there are enclosing level nulls that should be merged into the
  /// read reasult. If provided, this has 'numValues' bits and each
  /// zero marks an incoming null for which no bit is read from the
  /// nulls stream of 'this'. For Parquet, 'nulls' is always set to
  /// nullptr because nulls are represented by the data pages
  /// themselves.
  virtual void readNulls(
      vector_size_t numValues,
      const uint64_t* incomingNulls,
      BufferPtr& nulls) = 0;

  /// Reads 'numValues' bits of null flags and returns the number of non-nulls
  /// in the read null flags. No-op for formats that do not have separate null
  /// flags, e.g. Parquet.
  virtual uint64_t skipNulls(uint64_t numValues) = 0;

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
  /// 'this'.
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
      const StatsContext& context) = 0;
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
