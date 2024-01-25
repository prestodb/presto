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

// Adapted from Apache Arrow.

#pragma once

#include "velox/dwio/common/compression/Compression.h"

namespace facebook::velox::parquet {

/// ColumnChunkMetaDataPtr is a proxy around pointer to thrift::ColumnChunk.
class ColumnChunkMetaDataPtr {
 public:
  explicit ColumnChunkMetaDataPtr(const void* metadata);

  ~ColumnChunkMetaDataPtr();

  int64_t numValues() const;

  common::CompressionKind compression() const;

 private:
  const void* ptr_;
};

/// RowGroupMetaDataPtr is a proxy around pointer to thrift::RowGroup.
class RowGroupMetaDataPtr {
 public:
  explicit RowGroupMetaDataPtr(const void* metadata);

  ~RowGroupMetaDataPtr();

  /// The number of columns in this row group. The order must match the
  /// parent's column ordering.
  int numColumns() const;

  /// Return the ColumnChunkMetaData pointer of the corresponding column index.
  ColumnChunkMetaDataPtr columnChunk(int index) const;

  /// Number of rows in this row group.
  int64_t numRows() const;

  /// Total byte size of all the uncompressed column data in this row
  /// group.
  int64_t totalByteSize() const;

  /// Total byte size of all the compressed (and potentially encrypted)
  /// column data in this row group.
  /// This information is optional and may be 0 if omitted.
  int64_t totalCompressedSize() const;

 private:
  const void* ptr_;
};

/// FileMetaData is a proxy around pointer to thrift::FileMetaData.
class FileMetaDataPtr {
 public:
  explicit FileMetaDataPtr(const void* metadata);

  ~FileMetaDataPtr();

  /// The total number of rows.
  int64_t numRows() const;

  /// The number of row groups in the file.
  int numRowGroups() const;

  /// Return the RowGroupMetaData pointer of the corresponding row group index.
  RowGroupMetaDataPtr rowGroup(int index) const;

 private:
  const void* ptr_;
};

} // namespace facebook::velox::parquet
