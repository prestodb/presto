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

#include "velox/experimental/cudf/vector/CudfVector.h"

#include "velox/buffer/Buffer.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/TypeAliases.h"

#include <cudf/column/column.hpp>
#include <cudf/table/table.hpp>

namespace facebook::velox::cudf_velox {
namespace {

/// Calculates the total memory size in bytes of a cudf column and reconstructs
/// it.
///
/// This function disassembles a cudf column to access its underlying memory
/// buffers, calculates the total size including children columns (for nested
/// types), and then reassembles the column.
///
/// @return A pair containing the total size in bytes and the reconstructed
/// column
std::pair<uint64_t, std::unique_ptr<cudf::column>> getColumnSize(
    std::unique_ptr<cudf::column> column) {
  // Store column metadata (type, null count, and size) before releasing it,
  // as the release() operation transfers ownership of the underlying buffers
  // and invalidates access to these properties.
  auto type = column->type();
  auto nullCount = column->null_count();
  auto size = column->size();

  auto contents = column->release();
  auto bytes = contents.data->size() + contents.null_mask->size();

  // Recursively get the size of the children columns.
  std::vector<std::unique_ptr<cudf::column>> children;
  for (auto& child : contents.children) {
    auto [childBytes, childColumn] = getColumnSize(std::move(child));
    bytes += childBytes;
    children.push_back(std::move(childColumn));
  }

  // Reassemble the column with the original metadata.
  auto reconstitutedColumn = std::make_unique<cudf::column>(
      type,
      size,
      std::move(*contents.data.release()),
      std::move(*contents.null_mask.release()),
      nullCount,
      std::move(children));

  return std::make_pair(bytes, std::move(reconstitutedColumn));
}

/// Calculates the total memory size in bytes of a cudf table and reconstructs
/// it.
///
/// This function disassembles a cudf table to access its underlying columns,
/// calculates the total size, and then reassembles the table.
///
/// @note This is a workaround because cudf::table doesn't have an API to get
/// this information without involving estimation and d->h copies.
/// @see https://github.com/rapidsai/cudf/issues/18462
///
/// @return A pair containing the total size in bytes and the reconstructed
/// table
std::pair<uint64_t, std::unique_ptr<cudf::table>> getTableSize(
    std::unique_ptr<cudf::table>&& table) {
  auto columns = table->release();
  std::vector<std::unique_ptr<cudf::column>> columnsOut;
  uint64_t totalBytes = 0;

  for (auto& column : columns) {
    auto [bytes, columnOut] = getColumnSize(std::move(column));
    totalBytes += bytes;
    columnsOut.push_back(std::move(columnOut));
  }
  return std::make_pair(
      totalBytes, std::make_unique<cudf::table>(std::move(columnsOut)));
}

} // namespace

CudfVector::CudfVector(
    velox::memory::MemoryPool* pool,
    TypePtr type,
    vector_size_t size,
    std::unique_ptr<cudf::table>&& table,
    rmm::cuda_stream_view stream)
    : RowVector(
          pool,
          std::move(type),
          BufferPtr(nullptr),
          size,
          std::vector<VectorPtr>(),
          std::nullopt),
      table_{std::move(table)},
      stream_{stream} {
  auto [bytes, tableOut] = getTableSize(std::move(table_));
  flatSize_ = bytes;
  table_ = std::move(tableOut);
}

uint64_t CudfVector::estimateFlatSize() const {
  return flatSize_;
}

} // namespace facebook::velox::cudf_velox
