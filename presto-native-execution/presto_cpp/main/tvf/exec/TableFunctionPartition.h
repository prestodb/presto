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

#include "velox/exec/RowContainer.h"
#include "velox/vector/BaseVector.h"

/// Simple TableFunctionPartition that builds over the RowContainer used for
/// storing the input rows in the Table Function Operator. This works completely
/// in-memory.

namespace facebook::presto::tvf {

class TableFunctionPartition {
 public:
  /// The TableFunctionPartition is used by the TableFunctionOperator and
  /// TableFunction objects to access the underlying data and columns of a
  /// partition of rows.
  /// The TableFunctionPartition is constructed by TableFunctionOperator
  /// from the input data.
  /// 'data' : Underlying RowContainer of the TableFunctionOperator.
  TableFunctionPartition(
      velox::exec::RowContainer* data,
      const folly::Range<char**>& rows,
      const std::vector<velox::column_index_t>& inputMapping);

  ~TableFunctionPartition();

  /// Returns the number of rows in the current TableFunctionPartition.
  velox::vector_size_t numRows() const {
    return partition_.size();
  }

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for the rows at positions in the 'rowNumbers'
  /// array from the partition input data.
  void extractColumn(
      int32_t columnIndex,
      folly::Range<const velox::vector_size_t*> rowNumbers,
      velox::vector_size_t resultOffset,
      const velox::VectorPtr& result) const;

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for 'numRows' starting at positions 'partitionOffset'
  /// in the partition input data.
  void extractColumn(
      int32_t columnIndex,
      velox::vector_size_t partitionOffset,
      velox::vector_size_t numRows,
      velox::vector_size_t resultOffset,
      const velox::VectorPtr& result) const;

  /// Extracts null positions at 'columnIndex' into 'nullsBuffer' for
  /// 'numRows' starting at positions 'partitionOffset' in the partition
  /// input data.
  void extractNulls(
      int32_t columnIndex,
      velox::vector_size_t partitionOffset,
      velox::vector_size_t numRows,
      const velox::BufferPtr& nullsBuffer) const;

 private:
  // The RowContainer associated with the partition.
  // It is owned by the TablePartitionBuild that creates the partition.
  velox::exec::RowContainer* const data_;

  // folly::Range is for the partition rows iterator provided by the
  // TableFunctionOperator. The pointers are to rows from a RowContainer owned
  // by the operator. We can assume these are valid values for the lifetime
  // of TableFunctionPartition.
  folly::Range<char**> partition_;

  // Mapping from window input column -> index in data_. This is required
  // because the TableFunctionPartitionBuild reorders data_ to place partition
  // and sort keys before other columns in data_. But the TableFunctionOperator
  // and TableFunction code accesses TableFunctionPartition using the
  // indexes of TableFunction input type.
  const std::vector<velox::column_index_t> inputMapping_;
};
} // namespace facebook::presto::tvf
