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

/// Simple WindowPartition that builds over the RowContainer used for storing
/// the input rows in the Window Operator. This works completely in-memory.
/// TODO: This implementation will be revised for Spill to disk semantics.

namespace facebook::velox::exec {
class WindowPartition {
 public:
  /// The WindowPartition is used by the Window operator and WindowFunction
  /// objects to access the underlying data and columns of a partition of rows.
  /// The WindowPartition is constructed by WindowBuild from the input data.
  /// 'data' : Underlying RowContainer of the WindowBuild.
  /// 'rows' : Pointers to rows in the RowContainer belonging to this partition.
  /// 'columns' : Input rows of 'data' used for accessing column data from it.
  /// 'sortKeyInfo' : Order by columns used by the the Window operator. Used to
  /// get peer rows from the input partition.
  WindowPartition(
      RowContainer* data,
      const folly::Range<char**>& rows,
      const std::vector<exec::RowColumn>& columns,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo);

  /// Returns the number of rows in the current WindowPartition.
  vector_size_t numRows() const {
    return partition_.size();
  }

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for the rows at positions in the 'rowNumbers'
  /// array from the partition input data.
  void extractColumn(
      int32_t columnIndex,
      folly::Range<const vector_size_t*> rowNumbers,
      vector_size_t resultOffset,
      const VectorPtr& result) const;

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for 'numRows' starting at positions 'partitionOffset'
  /// in the partition input data.
  void extractColumn(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) const;

  /// Extracts null positions at 'columnIndex' into 'nullsBuffer' for
  /// 'numRows' starting at positions 'partitionOffset' in the partition
  /// input data.
  void extractNulls(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const;

  /// Extracts null positions at 'col' into 'nulls'. The null positions
  /// are from the smallest 'frameStarts' value to the greatest 'frameEnds'
  /// value for 'validRows'. Both 'frameStarts' and 'frameEnds' are buffers
  /// of type vector_size_t.
  /// The returned value is an optional pair of vector_size_t.
  /// The pair is returned only if null values are found in the nulls
  /// extracted. The first value of the pair is the smallest frameStart for
  /// nulls. The second is the number of frames extracted.
  std::optional<std::pair<vector_size_t, vector_size_t>> extractNulls(
      column_index_t col,
      const SelectivityVector& validRows,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      BufferPtr* nulls) const;

  /// Sets in 'rawPeerStarts' and in 'rawPeerEnds' the peer start and peer end
  /// offsets of the rows between 'start' and 'end' of the current partition.
  /// 'peer' row are all the rows having the same value of the order by columns
  /// as the current row.
  /// computePeerBuffers is called multiple times for each partition. It is
  /// called in sequential order of start to end rows.
  /// The peerStarts/peerEnds of the startRow could be same as the last row in
  /// the previous call to computePeerBuffers (if they have the same order by
  /// keys). So peerStart and peerEnd of the last row of this call are returned
  /// to be passed as prevPeerStart and prevPeerEnd to the subsequent
  /// call to computePeerBuffers.
  std::pair<vector_size_t, vector_size_t> computePeerBuffers(
      vector_size_t start,
      vector_size_t end,
      vector_size_t prevPeerStart,
      vector_size_t prevPeerEnd,
      vector_size_t* rawPeerStarts,
      vector_size_t* rawPeerEnds) const;

 private:
  bool compareRowsWithSortKeys(const char* lhs, const char* rhs) const;

  // The RowContainer associated with the partition.
  // It is owned by the WindowBuild that creates the partition.
  RowContainer* data_;

  // folly::Range is for the partition rows iterator provided by the
  // Window operator. The pointers are to rows from a RowContainer owned
  // by the operator. We can assume these are valid values for the lifetime
  // of WindowPartition.
  folly::Range<char**> partition_;

  // Copy of the input RowColumn objects that are used for
  // accessing the partition row columns. These RowColumn objects
  // index into RowContainer data_ above and can retrieve the column values.
  // The order of these columns is the same as that of the input row
  // of the Window operator. The WindowFunctions know the
  // corresponding indexes of their input arguments into this vector.
  // They will request for column vector values at the respective index.
  std::vector<exec::RowColumn> columns_;

  // ORDER BY column info for this partition.
  const std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo_;
};
} // namespace facebook::velox::exec
