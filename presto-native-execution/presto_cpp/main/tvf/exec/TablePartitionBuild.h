/*
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

#include "presto_cpp/main/tvf/exec/TableFunctionPartition.h"

#include "velox/exec/PrefixSort.h"

namespace facebook::presto::tvf {

// This is a class used by the TableFunction Operator to maintain and provide
// table partitions for function evaluation at execution.

class TablePartitionBuild {
 public:
  TablePartitionBuild(
      const velox::RowTypePtr& inputType,
      std::vector<velox::core::FieldAccessTypedExprPtr> partitionKeys,
      std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys,
      std::vector<velox::core::SortOrder> sortingOrders,
      velox::memory::MemoryPool* pool,
      velox::common::PrefixSortConfig&& prefixSortConfig);

  ~TablePartitionBuild() {
    pool_->release();
  }

  void addInput(velox::RowVectorPtr input);

  void noMoreInput();

  /// Returns true if a new Table function partition is available for the
  /// Table function operator to consume.
  bool hasNextPartition();

  /// The Table function operator invokes this function to get the next
  /// TableFunctionPartition for evaluation. The TableFunctionPartition
  /// has APIs to access the underlying columns of Table Function Operator data.
  /// Check hasNextPartition() before invoking this function. This function
  /// fails if called when no partition is available.
  std::shared_ptr<TableFunctionPartition> nextPartition();

  velox::vector_size_t numRows() {
    return numRows_;
  }

  /// Returns the average size of input rows in bytes stored in the data
  /// container of the WindowBuild.
  std::optional<int64_t> estimateRowSize() {
    return data_->estimateRowSize();
  }

 private:
  // Main sorting function loop done after all input rows are received
  // by WindowBuild.
  void sortPartitions();

  // Function to compute the partitionStartRows_ structure.
  // partitionStartRows_ is vector of the starting rows index
  // of each partition in the data. This is an auxiliary
  // structure that helps simplify the window function computations.
  void computePartitionStartRows();

  // Find the next partition start row from start.
  velox::vector_size_t findNextPartitionStartRow(velox::vector_size_t start);

  bool compareRowsWithKeys(
      const char* lhs,
      const char* rhs,
      const std::vector<
          std::pair<velox::column_index_t, velox::core::SortOrder>>& keys);

  velox::memory::MemoryPool* const pool_;

  /// Input column types in 'inputChannels_' order.
  velox::RowTypePtr inputType_;

  // Compare flags for partition and sorting keys. Compare flags for partition
  // keys are set to default values. Compare flags for sorting keys match
  // sorting order specified in the plan node.
  //
  // Used to sort 'data_' while spilling and in Prefix sort.
  const std::vector<velox::CompareFlags> compareFlags_;

  // Config for Prefix-sort.
  const velox::common::PrefixSortConfig prefixSortConfig_;

  /// Input columns in the order of: partition keys, sorting keys, the rest.
  std::vector<velox::column_index_t> inputChannels_;

  /// The mapping from original input column index to the index after column
  /// reordering. This is the inversed mapping of inputChannels_.
  std::vector<velox::column_index_t> inversedInputChannels_;

  /// The RowContainer holds all the input rows in TablePartitionBuild. Columns
  /// are already reordered according to inputChannels_.
  std::unique_ptr<velox::exec::RowContainer> data_;

  /// The decodedInputVectors_ are reused across addInput() calls to decode the
  /// partition and sort keys for the above RowContainer.
  std::vector<velox::DecodedVector> decodedInputVectors_;

  /// Number of input rows.
  velox::vector_size_t numRows_ = 0;

  /// The below 2 vectors represent the ChannelIndex of the partition keys
  /// and the order by keys. These keyInfo are used for sorting by those
  /// key combinations during the processing. partitionKeyInfo_ is used to
  /// separate partitions in the rows. sortKeyInfo_ is used to identify peer
  /// rows in a partition.
  std::vector<std::pair<velox::column_index_t, velox::core::SortOrder>>
      partitionKeyInfo_;
  std::vector<std::pair<velox::column_index_t, velox::core::SortOrder>>
      sortKeyInfo_;

  // allKeyInfo_ is a combination of (partitionKeyInfo_ and sortKeyInfo_).
  // It is used to perform a full sorting of the input rows to be able to
  // separate partitions and sort the rows in it. The rows are output in
  // this order by the operator.
  std::vector<std::pair<velox::column_index_t, velox::core::SortOrder>>
      allKeyInfo_;

  // Vector of pointers to each input row in the data_ RowContainer.
  // The rows are sorted by partitionKeys + sortKeys. This total
  // ordering can be used to split partitions (with the correct
  // order by) for the processing.
  std::vector<char*, velox::memory::StlAllocator<char*>> sortedRows_;

  // This is a vector that gives the index of the start row
  // (in sortedRows_) of each partition in the RowContainer data_.
  // This auxiliary structure helps demarcate partitions.
  std::vector<
      velox::vector_size_t,
      velox::memory::StlAllocator<velox::vector_size_t>>
      partitionStartRows_;

  // Current partition being output. Used to construct WindowPartitions
  // during resetPartition.
  velox::vector_size_t currentPartition_ = -1;
};

} // namespace facebook::presto::tvf
