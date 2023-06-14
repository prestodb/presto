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

#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/exec/RowContainer.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

/// Accumulates inputs for aggregations over sorted input, sorts these inputs
/// and computes aggregates.
class SortedAggregations {
 public:
  /// @param aggregates Non-empty list of aggregates that require inputs to be
  /// sorted.
  /// @param inputType Input row type for the aggregation operator.
  /// @param pool Memory pool.
  SortedAggregations(
      std::vector<AggregateInfo*> aggregates,
      const RowTypePtr& inputType,
      memory::MemoryPool* pool);

  /// Returns metadata about the accumulator used to store lists of input rows.
  Accumulator accumulator() const;

  /// Aggregate-like APIs to aggregate input rows per group.
  void setAllocator(HashStringAllocator* allocator) {
    allocator_ = allocator;
  }

  void setOffsets(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      int32_t rowSizeOffset) {
    offset_ = offset;
    nullByte_ = nullByte;
    nullMask_ = nullMask;
    rowSizeOffset_ = rowSizeOffset;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices);

  void addInput(char** groups, const RowVectorPtr& input);

  void addSingleGroupInput(char* group, const RowVectorPtr& input);

  void noMoreInput();

  /// Sorts input row for the specified groups, computes aggregations and stores
  /// results in the specified 'result' vector.
  void extractValues(folly::Range<char**> groups, const RowVectorPtr& result);

 private:
  void addNewRow(char* group, char* newRow);

  bool compareRowsWithKeys(
      const char* lhs,
      const char* rhs,
      const std::vector<std::pair<column_index_t, core::SortOrder>>& keys);

  void sortSingleGroup(
      std::vector<char*>& groupRows,
      const AggregateInfo& aggregate);

  std::vector<VectorPtr> extractSingleGroup(
      std::vector<char*>& groupRows,
      const AggregateInfo& aggregate);

  const std::vector<AggregateInfo*> aggregates_;

  /// Indices of all inputs for all aggregates.
  std::vector<column_index_t> inputs_;
  /// Stores all input rows for all groups.
  std::unique_ptr<RowContainer> inputData_;
  /// Mapping from the input column index to an index into 'inputs_'.
  std::vector<column_index_t> inputMapping_;

  std::vector<DecodedVector> decodedInputs_;

  HashStringAllocator* allocator_;
  int32_t offset_;
  int32_t nullByte_;
  uint8_t nullMask_;
  int32_t rowSizeOffset_;
};

} // namespace facebook::velox::exec
