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
      const std::vector<const AggregateInfo*>& aggregates,
      const RowTypePtr& inputType,
      memory::MemoryPool* pool);

  /// Create a SortedAggregations instance using aggregation infos. Return null
  /// if there is no sorted aggregation.
  static std::unique_ptr<SortedAggregations> create(
      const std::vector<AggregateInfo>& aggregates,
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
      int32_t initializedByte,
      uint8_t initializedMask,
      int32_t rowSizeOffset) {
    offset_ = offset;
    nullByte_ = nullByte;
    nullMask_ = nullMask;
    initializedByte_ = initializedByte;
    initializedMask_ = initializedMask;
    rowSizeOffset_ = rowSizeOffset;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices);

  void addInput(char** groups, const RowVectorPtr& input);

  void addSingleGroupInput(char* group, const RowVectorPtr& input);

  void addSingleGroupSpillInput(
      char* group,
      const VectorPtr& input,
      vector_size_t index);

  /// Sorts input row for the specified groups, computes aggregations and stores
  /// results in the specified 'result' vector.
  void extractValues(folly::Range<char**> groups, const RowVectorPtr& result);

  /// Clears all data accumulated so far. Used to release memory after spilling.
  void clear();

 private:
  void addNewRow(char* group, char* newRow);

  // A list of sorting keys along with sorting orders.
  using SortingSpec = std::vector<std::pair<column_index_t, core::SortOrder>>;

  SortingSpec toSortingSpec(const AggregateInfo& aggregate) const;

  bool compareRowsWithKeys(
      const char* lhs,
      const char* rhs,
      const SortingSpec& sortingSpec);

  void sortSingleGroup(
      std::vector<char*>& groupRows,
      const SortingSpec& sortingSpec);

  vector_size_t extractSingleGroup(
      std::vector<char*>& groupRows,
      const AggregateInfo& aggregate,
      std::vector<VectorPtr>& inputVectors);

  void extractForSpill(folly::Range<char**> groups, VectorPtr& result) const;

  struct Hash {
    static uint64_t hashSortOrder(const core::SortOrder& sortOrder) {
      return bits::hashMix(
          folly::hasher<bool>{}(sortOrder.isAscending()),
          folly::hasher<bool>{}(sortOrder.isNullsFirst()));
    }

    size_t operator()(const SortingSpec& elements) const {
      uint64_t hash = 0;

      for (auto i = 0; i < elements.size(); ++i) {
        auto column = elements[i].first;
        auto sortOrder = elements[i].second;

        auto elementHash = bits::hashMix(
            folly::hasher<vector_size_t>{}(column), hashSortOrder(sortOrder));

        hash = (i == 0) ? elementHash : bits::hashMix(hash, elementHash);
      }
      return hash;
    }
  };

  struct EqualTo {
    bool operator()(const SortingSpec& left, const SortingSpec& right) const {
      return left == right;
    }
  };

  // Aggregates grouped by sorting keys and orders.
  folly::
      F14FastMap<SortingSpec, std::vector<const AggregateInfo*>, Hash, EqualTo>
          aggregates_;

  // Indices of all inputs for all aggregates.
  std::vector<column_index_t> inputs_;

  // Stores all input rows for all groups.
  std::unique_ptr<RowContainer> inputData_;

  // Mapping from the input column index to an index into 'inputs_'.
  std::vector<column_index_t> inputMapping_;

  std::vector<DecodedVector> decodedInputs_;

  HashStringAllocator* allocator_;
  int32_t offset_;
  int32_t nullByte_;
  uint8_t nullMask_;
  int32_t initializedByte_;
  uint8_t initializedMask_;
  int32_t rowSizeOffset_;
};

} // namespace facebook::velox::exec
