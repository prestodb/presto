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

#include <cstdint>
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/exec/RowContainer.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

/// Computes aggregations over de-duplicated inputs. Supports aggregations with
/// single (e.g. sum, count) or multiple (e.g. covar_pop) input columns.
class DistinctAggregations {
 public:
  /// @param aggregates Non-empty list of
  /// aggregates that require inputs to be de-duplicated. All
  /// aggregates should have the same inputs.
  /// @param inputType Input row type for the aggregation operator.
  /// @param pool Memory pool.
  static std::unique_ptr<DistinctAggregations> create(
      std::vector<AggregateInfo*> aggregates,
      const RowTypePtr& inputType,
      memory::MemoryPool* pool);

  virtual ~DistinctAggregations() = default;

  virtual Accumulator accumulator() const = 0;

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

  // Initializes null flags and accumulators for newly encountered groups.  This
  // function should be called only once for each group.
  //
  // @param groups Pointers to the start of the new group rows.
  // @param indices Indices into 'groups' of the new entries.
  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) {
    initializeNewGroupsInternal(groups, indices);

    for (auto index : indices) {
      groups[index][initializedByte_] |= initializedMask_;
    }
  }

  virtual void addInput(
      char** groups,
      const RowVectorPtr& input,
      const SelectivityVector& rows) = 0;

  virtual void addSingleGroupInput(
      char* group,
      const RowVectorPtr& input,
      const SelectivityVector& rows) = 0;

  /// Computes aggregations and stores results in the specified 'result' vector.
  virtual void extractValues(
      folly::Range<char**> groups,
      const RowVectorPtr& result) = 0;

 protected:
  // Initializes null flags and accumulators for newly encountered groups.  This
  // function should be called only once for each group.
  //
  // @param groups Pointers to the start of the new group rows.
  // @param indices Indices into 'groups' of the new entries.
  virtual void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) = 0;

  HashStringAllocator* allocator_;
  int32_t offset_;
  int32_t nullByte_;
  uint8_t nullMask_;
  int32_t initializedByte_;
  uint8_t initializedMask_;
  int32_t rowSizeOffset_;
};

} // namespace facebook::velox::exec
