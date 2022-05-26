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

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/core/PlanNode.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::exec {

class AggregateFunctionSignature;

// Returns true if aggregation receives raw (unprocessed) input, e.g. partial
// and single aggregation.
bool isRawInput(core::AggregationNode::Step step);

// Returns false if aggregation produces final result, e.g. final
// and single aggregation.
bool isPartialOutput(core::AggregationNode::Step step);

class Aggregate {
 protected:
  explicit Aggregate(TypePtr resultType) : resultType_(resultType) {}

 public:
  virtual ~Aggregate() {}

  TypePtr resultType() const {
    return resultType_;
  }

  // Returns the fixed number of bytes the accumulator takes on a group
  // row. Variable width accumulators will reference the variable
  // width part of the state from the fixed part.
  virtual int32_t accumulatorFixedWidthSize() const = 0;

  // Return true if accumulator is allocated from external memory, e.g. memory
  // not managed by Velox.
  virtual bool accumulatorUsesExternalMemory() const {
    return false;
  }

  // Returns true if the accumulator never takes more than
  // accumulatorFixedWidthSize() bytes. If this is false, the
  // accumulator needs to track its changing variable length footprint
  // using RowSizeTracker (Aggregate::trackRowSize), see ArrayAggAggregate for
  // sample usage. A group row with at least one variable length key or
  // aggregate will have a 32-bit slot at offset RowContainer::rowSize_ for
  // keeping track of per-row size. The size is relevant for keeping caps on
  // result set and spilling batch sizes with skewed data.
  virtual bool isFixedSize() const {
    return true;
  }

  void setAllocator(HashStringAllocator* allocator) {
    allocator_ = allocator;
  }

  // Sets the offset and null indicator position of 'this'.
  // @param offset Offset in bytes from the start of the row of the accumulator
  // @param nullByte Offset in bytes from the start of the row of the null flag
  // @param nullMask The specific bit in the nullByte that stores the null flag
  // @param rowSizeOffset The offset of a uint32_t row size from the start of
  // the row. Only applies to accumulators that store variable size data out of
  // line. Fixed length accumulators do not use this. 0 if the row does not have
  // a size field.
  void setOffsets(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      int32_t rowSizeOffset) {
    nullByte_ = nullByte;
    nullMask_ = nullMask;
    offset_ = offset;
    rowSizeOffset_ = rowSizeOffset;
  }

  // Initializes null flags and accumulators for newly encountered groups.
  // @param groups Pointers to the start of the new group rows.
  // @param indices Indices into 'groups' of the new entries.
  virtual void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) = 0;

  // Single Aggregate instance is able to take both raw data and
  // intermediate result as input based on the assumption that Partial
  // accumulator and Final accumulator are of the same type.
  //
  // Updates partial accumulators from raw input data.
  // @param groups Pointers to the start of the group rows. These are aligned
  // with the 'args', e.g. data in the i-th row of the 'args' goes to the i-th
  // group. The groups may repeat if different rows go into the same group.
  // @param rows Rows of the 'args' to add to the accumulators. These may not be
  // contiguous if the aggregation has mask or is configured to drop null
  // grouping keys. The latter would be the case when aggregation is followed
  // by the join on the grouping keys. 'rows' is guaranteed to have at least one
  // active row.
  // @param args Raw input.
  // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
  // The pushdown can happen only if this flag is true and 'args' is a single
  // LazyVector.
  virtual void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Updates final accumulators from intermediate results.
  // @param groups Pointers to the start of the group rows. These are aligned
  // with the 'args', e.g. data in the i-th row of the 'args' goes to the i-th
  // group. The groups may repeat if different rows go into the same group.
  // @param rows Rows of the 'args' to add to the accumulators. These may not be
  // contiguous if the aggregation has mask or is configured to drop null
  // grouping keys. The latter would be the case when aggregation is followed
  // by the join on the grouping keys. 'rows' is guaranteed to have at least one
  // active row.
  // @param args Intermediate results produced by extractAccumulators().
  // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
  // The pushdown can happen only if this flag is true and 'args' is a single
  // LazyVector.
  virtual void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Updates the single partial accumulator from raw input data for global
  // aggregation.
  // @param group Pointer to the start of the group row.
  // @param rows Rows of the 'args' to add to the accumulators. These may not
  // be contiguous if the aggregation has mask. 'rows' is guaranteed to have at
  // least one active row.
  // @param args Raw input to add to the accumulators.
  // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
  // The pushdown can happen only if this flag is true and 'args' is a single
  // LazyVector.
  virtual void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Updates the single final accumulator from intermediate results for global
  // aggregation.
  // @param group Pointer to the start of the group row.
  // @param rows Rows of the 'args' to add to the accumulators. These may not
  // be contiguous if the aggregation has mask. 'rows' is guaranteed to have at
  // least one active row.
  // @param args Intermediate results produced by extractAccumulators().
  // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
  // The pushdown can happen only if this flag is true and 'args' is a single
  // LazyVector.
  virtual void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Finalizes the state in groups. Defaults to no op for cases like
  // sum and max.
  virtual void finalize(char** groups, int32_t numGroups) = 0;

  // Extracts final results (used for final and single aggregations).
  // @param groups Pointers to the start of the group rows.
  // @param numGroups Number of groups to extract results from.
  // @param result The result vector to store the results in.
  //
  // 'result' and its parts are expected to be singly referenced. If
  // other threads or operators hold references that they would use
  // after 'result' has been updated by this, effects will b unpredictable.
  virtual void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) = 0;

  // Extracts partial results (used for partial and intermediate aggregations).
  // @param groups Pointers to the start of the group rows.
  // @param numGroups Number of groups to extract results from.
  // @param result The result vector to store the results in.
  //
  // See comment on 'result' in extractValues().
  virtual void
  extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result) = 0;

  // Frees any out of line storage for the accumulator in
  // 'groups'. No-op for fixed length accumulators.
  virtual void destroy(folly::Range<char**> /*groups*/) {}

  // Clears state between reuses, e.g. this is called before reusing
  // the aggregation operator's state after flushing a partial
  // aggregation.
  void clear() {
    numNulls_ = 0;
  }

  static std::unique_ptr<Aggregate> create(
      const std::string& name,
      core::AggregationNode::Step step,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& resultType);

  // Returns the intermediate type for 'name' with signature
  // 'argTypes'. Throws if cannot resolve.
  static TypePtr intermediateType(
      const std::string& name,
      const std::vector<TypePtr>& argTypes);

 protected:
  // Shorthand for maintaining accumulator variable length size in
  // accumulator update methods. Use like: { auto tracker =
  // trackRowSize(group); update(group); }
  RowSizeTracker<char, uint32_t> trackRowSize(char* group) {
    return RowSizeTracker<char, uint32_t>(group[rowSizeOffset_], *allocator_);
  }

  bool isNull(char* group) const {
    return numNulls_ && (group[nullByte_] & nullMask_);
  }

  void incrementRowSize(char* row, uint64_t bytes) {
    VELOX_DCHECK(rowSizeOffset_);
    uint32_t* ptr = reinterpret_cast<uint32_t*>(row + rowSizeOffset_);
    uint64_t size = *ptr + bytes;
    *ptr = std::min<uint64_t>(size, std::numeric_limits<uint32_t>::max());
  }

  // Sets null flag for all specified groups to true.
  // For any given group, this method can be called at most once.
  void setAllNulls(char** groups, folly::Range<const vector_size_t*> indices) {
    for (auto i : indices) {
      groups[i][nullByte_] |= nullMask_;
    }
    numNulls_ += indices.size();
  }

  inline bool clearNull(char* group) {
    if (numNulls_) {
      uint8_t mask = group[nullByte_];
      if (mask & nullMask_) {
        group[nullByte_] = mask & ~nullMask_;
        --numNulls_;
        return true;
      }
    }
    return false;
  }

  template <typename T>
  T* value(char* group) const {
    return reinterpret_cast<T*>(group + offset_);
  }

  template <typename T>
  static uint64_t* getRawNulls(T* vector) {
    if (vector->mayHaveNulls()) {
      BufferPtr nulls = vector->mutableNulls(vector->size());
      return nulls->asMutable<uint64_t>();
    } else {
      return nullptr;
    }
  }

  static void clearNull(uint64_t* rawNulls, vector_size_t index) {
    if (rawNulls) {
      bits::clearNull(rawNulls, index);
    }
  }

  const TypePtr resultType_;

  // Byte position of null flag in group row.
  int32_t nullByte_;
  uint8_t nullMask_;
  // Offset of fixed length accumulator state in group row.
  int32_t offset_;

  // Offset of uint32_t row byte size of row. 0 if there are no
  // variable width fields or accumulators on the row.  The size is
  // capped at 4G and will stay at 4G and not wrap around if growing
  // past this. This serves to track the batch size when extracting
  // rows. A size in excess of 4G would finish the batch in any case,
  // so larger values need not be represented.
  int32_t rowSizeOffset_ = 0;

  // Number of null accumulators in the current state of the aggregation
  // operator for this aggregate. If 0, clearing the null as part of update
  // is not needed.
  uint64_t numNulls_ = 0;
  HashStringAllocator* allocator_;

  // When selectivity vector has holes, in the pushdown, we need to generate a
  // different indices vector as the one we get from the DecodedVector is simply
  // sequential.
  std::vector<vector_size_t> pushdownCustomIndices_;
};

using AggregateFunctionFactory = std::function<std::unique_ptr<Aggregate>(
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& resultType)>;

/// Register an aggregate function with the specified name and signatures.
bool registerAggregateFunction(
    const std::string& name,
    std::vector<std::shared_ptr<AggregateFunctionSignature>> signatures,
    AggregateFunctionFactory factory);

/// Returns signatures of the aggregate function with the specified name.
/// Returns empty std::optional if function with that name is not found.
std::optional<std::vector<std::shared_ptr<AggregateFunctionSignature>>>
getAggregateFunctionSignatures(const std::string& name);

struct AggregateFunctionEntry {
  std::vector<std::shared_ptr<AggregateFunctionSignature>> signatures;
  AggregateFunctionFactory factory;
};

using AggregateFunctionMap =
    std::unordered_map<std::string, AggregateFunctionEntry>;

AggregateFunctionMap& aggregateFunctions();
} // namespace facebook::velox::exec
