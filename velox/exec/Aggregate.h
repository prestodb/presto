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

#include "velox/common/serialization/Registry.h"
#include "velox/core/PlanNode.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::exec {

class HashStringAllocator;

// Returns true if aggregation receives raw (unprocessed) input, e.g. partial
// and single aggregation.
bool isRawInput(core::AggregationNode::Step step);

// Returns true if aggregation produces final result, e.g. final
// and single aggregation.
bool isPartialOutput(core::AggregationNode::Step step);

class Aggregate {
 protected:
  explicit Aggregate(core::AggregationNode::Step step, TypePtr resultType)
      : isRawInput_{isRawInput(step)}, resultType_(resultType) {}

 public:
  virtual ~Aggregate() {}

  TypePtr resultType() const {
    return resultType_;
  }

  // Returns the fixed number of bytes the accumulator takes on a group
  // row. Variable width accumulators will reference the variable
  // width part of the state from the fixed part.
  virtual int32_t accumulatorFixedWidthSize() const = 0;

  void setAllocator(HashStringAllocator* allocator) {
    allocator_ = allocator;
  }

  // Sets the offset and null indicator position of 'this'.
  // @param offset Offset in bytes from the start of the row of the accumulator
  // @param nullByte Offset in bytes from the start of the row of the null flag
  // @param nullMask The specific bit in the nullByte that stores the null flag
  void setOffsets(int32_t offset, int32_t nullByte, uint8_t nullMask) {
    nullByte_ = nullByte;
    nullMask_ = nullMask;
    offset_ = offset;
  }

  // Initializes null flags and accumulators for newly encountered groups.
  // @param groups Pointers to the start of the new group rows.
  // @param indices Indices into 'groups' of the new entries.
  virtual void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) = 0;

  // Updates the accumulator in 'groups' with the values in 'args'.
  // @param groups Pointers to the start of the group rows. These are aligned
  // with the 'args', e.g. data in the i-th row of the 'args' goes to the i-th
  // group. The groups may repeat if different rows go into the same group.
  // @param rows Rows of the 'args' to add to the accumulators. These may not be
  // contiguous if the aggregation is configured to drop null grouping keys.
  // This would be the case when aggregation is followed by the join on the
  // grouping keys.
  // @param args Data to add to the accumulators.
  // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
  // The pushdown can happen only if this flag is true and 'args' is a single
  // LazyVector.
  void update(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) {
    isRawInput_ ? updatePartial(groups, rows, args, mayPushdown)
                : updateFinal(groups, rows, args, mayPushdown);
  }

  // Updates the single accumulator used for global aggregation.
  // @param group Pointer to the start of the group row.
  // @param allRows A contiguous range of row numbers starting from 0.
  // @param args Data to add to the accumulators.
  // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
  // The pushdown can happen only if this flag is true and 'args' is a single
  // LazyVector.
  void updateSingleGroup(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) {
    isRawInput_ ? updateSingleGroupPartial(group, allRows, args, mayPushdown)
                : updateSingleGroupFinal(group, allRows, args, mayPushdown);
  }

  // Finalizes the state in groups. Defaults to no op for cases like
  // sum and max.
  virtual void finalize(char** groups, int32_t numGroups) = 0;

  // Extracts final results (used for final and single aggregations).
  // @param groups Pointers to the start of the group rows.
  // @param numGroups Number of groups to extract results from.
  // @param result The result vector to store the results in.
  virtual void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) = 0;

  // Extracts partial results (used for partial and intermediate aggregations).
  // @param groups Pointers to the start of the group rows.
  // @param numGroups Number of groups to extract results from.
  // @param result The result vector to store the results in.
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

 protected:
  // Updates partial accumulators from raw input data.
  // @param args Raw input.
  virtual void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Updates final accumulators from intermediate results.
  // @param args Intermediate results produced by extractAccumulators().
  virtual void updateFinal(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Updates partial accumulators from raw input data for global aggregation.
  // @param args Raw input to add to the accumulators.
  virtual void updateSingleGroupPartial(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Updates final accumulators from intermediate results for global
  // aggregation.
  // @param args Intermediate results produced by extractAccumulators().
  virtual void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  bool isNull(char* group) const {
    return numNulls_ && (group[nullByte_] & nullMask_);
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
      bits::clearBit(rawNulls, index);
    }
  }

  const bool isRawInput_;
  const TypePtr resultType_;

  // Byte position of null flag in group row.
  int32_t nullByte_;
  uint8_t nullMask_;
  // Offset of fixed length accumulator state in group row.
  int32_t offset_;

  // Number of null accumulators in the current state of the aggregation
  // operator for this aggregate. If 0, clearing the null as part of update
  // is not needed.
  uint64_t numNulls_ = 0;
  HashStringAllocator* allocator_;
};

using AggregateFunctionRegistry = Registry<
    std::string,
    std::unique_ptr<Aggregate>(
        core::AggregationNode::Step step,
        const std::vector<TypePtr>& argTypes,
        const TypePtr& resultType)>;

AggregateFunctionRegistry& AggregateFunctions();
} // namespace facebook::velox::exec
