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
#include "velox/exec/SetAccumulator.h"
#include "velox/functions/lib/CheckNestedNulls.h"

namespace facebook::velox::functions::aggregate {

/// @tparam ignoreNulls Whether null inputs are ignored.
/// @tparam nullForEmpty When true, nulls are returned for empty groups.
/// Otherwise, empty arrays.
template <typename T, bool ignoreNulls = false, bool nullForEmpty = true>
class SetBaseAggregate : public exec::Aggregate {
 public:
  explicit SetBaseAggregate(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  using AccumulatorType = velox::aggregate::prestosql::SetAccumulator<T>;

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  bool isFixedSize() const override {
    return false;
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto arrayVector = (*result)->as<ArrayVector>();
    arrayVector->resize(numGroups);

    auto* rawOffsets = arrayVector->offsets()->asMutable<vector_size_t>();
    auto* rawSizes = arrayVector->sizes()->asMutable<vector_size_t>();

    vector_size_t numValues = 0;
    uint64_t* rawNulls = getRawNulls(arrayVector);
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];
      if (isNull(group)) {
        if constexpr (nullForEmpty) {
          arrayVector->setNull(i, true);
        } else {
          // If the group's accumulator is null, the corresponding result is an
          // empty array.
          clearNull(rawNulls, i);
          rawOffsets[i] = 0;
          rawSizes[i] = 0;
        }
      } else {
        clearNull(rawNulls, i);

        const auto size = value(group)->size();

        rawOffsets[i] = numValues;
        rawSizes[i] = size;

        numValues += size;
      }
    }

    if constexpr (std::is_same_v<T, ComplexType>) {
      auto values = arrayVector->elements();
      values->resize(numValues);

      vector_size_t offset = 0;
      for (auto i = 0; i < numGroups; ++i) {
        auto* group = groups[i];
        if (!isNull(group)) {
          offset += value(group)->extractValues(*values, offset);
        }
      }
    } else {
      auto values = arrayVector->elements()->as<FlatVector<T>>();
      values->resize(numValues);

      vector_size_t offset = 0;
      for (auto i = 0; i < numGroups; ++i) {
        auto* group = groups[i];
        if (!isNull(group)) {
          offset += value(group)->extractValues(*values, offset);
        }
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    return extractValues(groups, numGroups, result);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addIntermediateResultsInt(groups, rows, args, false);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addSingleGroupIntermediateResultsInt(group, rows, args, false);
  }

 protected:
  inline AccumulatorType* value(char* group) {
    return reinterpret_cast<AccumulatorType*>(group + Aggregate::offset_);
  }

  void addIntermediateResultsInt(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool clearNullForAllInputs) {
    decoded_.decode(*args[0], rows);

    auto baseArray = decoded_.base()->template as<ArrayVector>();
    decodedElements_.decode(*baseArray->elements());

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded_.isNullAt(i)) {
        if (clearNullForAllInputs) {
          clearNull(groups[i]);
        }
        return;
      }

      auto* group = groups[i];
      clearNull(group);

      auto tracker = trackRowSize(group);

      auto decodedIndex = decoded_.index(i);
      if constexpr (ignoreNulls) {
        value(group)->addNonNullValues(
            *baseArray, decodedIndex, decodedElements_, allocator_);
      } else {
        value(group)->addValues(
            *baseArray, decodedIndex, decodedElements_, allocator_);
      }
    });
  }

  void addSingleGroupIntermediateResultsInt(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool clearNullForAllInputs) {
    decoded_.decode(*args[0], rows);

    auto baseArray = decoded_.base()->template as<ArrayVector>();

    decodedElements_.decode(*baseArray->elements());

    auto* accumulator = value(group);

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](vector_size_t i) {
      if (decoded_.isNullAt(i)) {
        if (clearNullForAllInputs) {
          clearNull(group);
        }
        return;
      }

      clearNull(group);

      auto decodedIndex = decoded_.index(i);
      if constexpr (ignoreNulls) {
        accumulator->addNonNullValues(
            *baseArray, decodedIndex, decodedElements_, allocator_);
      } else {
        accumulator->addValues(
            *baseArray, decodedIndex, decodedElements_, allocator_);
      }
    });
  }

  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    const auto& type = resultType()->childAt(0);
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) AccumulatorType(type, allocator_);
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    for (auto* group : groups) {
      if (isInitialized(group) && !isNull(group)) {
        value(group)->free(*allocator_);
      }
    }
  }

  DecodedVector decoded_;
  DecodedVector decodedElements_;
};

template <typename T, bool ignoreNulls = false, bool nullForEmpty = true>
class SetAggAggregate : public SetBaseAggregate<T, ignoreNulls, nullForEmpty> {
 public:
  explicit SetAggAggregate(
      const TypePtr& resultType,
      const bool throwOnNestedNulls = false)
      : SetBaseAggregate<T, ignoreNulls, nullForEmpty>(resultType),
        throwOnNestedNulls_(throwOnNestedNulls) {}

  using Base = SetBaseAggregate<T, ignoreNulls, nullForEmpty>;

  bool supportsToIntermediate() const override {
    return true;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    const auto& elements = args[0];

    if (throwOnNestedNulls_) {
      DecodedVector decodedElements(*elements, rows);
      auto indices = decodedElements.indices();
      rows.applyToSelected([&](vector_size_t i) {
        velox::functions::checkNestedNulls(
            decodedElements, indices, i, throwOnNestedNulls_);
      });
    }

    const auto numRows = rows.size();

    // Convert input to a single-entry array.

    // Set nulls for rows not present in 'rows'.
    auto* pool = Base::allocator_->pool();
    BufferPtr nulls = allocateNulls(numRows, pool);
    memcpy(
        nulls->asMutable<uint64_t>(),
        rows.asRange().bits(),
        bits::nbytes(numRows));

    // Set offsets to 0, 1, 2, 3...
    BufferPtr offsets = allocateOffsets(numRows, pool);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    std::iota(rawOffsets, rawOffsets + numRows, 0);

    // Set sizes to 1.
    BufferPtr sizes = allocateSizes(numRows, pool);
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    std::fill(rawSizes, rawSizes + numRows, 1);

    result = std::make_shared<ArrayVector>(
        pool,
        ARRAY(elements->type()),
        nulls,
        numRows,
        offsets,
        sizes,
        BaseVector::loadedVectorShared(elements));
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    Base::decoded_.decode(*args[0], rows);
    auto indices = Base::decoded_.indices();
    rows.applyToSelected([&](vector_size_t i) {
      auto* group = groups[i];
      Base::clearNull(group);

      if (throwOnNestedNulls_) {
        velox::functions::checkNestedNulls(
            Base::decoded_, indices, i, throwOnNestedNulls_);
      }

      auto tracker = Base::trackRowSize(group);
      if constexpr (ignoreNulls) {
        Base::value(group)->addNonNullValue(
            Base::decoded_, i, Base::allocator_);
      } else {
        Base::value(group)->addValue(Base::decoded_, i, Base::allocator_);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    Base::decoded_.decode(*args[0], rows);

    Base::clearNull(group);
    auto* accumulator = Base::value(group);

    auto tracker = Base::trackRowSize(group);
    auto indices = Base::decoded_.indices();
    rows.applyToSelected([&](vector_size_t i) {
      if (throwOnNestedNulls_) {
        velox::functions::checkNestedNulls(
            Base::decoded_, indices, i, throwOnNestedNulls_);
      }

      if constexpr (ignoreNulls) {
        accumulator->addNonNullValue(Base::decoded_, i, Base::allocator_);
      } else {
        accumulator->addValue(Base::decoded_, i, Base::allocator_);
      }
    });
  }

 private:
  const bool throwOnNestedNulls_;
};

} // namespace facebook::velox::functions::aggregate
