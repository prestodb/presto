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

#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/SingleValueAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {

void resizeRowVectorAndChildren(RowVector& rowVector, vector_size_t size) {
  rowVector.resize(size);
  for (auto& child : rowVector.children()) {
    child->resize(size);
  }
}

std::pair<vector_size_t*, vector_size_t*> rawOffsetAndSizes(
    ArrayVector& arrayVector) {
  return {
      arrayVector.offsets()->asMutable<vector_size_t>(),
      arrayVector.sizes()->asMutable<vector_size_t>()};
}

template <typename T>
constexpr bool isNumericOrDate() {
  return std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
      std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
      std::is_same_v<T, float> || std::is_same_v<T, double> ||
      std::is_same_v<T, Date>;
}

template <typename T, typename TAccumulator>
void extract(
    TAccumulator* accumulator,
    const VectorPtr& vector,
    vector_size_t index,
    T* rawValues) {
  if constexpr (isNumericOrDate<T>()) {
    rawValues[index] = *accumulator;
  } else {
    accumulator->read(vector, index);
  }
}

template <typename T, typename TAccumulator>
void store(
    TAccumulator* accumulator,
    const DecodedVector& decodedVector,
    vector_size_t index,
    HashStringAllocator* allocator) {
  if constexpr (isNumericOrDate<T>()) {
    *accumulator = decodedVector.valueAt<T>(index);
  } else {
    accumulator->write(
        decodedVector.base(), decodedVector.index(index), allocator);
  }
}

/// Returns true if the value in 'index' row of 'newComparisons' is strictly
/// greater than the value in the 'accumulator'.
template <typename T, typename TAccumulator>
bool greaterThan(
    TAccumulator* accumulator,
    const DecodedVector& newComparisons,
    vector_size_t index) {
  if constexpr (isNumericOrDate<T>()) {
    return newComparisons.valueAt<T>(index) > *accumulator;
  } else {
    // SingleValueAccumulator::compare has the semantics of accumulator value is
    // less than vector value.
    return !accumulator->hasValue() ||
        (accumulator->compare(newComparisons, index) < 0);
  }
}

/// Returns true if the value in 'index' row of 'newComparisons' is strictly
/// less than the value in the 'accumulator'.
template <typename T, typename TAccumulator>
bool lessThan(
    TAccumulator* accumulator,
    const DecodedVector& newComparisons,
    vector_size_t index) {
  if constexpr (isNumericOrDate<T>()) {
    return newComparisons.valueAt<T>(index) < *accumulator;
  } else {
    // SingleValueAccumulator::compare has the semantics of accumulator value is
    // greater than vector value.
    return !accumulator->hasValue() ||
        (accumulator->compare(newComparisons, index) > 0);
  }
}

template <typename T, typename = void>
struct AccumulatorTypeTraits {};

template <typename T>
struct AccumulatorTypeTraits<T, std::enable_if_t<isNumericOrDate<T>(), void>> {
  using AccumulatorType = T;
};

template <typename T>
struct AccumulatorTypeTraits<T, std::enable_if_t<!isNumericOrDate<T>(), void>> {
  using AccumulatorType = SingleValueAccumulator;
};

template <typename T>
struct MinMaxTrait : public std::numeric_limits<T> {};

template <>
struct MinMaxTrait<Date> {
  static constexpr Date lowest() {
    return Date(std::numeric_limits<int32_t>::min());
  }

  static constexpr Date max() {
    return Date(std::numeric_limits<int32_t>::max());
  }
};

template <>
struct MinMaxTrait<Timestamp> {
  static constexpr Timestamp lowest() {
    return Timestamp(std::numeric_limits<int64_t>::min(), 0);
  }

  static constexpr Timestamp max() {
    return Timestamp(std::numeric_limits<int64_t>::max(), 999'999);
  }
};

/// MinMaxByAggregate is the base class for min_by and max_by functions
/// with numeric value and comparison types. These functions return the value of
/// X associated with the minimum/maximum value of Y over all input values.
/// Partial aggregation produces a pair of X and min/max Y. Final aggregation
/// takes a pair of X and min/max Y and returns X. T is the type of X and U is
/// the type of Y.
template <typename T, typename U>
class MinMaxByAggregate : public exec::Aggregate {
 public:
  using ValueAccumulatorType =
      typename AccumulatorTypeTraits<T>::AccumulatorType;
  using ComparisonAccumulatorType =
      typename AccumulatorTypeTraits<U>::AccumulatorType;

  /// NOTE: the passed min/max limit is only meaningful if comparison type U is
  /// a numeric type.
  MinMaxByAggregate(TypePtr resultType, U initialValue)
      : exec::Aggregate(resultType), initialValue_(initialValue) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ValueAccumulatorType) + sizeof(ComparisonAccumulatorType) +
        sizeof(bool);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (const vector_size_t i : indices) {
      auto group = groups[i];
      valueIsNull(group) = true;

      if constexpr (!isNumericOrDate<T>()) {
        new (group + offset_) SingleValueAccumulator();
      }

      if constexpr (isNumericOrDate<U>()) {
        *comparisonValue(group) = initialValue_;
      } else {
        new (
            group + offset_ +
            (isNumericOrDate<T>() ? sizeof(T) : sizeof(ValueAccumulatorType)))
            SingleValueAccumulator();
      }
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    (*result)->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(result->get());

    T* rawValues = nullptr;
    if constexpr (isNumericOrDate<T>()) {
      auto vector = (*result)->as<FlatVector<T>>();
      VELOX_CHECK(vector != nullptr);
      rawValues = vector->mutableRawValues();
    }

    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group) || valueIsNull(group)) {
        (*result)->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        extract<T, ValueAccumulatorType>(value(group), *result, i, rawValues);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    auto valueVector = rowVector->childAt(0);
    auto comparisonVector = rowVector->childAt(1);

    resizeRowVectorAndChildren(*rowVector, numGroups);
    uint64_t* rawNulls = getRawNulls(rowVector);

    T* rawValues = nullptr;
    if constexpr (isNumericOrDate<T>()) {
      auto flatValueVector = valueVector->as<FlatVector<T>>();
      VELOX_CHECK(flatValueVector != nullptr);
      rawValues = flatValueVector->mutableRawValues();
    }
    U* rawComparisonValues = nullptr;
    if constexpr (isNumericOrDate<U>()) {
      auto flatComparisonVector = comparisonVector->as<FlatVector<U>>();
      VELOX_CHECK(flatComparisonVector != nullptr);
      rawComparisonValues = flatComparisonVector->mutableRawValues();
    }
    uint64_t* rawValueNulls =
        valueVector->mutableNulls(rowVector->size())->asMutable<uint64_t>();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rowVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        const bool isValueNull = valueIsNull(group);
        bits::setNull(rawValueNulls, i, isValueNull);
        if (LIKELY(!isValueNull)) {
          extract<T, ValueAccumulatorType>(
              value(group), valueVector, i, rawValues);
        }
        extract<U, ComparisonAccumulatorType>(
            comparisonValue(group), comparisonVector, i, rawComparisonValues);
      }
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      if constexpr (!isNumericOrDate<T>()) {
        value(group)->destroy(allocator_);
      }
      if constexpr (!isNumericOrDate<U>()) {
        comparisonValue(group)->destroy(allocator_);
      }
    }
  }

 protected:
  template <typename MayUpdate>
  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      MayUpdate mayUpdate) {
    // decodedValue contains the values of column X. decodedComparisonValue
    // contains the values of column Y which is used to select the minimum or
    // the maximum.
    decodedValue_.decode(*args[0], rows);
    decodedComparison_.decode(*args[1], rows);

    if (decodedValue_.isConstantMapping() &&
        decodedComparison_.isConstantMapping() &&
        decodedComparison_.isNullAt(0)) {
      return;
    }
    if (decodedValue_.mayHaveNulls() || decodedComparison_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedComparison_.isNullAt(i)) {
          return;
        }
        updateValues(
            groups[i],
            decodedValue_,
            decodedComparison_,
            i,
            decodedValue_.isNullAt(i),
            mayUpdate);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateValues(
            groups[i], decodedValue_, decodedComparison_, i, false, mayUpdate);
      });
    }
  }

  template <typename MayUpdate>
  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      MayUpdate mayUpdate) {
    decodedIntermediateResult_.decode(*args[0], rows);
    auto baseRowVector =
        dynamic_cast<const RowVector*>(decodedIntermediateResult_.base());

    decodedValue_.decode(*baseRowVector->childAt(0), rows);
    decodedComparison_.decode(*baseRowVector->childAt(1), rows);

    if (decodedIntermediateResult_.isConstantMapping() &&
        decodedIntermediateResult_.isNullAt(0)) {
      return;
    }
    if (decodedIntermediateResult_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedIntermediateResult_.isNullAt(i)) {
          return;
        }
        const auto decodedIndex = decodedIntermediateResult_.index(i);
        updateValues(
            groups[i],
            decodedValue_,
            decodedComparison_,
            decodedIndex,
            decodedValue_.isNullAt(decodedIndex),
            mayUpdate);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        const auto decodedIndex = decodedIntermediateResult_.index(i);
        updateValues(
            groups[i],
            decodedValue_,
            decodedComparison_,
            decodedIndex,
            decodedValue_.isNullAt(decodedIndex),
            mayUpdate);
      });
    }
  }

  template <typename MayUpdate>
  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      MayUpdate mayUpdate) {
    // decodedValue contains the values of column X. decodedComparisonValue
    // contains the values of column Y which is used to select the minimum or
    // the maximum.
    decodedValue_.decode(*args[0], rows);
    decodedComparison_.decode(*args[1], rows);
    if (decodedValue_.isConstantMapping() &&
        decodedComparison_.isConstantMapping()) {
      if (decodedComparison_.isNullAt(0)) {
        return;
      }
      updateValues(
          group,
          decodedValue_,
          decodedComparison_,
          0,
          decodedValue_.isNullAt(0),
          mayUpdate);
    } else if (
        decodedValue_.mayHaveNulls() || decodedComparison_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedComparison_.isNullAt(i)) {
          return;
        }
        updateValues(
            group,
            decodedValue_,
            decodedComparison_,
            i,
            decodedValue_.isNullAt(i),
            mayUpdate);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateValues(
            group, decodedValue_, decodedComparison_, i, false, mayUpdate);
      });
    }
  }

  /// Final aggregation takes (value, comparisonValue) structs as inputs. It
  /// produces the Value associated with the maximum/minimum of comparisonValue
  /// over all structs.
  template <typename MayUpdate>
  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      MayUpdate mayUpdate) {
    // Decode struct(Value, ComparisonValue) as individual vectors.
    decodedIntermediateResult_.decode(*args[0], rows);
    auto baseRowVector =
        dynamic_cast<const RowVector*>(decodedIntermediateResult_.base());

    decodedValue_.decode(*baseRowVector->childAt(0), rows);
    decodedComparison_.decode(*baseRowVector->childAt(1), rows);

    if (decodedIntermediateResult_.isConstantMapping()) {
      if (decodedIntermediateResult_.isNullAt(0)) {
        return;
      }
      const auto decodedIndex = decodedIntermediateResult_.index(0);
      updateValues(
          group,
          decodedValue_,
          decodedComparison_,
          decodedIndex,
          decodedValue_.isNullAt(decodedIndex),
          mayUpdate);
    } else if (decodedIntermediateResult_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedIntermediateResult_.isNullAt(i)) {
          return;
        }
        const auto decodedIndex = decodedIntermediateResult_.index(i);
        updateValues(
            group,
            decodedValue_,
            decodedComparison_,
            decodedIndex,
            decodedValue_.isNullAt(decodedIndex),
            mayUpdate);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        const auto decodedIndex = decodedIntermediateResult_.index(i);
        updateValues(
            group,
            decodedValue_,
            decodedComparison_,
            decodedIndex,
            decodedValue_.isNullAt(decodedIndex),
            mayUpdate);
      });
    }
  }

 private:
  template <typename MayUpdate>
  inline void updateValues(
      char* group,
      const DecodedVector& decodedValues,
      const DecodedVector& decodedComparisons,
      vector_size_t index,
      bool isValueNull,
      MayUpdate mayUpdate) {
    clearNull(group);
    if (mayUpdate(comparisonValue(group), decodedComparisons, index)) {
      valueIsNull(group) = isValueNull;
      if (LIKELY(!isValueNull)) {
        store<T, ValueAccumulatorType>(
            value(group), decodedValues, index, allocator_);
      }
      store<U, ComparisonAccumulatorType>(
          comparisonValue(group), decodedComparisons, index, allocator_);
    }
  }

  inline ValueAccumulatorType* value(char* group) {
    return reinterpret_cast<ValueAccumulatorType*>(group + Aggregate::offset_);
  }

  inline ComparisonAccumulatorType* comparisonValue(char* group) {
    return reinterpret_cast<ComparisonAccumulatorType*>(
        group + Aggregate::offset_ + sizeof(ValueAccumulatorType));
  }

  inline bool& valueIsNull(char* group) {
    return *reinterpret_cast<bool*>(
        group + Aggregate::offset_ + sizeof(ValueAccumulatorType) +
        sizeof(ComparisonAccumulatorType));
  }

  /// Initial value takes the minimum and maximum values of the numerical
  /// limits. This is only meaningful if the comparison value is a numeric type.
  const U initialValue_;
  DecodedVector decodedValue_;
  DecodedVector decodedComparison_;
  DecodedVector decodedIntermediateResult_;
};

template <typename T, typename U>
class MaxByAggregate : public MinMaxByAggregate<T, U> {
 public:
  using ComparisonAccumulatorType =
      typename AccumulatorTypeTraits<U>::AccumulatorType;

  explicit MaxByAggregate(TypePtr resultType)
      : MinMaxByAggregate<T, U>(resultType, MinMaxTrait<U>::lowest()) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    MinMaxByAggregate<T, U>::addRawInput(
        groups,
        rows,
        args,
        [&](auto* accumulator, const auto& newComparisons, auto index) {
          return greaterThan<U, ComparisonAccumulatorType>(
              accumulator, newComparisons, index);
        });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    MinMaxByAggregate<T, U>::addIntermediateResults(
        groups,
        rows,
        args,
        [&](auto* accumulator, const auto& newComparisons, auto index) {
          return greaterThan<U, ComparisonAccumulatorType>(
              accumulator, newComparisons, index);
        });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    MinMaxByAggregate<T, U>::addSingleGroupRawInput(
        group,
        rows,
        args,
        [&](auto* accumulator, const auto& newComparisons, auto index) {
          return greaterThan<U, ComparisonAccumulatorType>(
              accumulator, newComparisons, index);
        });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    MinMaxByAggregate<T, U>::addSingleGroupIntermediateResults(
        group,
        rows,
        args,
        [&](auto* accumulator, const auto& newComparisons, auto index) {
          return greaterThan<U, ComparisonAccumulatorType>(
              accumulator, newComparisons, index);
        });
  }
};

template <typename T, typename U>
class MinByAggregate : public MinMaxByAggregate<T, U> {
 public:
  using ComparisonAccumulatorType =
      typename AccumulatorTypeTraits<U>::AccumulatorType;

  explicit MinByAggregate(TypePtr resultType)
      : MinMaxByAggregate<T, U>(resultType, MinMaxTrait<U>::max()) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    MinMaxByAggregate<T, U>::addRawInput(
        groups,
        rows,
        args,
        [&](auto* accumulator, const auto& newComparisons, auto index) {
          return lessThan<U, ComparisonAccumulatorType>(
              accumulator, newComparisons, index);
        });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    MinMaxByAggregate<T, U>::addIntermediateResults(
        groups,
        rows,
        args,
        [&](auto* accumulator, const auto& newComparisons, auto index) {
          return lessThan<U, ComparisonAccumulatorType>(
              accumulator, newComparisons, index);
        });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    MinMaxByAggregate<T, U>::addSingleGroupRawInput(
        group,
        rows,
        args,
        [&](auto* accumulator, const auto& newComparisons, auto index) {
          return lessThan<U, ComparisonAccumulatorType>(
              accumulator, newComparisons, index);
        });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    MinMaxByAggregate<T, U>::addSingleGroupIntermediateResults(
        group,
        rows,
        args,
        [&](auto* accumulator, const auto& newComparisons, auto index) {
          return lessThan<U, ComparisonAccumulatorType>(
              accumulator, newComparisons, index);
        });
  }
};

/// @tparam V Type of value.
/// @tparam C Type of compare.
/// @tparam Compare Type of comparator of std::pair<C, std::optional<V>>.
template <typename V, typename C, typename Compare>
struct MinMaxByNAccumulator {
  int64_t n{0};

  using Pair = std::pair<C, std::optional<V>>;
  std::priority_queue<Pair, std::vector<Pair, StlAllocator<Pair>>, Compare>
      topPairs;

  explicit MinMaxByNAccumulator(HashStringAllocator* allocator)
      : topPairs{Compare{}, StlAllocator<Pair>(allocator)} {}

  void
  compareAndAdd(C comparison, std::optional<V> value, Compare& comparator) {
    if (topPairs.size() < n) {
      topPairs.push({comparison, value});
    } else {
      const auto& topPair = topPairs.top();
      if (comparator.compare(comparison, topPair)) {
        topPairs.pop();
        topPairs.push({comparison, value});
      }
    }
  }

  /// Moves all values from 'topPairs' into 'rawValues' and 'rawValueNulls'
  /// buffers. The queue of 'topPairs' will be empty after this call.
  void
  extractValues(V* rawValues, uint64_t* rawValueNulls, vector_size_t offset) {
    const vector_size_t size = topPairs.size();
    for (auto i = size - 1; i >= 0; --i) {
      const auto& topPair = topPairs.top();
      const auto index = offset + i;

      const bool valueIsNull = !topPair.second.has_value();
      bits::setNull(rawValueNulls, index, valueIsNull);
      if (!valueIsNull) {
        rawValues[index] = topPair.second.value();
      }

      topPairs.pop();
    }
  }

  /// Moves all pairs of (comparison, value) from 'topPairs' into
  /// 'rawComparisons', 'rawValues' and 'rawValueNulls' buffers. The queue of
  /// 'topPairs' will be empty after this call.
  void extractPairs(
      C* rawComparisons,
      V* rawValues,
      uint64_t* rawValueNulls,
      vector_size_t offset) {
    const vector_size_t size = topPairs.size();
    for (auto i = size - 1; i >= 0; --i) {
      const auto& topPair = topPairs.top();
      const auto index = offset + i;

      rawComparisons[index] = topPair.first;

      const bool valueIsNull = !topPair.second.has_value();
      bits::setNull(rawValueNulls, index, valueIsNull);
      if (!valueIsNull) {
        rawValues[index] = topPair.second.value();
      }

      topPairs.pop();
    }
  }
};

template <typename V, typename C>
struct Less {
  using Pair = std::pair<C, std::optional<V>>;
  bool operator()(const Pair& lhs, const Pair& rhs) {
    return lhs.first < rhs.first;
  }

  bool compare(C lhs, const Pair& rhs) {
    return lhs < rhs.first;
  }
};

template <typename V, typename C>
struct Greater {
  using Pair = std::pair<C, std::optional<V>>;
  bool operator()(const Pair& lhs, const Pair& rhs) {
    return lhs.first > rhs.first;
  }

  bool compare(C lhs, const Pair& rhs) {
    return lhs > rhs.first;
  }
};

template <typename V, typename C, typename Compare>
class MinMaxByNAggregate : public exec::Aggregate {
 public:
  explicit MinMaxByNAggregate(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  using AccumulatorType = MinMaxByNAccumulator<V, C, Compare>;

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (const vector_size_t i : indices) {
      auto group = groups[i];
      new (group + offset_) AccumulatorType(allocator_);
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto valuesArray = (*result)->as<ArrayVector>();
    valuesArray->resize(numGroups);

    const auto numValues =
        countValuesAndSetResultNulls(groups, numGroups, *result);

    auto values = valuesArray->elements();
    values->resize(numValues);

    auto rawValues = values->as<FlatVector<V>>()->mutableRawValues();
    uint64_t* rawValueNulls = values->mutableRawNulls();

    auto [rawOffsets, rawSizes] = rawOffsetAndSizes(*valuesArray);

    vector_size_t offset = 0;
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];

      if (!isNull(group)) {
        auto* accumulator = value(group);
        const vector_size_t size = accumulator->topPairs.size();

        rawOffsets[i] = offset;
        rawSizes[i] = size;

        accumulator->extractValues(rawValues, rawValueNulls, offset);

        offset += size;
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    auto nVector = rowVector->childAt(0);
    auto valueArray = rowVector->childAt(1)->as<ArrayVector>();
    auto comparisonArray = rowVector->childAt(2)->as<ArrayVector>();

    resizeRowVectorAndChildren(*rowVector, numGroups);

    auto* rawNs = nVector->as<FlatVector<int64_t>>()->mutableRawValues();

    const auto numValues =
        countValuesAndSetResultNulls(groups, numGroups, *result);

    auto values = valueArray->elements();
    auto comparisons = comparisonArray->elements();

    values->resize(numValues);
    comparisons->resize(numValues);

    auto rawValues = values->as<FlatVector<V>>()->mutableRawValues();
    uint64_t* rawValueNulls = values->mutableRawNulls();
    auto rawComparisons = comparisons->as<FlatVector<C>>()->mutableRawValues();

    auto [rawValueOffsets, rawValueSizes] = rawOffsetAndSizes(*valueArray);
    auto [rawComparisonOffsets, rawComparisonSizes] =
        rawOffsetAndSizes(*comparisonArray);

    vector_size_t offset = 0;
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];

      if (!isNull(group)) {
        auto* accumulator = value(group);
        const auto size = accumulator->topPairs.size();

        rawNs[i] = accumulator->n;

        rawValueOffsets[i] = offset;
        rawValueSizes[i] = size;

        rawComparisonOffsets[i] = offset;
        rawComparisonSizes[i] = size;

        accumulator->extractPairs(
            rawComparisons, rawValues, rawValueNulls, offset);

        offset += size;
      }
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    decodedValue_.decode(*args[0], rows);
    decodedComparison_.decode(*args[1], rows);
    decodedN_.decode(*args[2], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decodedComparison_.isNullAt(i)) {
        return;
      }

      auto* group = groups[i];

      auto* accumulator = value(group);
      const auto n = validateN(decodedN_, i, accumulator->n);

      addRawInput(group, n, i);
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    auto results = decodeIntermediateResults(args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedIntermediates_.isNullAt(i)) {
        addIntermediateResults(groups[i], i, results);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    decodedValue_.decode(*args[0], rows);
    decodedComparison_.decode(*args[1], rows);

    auto* accumulator = value(group);
    const auto n = extractN(args[2], rows, accumulator->n);

    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedComparison_.isNullAt(i)) {
        addRawInput(group, n, i);
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    auto results = decodeIntermediateResults(args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedIntermediates_.isNullAt(i)) {
        addIntermediateResults(group, i, results);
      }
    });
  }

 private:
  inline AccumulatorType* value(char* group) {
    return reinterpret_cast<AccumulatorType*>(group + Aggregate::offset_);
  }

  static std::optional<V> optionalValue(
      const DecodedVector& decoded,
      vector_size_t index) {
    std::optional<V> value;
    if (!decoded.isNullAt(index)) {
      value = decoded.valueAt<V>(index);
    }

    return value;
  }

  static std::optional<V> optionalValue(
      const FlatVector<V>& vector,
      vector_size_t index) {
    std::optional<V> value;
    if (!vector.isNullAt(index)) {
      value = vector.valueAt(index);
    }

    return value;
  }

  void addRawInput(char* group, int64_t n, vector_size_t index) {
    clearNull(group);

    auto* accumulator = value(group);
    accumulator->n = n;

    const auto comparison = decodedComparison_.valueAt<C>(index);
    const auto value = optionalValue(decodedValue_, index);
    accumulator->compareAndAdd(comparison, value, comparator_);
  }

  struct IntermediateResult {
    const ArrayVector* valueArray;
    const FlatVector<V>* values;
    const ArrayVector* comparisonArray;
    const FlatVector<C>* comparisons;
  };

  void addIntermediateResults(
      char* group,
      vector_size_t index,
      const IntermediateResult& result) {
    clearNull(group);

    auto* accumulator = value(group);

    const auto decodedIndex = decodedIntermediates_.index(index);

    const auto n = validateN(decodedN_, decodedIndex, accumulator->n);
    accumulator->n = n;

    const auto* valueArray = result.valueArray;
    const auto* values = result.values;
    const auto* comparisonArray = result.comparisonArray;
    const auto* comparisons = result.comparisons;

    const auto numValues = valueArray->sizeAt(decodedIndex);
    const auto valueOffset = valueArray->offsetAt(decodedIndex);
    const auto comparisonOffset = comparisonArray->offsetAt(decodedIndex);
    for (auto i = 0; i < numValues; ++i) {
      const auto comparison = comparisons->valueAt(comparisonOffset + i);
      const auto value = optionalValue(*values, valueOffset + i);
      accumulator->compareAndAdd(comparison, value, comparator_);
    }
  }

  IntermediateResult decodeIntermediateResults(
      const VectorPtr& arg,
      const SelectivityVector& rows) {
    decodedIntermediates_.decode(*arg, rows);

    auto baseRowVector =
        dynamic_cast<const RowVector*>(decodedIntermediates_.base());

    decodedN_.decode(*baseRowVector->childAt(0), rows);
    decodedValue_.decode(*baseRowVector->childAt(1), rows);
    decodedComparison_.decode(*baseRowVector->childAt(2), rows);

    IntermediateResult result;
    result.valueArray = decodedValue_.base()->template as<ArrayVector>();
    result.comparisonArray =
        decodedComparison_.base()->template as<ArrayVector>();

    result.values = result.valueArray->elements()->template as<FlatVector<V>>();
    result.comparisons =
        result.comparisonArray->elements()->template as<FlatVector<C>>();

    return result;
  }

  /// Return total number of values in all accumulators of specified 'groups'.
  /// Set null flags in 'result'.
  vector_size_t countValuesAndSetResultNulls(
      char** groups,
      int32_t numGroups,
      VectorPtr& result) {
    vector_size_t numValues = 0;

    uint64_t* rawNulls = getRawNulls(result.get());

    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];
      auto* accumulator = value(group);

      if (isNull(group)) {
        result->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        numValues += accumulator->topPairs.size();
      }
    }

    return numValues;
  }

  int64_t
  validateN(DecodedVector& decodedN, vector_size_t row, int64_t currentN) {
    VELOX_USER_CHECK(
        !decodedN.isNullAt(row),
        "third argument of max_by/min_by must be a positive integer");
    const auto n = decodedN.valueAt<int64_t>(row);
    VELOX_USER_CHECK_GT(
        n, 0, "third argument of max_by/min_by must be a positive integer");

    if (currentN) {
      VELOX_USER_CHECK_EQ(
          n,
          currentN,
          "third argument of max_by/min_by must be a constant for all rows in a group");
    }
    return n;
  }

  int64_t extractN(
      const VectorPtr& arg,
      const SelectivityVector& rows,
      int64_t currentN) {
    decodedN_.decode(*arg, rows);
    if (decodedN_.isConstantMapping()) {
      return validateN(decodedN_, rows.begin(), currentN);
    }

    const auto n = validateN(decodedN_, rows.begin(), currentN);
    rows.applyToSelected([&](auto row) { validateN(decodedN_, row, n); });
    return n;
  }

  Compare comparator_;
  DecodedVector decodedValue_;
  DecodedVector decodedComparison_;
  DecodedVector decodedN_;
  DecodedVector decodedIntermediates_;
};

template <typename C, typename V>
class MinByNAggregate : public MinMaxByNAggregate<C, V, Less<C, V>> {
 public:
  explicit MinByNAggregate(TypePtr resultType)
      : MinMaxByNAggregate<C, V, Less<C, V>>(resultType) {}
};

template <typename C, typename V>
class MaxByNAggregate : public MinMaxByNAggregate<C, V, Greater<C, V>> {
 public:
  explicit MaxByNAggregate(TypePtr resultType)
      : MinMaxByNAggregate<C, V, Greater<C, V>>(resultType) {}
};

template <template <typename U, typename V> class Aggregate, typename W>
std::unique_ptr<exec::Aggregate> create(
    TypePtr resultType,
    TypePtr compareType,
    const std::string& errorMessage) {
  switch (compareType->kind()) {
    case TypeKind::TINYINT:
      return std::make_unique<Aggregate<W, int8_t>>(resultType);
    case TypeKind::SMALLINT:
      return std::make_unique<Aggregate<W, int16_t>>(resultType);
    case TypeKind::INTEGER:
      return std::make_unique<Aggregate<W, int32_t>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<Aggregate<W, int64_t>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<Aggregate<W, float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<Aggregate<W, double>>(resultType);
    case TypeKind::VARCHAR:
      return std::make_unique<Aggregate<W, StringView>>(resultType);
    case TypeKind::DATE:
      return std::make_unique<Aggregate<W, Date>>(resultType);
    case TypeKind::TIMESTAMP:
      return std::make_unique<Aggregate<W, Timestamp>>(resultType);
    default:
      VELOX_FAIL("{}", errorMessage);
      return nullptr;
  }
}

template <template <typename U, typename V> class Aggregate>
std::unique_ptr<exec::Aggregate> create(
    TypePtr resultType,
    TypePtr valueType,
    TypePtr compareType,
    const std::string& errorMessage) {
  switch (valueType->kind()) {
    case TypeKind::BOOLEAN:
      return create<Aggregate, bool>(resultType, compareType, errorMessage);
    case TypeKind::TINYINT:
      return create<Aggregate, int8_t>(resultType, compareType, errorMessage);
    case TypeKind::SMALLINT:
      return create<Aggregate, int16_t>(resultType, compareType, errorMessage);
    case TypeKind::INTEGER:
      return create<Aggregate, int32_t>(resultType, compareType, errorMessage);
    case TypeKind::BIGINT:
      return create<Aggregate, int64_t>(resultType, compareType, errorMessage);
    case TypeKind::REAL:
      return create<Aggregate, float>(resultType, compareType, errorMessage);
    case TypeKind::DOUBLE:
      return create<Aggregate, double>(resultType, compareType, errorMessage);
    case TypeKind::VARCHAR:
      return create<Aggregate, StringView>(
          resultType, compareType, errorMessage);
    case TypeKind::DATE:
      return create<Aggregate, Date>(resultType, compareType, errorMessage);
    case TypeKind::TIMESTAMP:
      return create<Aggregate, Timestamp>(
          resultType, compareType, errorMessage);
    case TypeKind::ARRAY:
      FOLLY_FALLTHROUGH;
    case TypeKind::MAP:
      FOLLY_FALLTHROUGH;
    case TypeKind::ROW:
      return create<Aggregate, ComplexType>(
          resultType, compareType, errorMessage);
    default:
      VELOX_FAIL(errorMessage);
  }
}

std::string toString(const std::vector<TypePtr>& types) {
  std::ostringstream out;
  for (auto i = 0; i < types.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << types[i]->toString();
  }
  return out.str();
}

template <
    template <typename U, typename V>
    class Aggregate,
    template <typename U, typename V>
    class NAggregate>
exec::AggregateRegistrationResult registerMinMaxBy(const std::string& name) {
  // TODO Add support for boolean 'compare' types.
  const std::vector<std::string> supportedCompareTypes = {
      "tinyint",
      "smallint",
      "integer",
      "bigint",
      "real",
      "double",
      "varchar",
      "date",
      "timestamp"};

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& compareType : supportedCompareTypes) {
    // V, C -> V.
    signatures.push_back(
        exec::AggregateFunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("T")
            .intermediateType(fmt::format("row(T,{})", compareType))
            .argumentType("T")
            .argumentType(compareType)
            .build());
  }

  // Add support for all value types to 3-arg version of the aggregate.
  for (const auto& compareType : supportedCompareTypes) {
    for (const auto& valueType : supportedCompareTypes) {
      // V, C, bigint -> array(V).
      signatures.push_back(
          exec::AggregateFunctionSignatureBuilder()
              .returnType(fmt::format("array({})", valueType))
              .intermediateType(fmt::format(
                  "row(bigint,array({}),array({}))", valueType, compareType))
              .argumentType(valueType)
              .argumentType(compareType)
              .argumentType("bigint")
              .build());
    }
  }

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        const auto isRawInput = exec::isRawInput(step);
        const std::string errorMessage = fmt::format(
            "Unknown input types for {} ({}) aggregation: {}",
            name,
            mapAggregationStepToName(step),
            toString(argTypes));

        const bool nAgg = (argTypes.size() == 3) ||
            (argTypes.size() == 1 && argTypes[0]->size() == 3);

        if (nAgg) {
          if (isRawInput) {
            // Input is: V, C, BIGINT.
            return create<NAggregate>(
                resultType, argTypes[0], argTypes[1], errorMessage);
          } else {
            // Input is: ROW(BIGINT, ARRAY(V), ARRAY(C)).
            const auto& rowType = argTypes[0];
            const auto& valueType = rowType->childAt(1)->childAt(0);
            const auto& compareType = rowType->childAt(2)->childAt(0);
            return create<NAggregate>(
                resultType, valueType, compareType, errorMessage);
          }
        } else {
          if (isRawInput) {
            // Input is: V, C.
            return create<Aggregate>(
                resultType, argTypes[0], argTypes[1], errorMessage);
          } else {
            // Input is: ROW(V, C).
            const auto& rowType = argTypes[0];
            const auto& valueType = rowType->childAt(0);
            const auto& compareType = rowType->childAt(1);
            return create<Aggregate>(
                resultType, valueType, compareType, errorMessage);
          }
        }
      });
}

} // namespace

void registerMinMaxByAggregates(const std::string& prefix) {
  registerMinMaxBy<MaxByAggregate, MaxByNAggregate>(prefix + kMaxBy);
  registerMinMaxBy<MinByAggregate, MinByNAggregate>(prefix + kMinBy);
}

} // namespace facebook::velox::aggregate::prestosql
