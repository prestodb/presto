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
#include "velox/exec/ContainerRowSerde.h"
#include "velox/functions/lib/CheckNestedNulls.h"
#include "velox/functions/lib/aggregates/SingleValueAccumulator.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::aggregate {

template <typename T>
constexpr bool isNumeric() {
  return std::is_same_v<T, bool> || std::is_same_v<T, int8_t> ||
      std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> ||
      std::is_same_v<T, int64_t> || std::is_same_v<T, float> ||
      std::is_same_v<T, double> || std::is_same_v<T, Timestamp>;
}

template <typename T>
void extract(
    T* accumulator,
    const VectorPtr& vector,
    vector_size_t index,
    T* rawValues,
    uint64_t* rawBoolValues) {
  if constexpr (std::is_same_v<T, bool>) {
    bits::setBit(rawBoolValues, index, *accumulator);
  } else {
    rawValues[index] = *accumulator;
  }
}

template <typename T>
void extract(
    SingleValueAccumulator* accumulator,
    const VectorPtr& vector,
    vector_size_t index,
    T* /* rawValues */,
    uint64_t* /* rawBoolValues */) {
  accumulator->read(vector, index);
}

template <typename T>
void store(
    T* accumulator,
    const DecodedVector& decodedVector,
    vector_size_t index,
    HashStringAllocator* allocator) {
  *accumulator = decodedVector.valueAt<T>(index);
}

template <typename T>
void store(
    SingleValueAccumulator* accumulator,
    const DecodedVector& decodedVector,
    vector_size_t index,
    HashStringAllocator* allocator) {
  accumulator->write(
      decodedVector.base(), decodedVector.index(index), allocator);
}

template <typename T, bool useCustomComparison = false, typename = void>
struct AccumulatorTypeTraits {};

template <typename T, bool useCustomComparison>
struct AccumulatorTypeTraits<
    T,
    useCustomComparison,
    std::enable_if_t<isNumeric<T>() && !useCustomComparison, void>> {
  using AccumulatorType = T;
};

template <typename T, bool useCustomComparison>
struct AccumulatorTypeTraits<
    T,
    useCustomComparison,
    std::enable_if_t<!isNumeric<T>() || useCustomComparison, void>> {
  using AccumulatorType = SingleValueAccumulator;
};

/// MinMaxByAggregateBase is the base class for min_by and max_by functions
/// with numeric value and comparison types. These functions return the value of
/// X associated with the minimum/maximum value of Y over all input values.
/// Partial aggregation produces a pair of X and min/max Y. Final aggregation
/// takes a pair of X and min/max Y and returns X. T is the type of X and U is
/// the type of Y.
template <
    typename T,
    typename U,
    bool isMaxFunc,
    class Comparator,
    bool compareTypeUsesCustomComparison>
class MinMaxByAggregateBase : public exec::Aggregate {
 public:
  using ValueAccumulatorType =
      typename AccumulatorTypeTraits<T>::AccumulatorType;
  using ComparisonAccumulatorType =
      typename AccumulatorTypeTraits<U, compareTypeUsesCustomComparison>::
          AccumulatorType;

  explicit MinMaxByAggregateBase(
      TypePtr resultType,
      bool throwOnNestedNulls = false)
      : exec::Aggregate(resultType), throwOnNestedNulls_(throwOnNestedNulls) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ValueAccumulatorType) + sizeof(ComparisonAccumulatorType) +
        sizeof(bool);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    addRawInput(
        groups,
        rows,
        args,
        [&](auto* accumulator,
            const auto& newComparisons,
            auto index,
            auto isFirstValue) {
          return Comparator::
              template compare<isMaxFunc, U, ComparisonAccumulatorType>(
                  accumulator, newComparisons, index, isFirstValue);
        });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addIntermediateResults(
        groups,
        rows,
        args,
        [&](auto* accumulator,
            const auto& newComparisons,
            auto index,
            auto isFirstValue) {
          return Comparator::
              template compare<isMaxFunc, U, ComparisonAccumulatorType>(
                  accumulator, newComparisons, index, isFirstValue);
        });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    addSingleGroupRawInput(
        group,
        rows,
        args,
        [&](auto* accumulator,
            const auto& newComparisons,
            auto index,
            auto isFirstValue) {
          return Comparator::
              template compare<isMaxFunc, U, ComparisonAccumulatorType>(
                  accumulator, newComparisons, index, isFirstValue);
        });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addSingleGroupIntermediateResults(
        group,
        rows,
        args,
        [&](auto* accumulator,
            const auto& newComparisons,
            auto index,
            auto isFirstValue) {
          return Comparator::
              template compare<isMaxFunc, U, ComparisonAccumulatorType>(
                  accumulator, newComparisons, index, isFirstValue);
        });
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    (*result)->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(result->get());

    T* rawValues = nullptr;
    uint64_t* rawBoolValues = nullptr;
    if constexpr (!std::
                      is_same_v<ValueAccumulatorType, SingleValueAccumulator>) {
      auto vector = (*result)->as<FlatVector<T>>();
      VELOX_CHECK(vector != nullptr);
      if constexpr (std::is_same_v<T, bool>) {
        rawBoolValues = vector->template mutableRawValues<uint64_t>();
      } else {
        rawValues = vector->mutableRawValues();
      }
    }

    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group) || valueIsNull(group)) {
        (*result)->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        extract<T>(value(group), *result, i, rawValues, rawBoolValues);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    rowVector->resize(numGroups);

    auto valueVector = rowVector->childAt(0);
    auto comparisonVector = rowVector->childAt(1);
    uint64_t* rawNulls = getRawNulls(rowVector);

    T* rawValues = nullptr;
    uint64_t* rawBoolValues = nullptr;
    if constexpr (!std::
                      is_same_v<ValueAccumulatorType, SingleValueAccumulator>) {
      auto flatValueVector = valueVector->as<FlatVector<T>>();
      VELOX_CHECK(flatValueVector != nullptr);
      if constexpr (std::is_same_v<T, bool>) {
        rawBoolValues = flatValueVector->template mutableRawValues<uint64_t>();
      } else {
        rawValues = flatValueVector->mutableRawValues();
      }
    }
    U* rawComparisonValues = nullptr;
    uint64_t* rawBoolComparisonValues = nullptr;
    if constexpr (!std::is_same_v<
                      ComparisonAccumulatorType,
                      SingleValueAccumulator>) {
      auto flatComparisonVector = comparisonVector->as<FlatVector<U>>();
      VELOX_CHECK(flatComparisonVector != nullptr);
      if constexpr (std::is_same_v<U, bool>) {
        rawBoolComparisonValues =
            flatComparisonVector->template mutableRawValues<uint64_t>();
      } else {
        rawComparisonValues = flatComparisonVector->mutableRawValues();
      }
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
          extract<T>(value(group), valueVector, i, rawValues, rawBoolValues);
        }
        extract<U>(
            comparisonValue(group),
            comparisonVector,
            i,
            rawComparisonValues,
            rawBoolComparisonValues);
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

    const auto* indices = decodedComparison_.indices();
    if (decodedValue_.mayHaveNulls() || decodedComparison_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (checkNestedNulls(
                decodedComparison_, indices, i, throwOnNestedNulls_)) {
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
        if (throwOnNestedNulls_) {
          checkNestedNulls(decodedComparison_, indices, i, throwOnNestedNulls_);
        }
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
    const auto* indices = decodedComparison_.indices();

    if (decodedValue_.isConstantMapping() &&
        decodedComparison_.isConstantMapping()) {
      if (checkNestedNulls(
              decodedComparison_, indices, 0, throwOnNestedNulls_)) {
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
        if (checkNestedNulls(
                decodedComparison_, indices, i, throwOnNestedNulls_)) {
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
        if (throwOnNestedNulls_) {
          checkNestedNulls(decodedComparison_, indices, i, throwOnNestedNulls_);
        }
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

  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (const vector_size_t i : indices) {
      auto group = groups[i];
      valueIsNull(group) = true;

      if constexpr (std::is_same_v<
                        ValueAccumulatorType,
                        SingleValueAccumulator>) {
        new (group + offset_) SingleValueAccumulator();
      } else {
        *value(group) = ValueAccumulatorType();
      }

      if constexpr (!std::is_same_v<
                        ComparisonAccumulatorType,
                        SingleValueAccumulator>) {
        *comparisonValue(group) = ComparisonAccumulatorType();
      } else {
        new (group + offset_ + sizeof(ValueAccumulatorType))
            SingleValueAccumulator();
      }
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    for (auto group : groups) {
      if (isInitialized(group)) {
        if constexpr (std::is_same_v<
                          ValueAccumulatorType,
                          SingleValueAccumulator>) {
          value(group)->destroy(allocator_);
        }
        if constexpr (std::is_same_v<
                          ComparisonAccumulatorType,
                          SingleValueAccumulator>) {
          comparisonValue(group)->destroy(allocator_);
        }
      }
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
    auto isFirstValue = isNull(group);
    clearNull(group);
    if (mayUpdate(
            comparisonValue(group), decodedComparisons, index, isFirstValue)) {
      valueIsNull(group) = isValueNull;
      if (LIKELY(!isValueNull)) {
        store<T>(value(group), decodedValues, index, allocator_);
      }
      store<U>(comparisonValue(group), decodedComparisons, index, allocator_);
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

  const bool throwOnNestedNulls_;
  DecodedVector decodedValue_;
  DecodedVector decodedComparison_;
  DecodedVector decodedIntermediateResult_;
};

template <
    template <typename U, typename V, bool B1, class C, bool B2>
    class Aggregate,
    bool isMaxFunc,
    class Comparator,
    bool compareTypeUsesCustomComparison,
    typename W>
std::unique_ptr<exec::Aggregate> create(
    TypePtr resultType,
    TypePtr compareType,
    const std::string& errorMessage,
    bool throwOnNestedNulls = false) {
  switch (compareType->kind()) {
    case TypeKind::BOOLEAN:
      return std::make_unique<Aggregate<
          W,
          bool,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    case TypeKind::TINYINT:
      return std::make_unique<Aggregate<
          W,
          int8_t,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    case TypeKind::SMALLINT:
      return std::make_unique<Aggregate<
          W,
          int16_t,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    case TypeKind::INTEGER:
      return std::make_unique<Aggregate<
          W,
          int32_t,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<Aggregate<
          W,
          int64_t,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<Aggregate<
          W,
          float,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<Aggregate<
          W,
          double,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    case TypeKind::HUGEINT:
      return std::make_unique<Aggregate<
          W,
          int128_t,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    case TypeKind::VARBINARY:
      [[fallthrough]];
    case TypeKind::VARCHAR:
      return std::make_unique<Aggregate<
          W,
          StringView,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    case TypeKind::TIMESTAMP:
      return std::make_unique<Aggregate<
          W,
          Timestamp,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    case TypeKind::ARRAY:
      [[fallthrough]];
    case TypeKind::MAP:
      [[fallthrough]];
    case TypeKind::ROW:
      return std::make_unique<Aggregate<
          W,
          ComplexType,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType, throwOnNestedNulls);
    case TypeKind::UNKNOWN:
      return std::make_unique<Aggregate<
          W,
          UnknownValue,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison>>(resultType);
    default:
      VELOX_FAIL("{}", errorMessage);
      return nullptr;
  }
}

template <
    template <typename U, typename V, bool B1, class C, bool B2>
    class Aggregate,
    class Comparator,
    bool isMaxFunc,
    bool compareTypeUsesCustomComparison = false>
std::unique_ptr<exec::Aggregate> create(
    TypePtr resultType,
    TypePtr valueType,
    TypePtr compareType,
    const std::string& errorMessage,
    bool throwOnNestedNulls = false) {
  switch (valueType->kind()) {
    case TypeKind::BOOLEAN:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          bool>(resultType, compareType, errorMessage, throwOnNestedNulls);
    case TypeKind::TINYINT:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          int8_t>(resultType, compareType, errorMessage, throwOnNestedNulls);
    case TypeKind::SMALLINT:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          int16_t>(resultType, compareType, errorMessage, throwOnNestedNulls);
    case TypeKind::INTEGER:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          int32_t>(resultType, compareType, errorMessage, throwOnNestedNulls);
    case TypeKind::BIGINT:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          int64_t>(resultType, compareType, errorMessage, throwOnNestedNulls);
    case TypeKind::HUGEINT:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          int128_t>(resultType, compareType, errorMessage);
    case TypeKind::REAL:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          float>(resultType, compareType, errorMessage, throwOnNestedNulls);
    case TypeKind::DOUBLE:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          double>(resultType, compareType, errorMessage, throwOnNestedNulls);
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          StringView>(
          resultType, compareType, errorMessage, throwOnNestedNulls);
    case TypeKind::TIMESTAMP:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          Timestamp>(resultType, compareType, errorMessage, throwOnNestedNulls);
    case TypeKind::ARRAY:
      [[fallthrough]];
    case TypeKind::MAP:
      [[fallthrough]];
    case TypeKind::ROW:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          ComplexType>(
          resultType, compareType, errorMessage, throwOnNestedNulls);
    case TypeKind::UNKNOWN:
      return create<
          Aggregate,
          isMaxFunc,
          Comparator,
          compareTypeUsesCustomComparison,
          UnknownValue>(
          resultType, compareType, errorMessage, throwOnNestedNulls);
    default:
      VELOX_FAIL(errorMessage);
  }
}

} // namespace facebook::velox::functions::aggregate
