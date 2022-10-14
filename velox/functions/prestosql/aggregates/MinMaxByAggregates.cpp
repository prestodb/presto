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
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/SingleValueAccumulator.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
namespace facebook::velox::aggregate::prestosql {

namespace {

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

  void finalize(char** /* unused */, int32_t /* unused */) override {}

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

    rowVector->resize(numGroups);
    valueVector->resize(numGroups);
    comparisonVector->resize(numGroups);
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
    default:
      VELOX_FAIL("{}", errorMessage);
      return nullptr;
  }
}

template <template <typename U, typename V> class Aggregate>
bool registerMinMaxByAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& valueType :
       {"tinyint",
        "smallint",
        "integer",
        "bigint",
        "real",
        "double",
        "varchar",
        "date"}) {
    for (const auto& compareType :
         {"tinyint",
          "smallint",
          "integer",
          "bigint",
          "real",
          "double",
          "varchar",
          "date"}) {
      signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                               .returnType(valueType)
                               .intermediateType(fmt::format(
                                   "row({},{})", valueType, compareType))
                               .argumentType(valueType)
                               .argumentType(compareType)
                               .build());
    }
  }

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        auto isRawInput = exec::isRawInput(step);
        if (isRawInput) {
          VELOX_CHECK_EQ(
              argTypes.size(),
              2,
              "{} partial aggregation takes 2 arguments",
              name);
        } else {
          VELOX_CHECK_EQ(
              argTypes.size(),
              1,
              "{} final aggregation takes one argument",
              name);
          VELOX_CHECK_EQ(
              argTypes[0]->kind(),
              TypeKind::ROW,
              "{} final aggregation takes ROW({NUMERIC,NUMERIC}) structs as input",
              name);
        }
        const auto valueType =
            isRawInput ? argTypes[0] : argTypes[0]->childAt(0);
        const auto compareType =
            isRawInput ? argTypes[1] : argTypes[0]->childAt(1);
        const std::string errorMessage = fmt::format(
            "Unknown input types for {} ({}) aggregation: {}, {}",
            name,
            mapAggregationStepToName(step),
            valueType->kindName(),
            compareType->kindName());

        switch (valueType->kind()) {
          case TypeKind::TINYINT:
            return create<Aggregate, int8_t>(
                resultType, compareType, errorMessage);
          case TypeKind::SMALLINT:
            return create<Aggregate, int16_t>(
                resultType, compareType, errorMessage);
          case TypeKind::INTEGER:
            return create<Aggregate, int32_t>(
                resultType, compareType, errorMessage);
          case TypeKind::BIGINT:
            return create<Aggregate, int64_t>(
                resultType, compareType, errorMessage);
          case TypeKind::REAL:
            return create<Aggregate, float>(
                resultType, compareType, errorMessage);
          case TypeKind::DOUBLE:
            return create<Aggregate, double>(
                resultType, compareType, errorMessage);
          case TypeKind::VARCHAR:
            return create<Aggregate, StringView>(
                resultType, compareType, errorMessage);
          case TypeKind::DATE:
            return create<Aggregate, Date>(
                resultType, compareType, errorMessage);
          default:
            VELOX_FAIL(errorMessage);
        }
      });
  return true;
}

} // namespace

void registerMinMaxByAggregates() {
  registerMinMaxByAggregate<MaxByAggregate>(kMaxBy);
  registerMinMaxByAggregate<MinByAggregate>(kMinBy);
}

} // namespace facebook::velox::aggregate::prestosql
