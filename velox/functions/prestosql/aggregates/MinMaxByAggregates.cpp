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
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate {

namespace {

// MinMaxByAggregate is the base class for min_by and max_by functions. These
// functions return the value of X associated with the minimum/maximum value of
// Y over all input values. Partial aggregation produces a pair of X and min/max
// Y. Final aggregation takes a pair of X and min/max Y and returns X. T is the
// type of X and U is the type of Y.
template <typename T, typename U>
class MinMaxByAggregate : public exec::Aggregate {
 public:
  MinMaxByAggregate(TypePtr resultType, U initialValue)
      : exec::Aggregate(resultType), initialValue_(initialValue) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(T) + sizeof(U) + sizeof(bool);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      comparisonValue(group) = initialValue_;
      valueIsNull(group) = true;
    }
  }

  void finalize(char** /* unused */, int32_t /* unused */) override {}

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->as<FlatVector<T>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);

    T* rawValues = vector->mutableRawValues();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group) || valueIsNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        rawValues[i] = this->value(group);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    auto valueVector = rowVector->childAt(0)->asFlatVector<T>();
    auto comparisonVector = rowVector->childAt(1)->asFlatVector<U>();

    rowVector->resize(numGroups);
    valueVector->resize(numGroups);
    comparisonVector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(rowVector);

    T* rawValues = valueVector->mutableRawValues();
    U* rawComparisonValues = comparisonVector->mutableRawValues();
    BufferPtr nulls = valueVector->mutableNulls(rowVector->size());
    uint64_t* nullValues = nulls->asMutable<uint64_t>();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rowVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        bits::setNull(nullValues, i, valueIsNull(group));
        rawValues[i] = value(group);
        rawComparisonValues[i] = comparisonValue(group);
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
    // decodedValue will contain the values of column X.
    // decodedComparisonValue will contain the values of column Y which will be
    // used to select the minimum or the maximum.
    DecodedVector decodedValue(*args[0], rows);
    DecodedVector decodedComparisonValue(*args[1], rows);

    if (decodedValue.isConstantMapping() &&
        decodedComparisonValue.isConstantMapping()) {
      if (decodedComparisonValue.isNullAt(0)) {
        return;
      }
      auto value = decodedValue.valueAt<T>(0);
      auto comparisonValue = decodedComparisonValue.valueAt<U>(0);
      auto nullValue = decodedValue.isNullAt(0);
      rows.applyToSelected([&](vector_size_t i) {
        updateValues(groups[i], value, comparisonValue, nullValue, mayUpdate);
      });
    } else if (
        decodedValue.mayHaveNulls() || decodedComparisonValue.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedComparisonValue.isNullAt(i)) {
          return;
        }
        updateValues(
            groups[i],
            decodedValue.valueAt<T>(i),
            decodedComparisonValue.valueAt<U>(i),
            decodedValue.isNullAt(i),
            mayUpdate);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateValues(
            groups[i],
            decodedValue.valueAt<T>(i),
            decodedComparisonValue.valueAt<U>(i),
            false,
            mayUpdate);
      });
    }
  }

  template <typename MayUpdate>
  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      MayUpdate mayUpdate) {
    DecodedVector decodedPairs(*args[0], rows);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPairs.base());
    auto baseValueVector = baseRowVector->childAt(0)->as<FlatVector<T>>();
    auto baseComparisonVector = baseRowVector->childAt(1)->as<FlatVector<U>>();

    if (decodedPairs.isConstantMapping()) {
      if (decodedPairs.isNullAt(0)) {
        return;
      }
      auto decodedIndex = decodedPairs.index(0);
      auto value = baseValueVector->valueAt(decodedIndex);
      auto comparisonValue = baseComparisonVector->valueAt(decodedIndex);
      auto nullValue = baseValueVector->isNullAt(decodedIndex);
      rows.applyToSelected([&](vector_size_t i) {
        updateValues(groups[i], value, comparisonValue, nullValue, mayUpdate);
      });
    } else if (decodedPairs.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedPairs.isNullAt(i)) {
          return;
        }
        auto decodedIndex = decodedPairs.index(i);
        updateValues(
            groups[i],
            baseValueVector->valueAt(decodedIndex),
            baseComparisonVector->valueAt(decodedIndex),
            baseValueVector->isNullAt(decodedIndex),
            mayUpdate);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPairs.index(i);
        updateValues(
            groups[i],
            baseValueVector->valueAt(decodedIndex),
            baseComparisonVector->valueAt(decodedIndex),
            baseValueVector->isNullAt(decodedIndex),
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
    // decodedValue will contain the values of column X.
    // decodedComparisonValue will contain the values of column Y which will be
    // used to select the minimum or the maximum.
    DecodedVector decodedValue(*args[0], rows);
    DecodedVector decodedComparisonValue(*args[1], rows);

    if (decodedValue.isConstantMapping() &&
        decodedComparisonValue.isConstantMapping()) {
      if (decodedComparisonValue.isNullAt(0)) {
        return;
      }
      auto value = decodedValue.valueAt<T>(0);
      auto comparisonValue = decodedComparisonValue.valueAt<U>(0);
      auto nullValue = decodedValue.isNullAt(0);
      rows.applyToSelected([&](vector_size_t /*i*/) {
        updateValues(group, value, comparisonValue, nullValue, mayUpdate);
      });
    } else if (
        decodedValue.mayHaveNulls() || decodedComparisonValue.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedComparisonValue.isNullAt(i)) {
          return;
        }
        updateValues(
            group,
            decodedValue.valueAt<T>(i),
            decodedComparisonValue.valueAt<U>(i),
            decodedValue.isNullAt(i),
            mayUpdate);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateValues(
            group,
            decodedValue.valueAt<T>(i),
            decodedComparisonValue.valueAt<U>(i),
            false,
            mayUpdate);
      });
    }
  }

  // Final aggregation will take (Value, comparisonValue) structs as inputs. It
  // will produce the Value associated with the maximum/minimum of
  // comparisonValue over all structs.
  template <typename MayUpdate>
  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      MayUpdate mayUpdate) {
    // Decode struct(Value, ComparisonValue) as individual vectors.
    DecodedVector decodedPairs(*args[0], rows);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPairs.base());
    auto baseValueVector = baseRowVector->childAt(0)->as<FlatVector<T>>();
    auto baseComparisonVector = baseRowVector->childAt(1)->as<FlatVector<U>>();

    if (decodedPairs.isConstantMapping()) {
      if (decodedPairs.isNullAt(0)) {
        return;
      }
      auto decodedIndex = decodedPairs.index(0);
      auto value = baseValueVector->valueAt(decodedIndex);
      auto comparisonValue = baseComparisonVector->valueAt(decodedIndex);
      auto nullValue = baseValueVector->isNullAt(decodedIndex);
      rows.applyToSelected([&](vector_size_t /*i*/) {
        updateValues(group, value, comparisonValue, nullValue, mayUpdate);
      });
    } else if (decodedPairs.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedPairs.isNullAt(i)) {
          return;
        }
        auto decodedIndex = decodedPairs.index(i);
        updateValues(
            group,
            baseValueVector->valueAt(decodedIndex),
            baseComparisonVector->valueAt(decodedIndex),
            baseValueVector->isNullAt(decodedIndex),
            mayUpdate);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPairs.index(i);
        updateValues(
            group,
            baseValueVector->valueAt(decodedIndex),
            baseComparisonVector->valueAt(decodedIndex),
            baseValueVector->isNullAt(decodedIndex),
            mayUpdate);
      });
    }
  }

 private:
  template <typename MayUpdate>
  inline void updateValues(
      char* group,
      T newValue,
      U newComparisonValue,
      bool isNull,
      MayUpdate mayUpdate) {
    clearNull(group);
    if (mayUpdate(comparisonValue(group), newComparisonValue)) {
      value(group) = newValue;
      comparisonValue(group) = newComparisonValue;
      valueIsNull(group) = isNull;
    }
  }

  inline T& value(char* group) {
    return *reinterpret_cast<T*>(group + Aggregate::offset_);
  }

  inline U& comparisonValue(char* group) {
    return *reinterpret_cast<U*>(group + Aggregate::offset_ + sizeof(T));
  }

  inline bool& valueIsNull(char* group) {
    return *reinterpret_cast<bool*>(
        group + Aggregate::offset_ + sizeof(T) + sizeof(U));
  }

  // Initial value will take the minimum and maximum values of the numerical
  // limits.
  const U initialValue_;
};

template <typename T, typename U>
class MaxByAggregate : public MinMaxByAggregate<T, U> {
 public:
  explicit MaxByAggregate(TypePtr resultType)
      : MinMaxByAggregate<T, U>(resultType, std::numeric_limits<U>::min()) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    MinMaxByAggregate<T, U>::addRawInput(
        groups, rows, args, [](U& currentValue, U newValue) {
          return newValue > currentValue;
        });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    MinMaxByAggregate<T, U>::addIntermediateResults(
        groups, rows, args, [](U& currentValue, U newValue) {
          return newValue > currentValue;
        });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    MinMaxByAggregate<T, U>::addSingleGroupRawInput(
        group, rows, args, [](U& currentValue, U newValue) {
          return newValue > currentValue;
        });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    MinMaxByAggregate<T, U>::addSingleGroupIntermediateResults(
        group, rows, args, [](U& currentValue, U newValue) {
          return newValue > currentValue;
        });
  }
};

template <typename T, typename U>
class MinByAggregate : public MinMaxByAggregate<T, U> {
 public:
  explicit MinByAggregate(TypePtr resultType)
      : MinMaxByAggregate<T, U>(resultType, std::numeric_limits<U>::max()) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    MinMaxByAggregate<T, U>::addRawInput(
        groups, rows, args, [](U& currentValue, U newValue) {
          return newValue < currentValue;
        });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    MinMaxByAggregate<T, U>::addIntermediateResults(
        groups, rows, args, [](U& currentValue, U newValue) {
          return newValue < currentValue;
        });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    MinMaxByAggregate<T, U>::addSingleGroupRawInput(
        group, rows, args, [](U& currentValue, U newValue) {
          return newValue < currentValue;
        });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    MinMaxByAggregate<T, U>::addSingleGroupIntermediateResults(
        group, rows, args, [](U& currentValue, U newValue) {
          return newValue < currentValue;
        });
  }
};

template <template <typename U, typename V> class T, typename W>
std::unique_ptr<exec::Aggregate> create(
    TypePtr resultType,
    TypeKind compareType,
    const std::string& errorMessage) {
  switch (compareType) {
    case TypeKind::TINYINT:
      return std::make_unique<T<W, int8_t>>(resultType);
    case TypeKind::SMALLINT:
      return std::make_unique<T<W, int16_t>>(resultType);
    case TypeKind::INTEGER:
      return std::make_unique<T<W, int32_t>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<T<W, int64_t>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<T<W, float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<T<W, double>>(resultType);
    default:
      VELOX_FAIL("{}", errorMessage);
      return nullptr;
  }
}

template <template <typename U, typename V> class T>
bool registerMinMaxByAggregate(const std::string& name) {
  // TODO(spershin): Need to add support for varchar and other types of
  // variable length. For both arguments. See MinMaxAggregates for
  // reference.
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& valueType :
       {"tinyint", "smallint", "integer", "bigint", "real", "double"}) {
    for (const auto& compareType :
         {"tinyint", "smallint", "integer", "bigint", "real", "double"}) {
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

        auto valueType = isRawInput ? argTypes[0] : argTypes[0]->childAt(0);
        auto compareType = isRawInput ? argTypes[1] : argTypes[0]->childAt(1);
        std::string errorMessage = fmt::format(
            "Unknown input types for {} ({}) aggregation: {}, {}",
            name,
            mapAggregationStepToName(step),
            valueType->kindName(),
            compareType->kindName());

        switch (valueType->kind()) {
          case TypeKind::TINYINT:
            return create<T, int8_t>(
                resultType, compareType->kind(), errorMessage);
          case TypeKind::SMALLINT:
            return create<T, int16_t>(
                resultType, compareType->kind(), errorMessage);
          case TypeKind::INTEGER:
            return create<T, int32_t>(
                resultType, compareType->kind(), errorMessage);
          case TypeKind::BIGINT:
            return create<T, int64_t>(
                resultType, compareType->kind(), errorMessage);
          case TypeKind::REAL:
            return create<T, float>(
                resultType, compareType->kind(), errorMessage);
          case TypeKind::DOUBLE:
            return create<T, double>(
                resultType, compareType->kind(), errorMessage);
          default:
            VELOX_FAIL(errorMessage);
        }
      });

  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerMinMaxByAggregate<MaxByAggregate>(kMaxBy);
static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerMinMaxByAggregate<MinByAggregate>(kMinBy);

} // namespace
} // namespace facebook::velox::aggregate
