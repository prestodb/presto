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

#include <limits>
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregationHook.h"
#include "velox/functions/lib/aggregates/MinMaxAggregateBase.h"
#include "velox/functions/lib/aggregates/SimpleNumericAggregate.h"
#include "velox/functions/lib/aggregates/SingleValueAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/type/FloatingPointUtil.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {

std::pair<vector_size_t*, vector_size_t*> rawOffsetAndSizes(
    ArrayVector& arrayVector) {
  return {
      arrayVector.offsets()->asMutable<vector_size_t>(),
      arrayVector.sizes()->asMutable<vector_size_t>()};
}

/// @tparam V Type of value.
/// @tparam Compare Type of comparator for T.
template <typename T, typename Compare>
struct MinMaxNAccumulator {
  int64_t n{0};
  using Allocator = std::conditional_t<
      std::is_same_v<int128_t, T>,
      AlignedStlAllocator<T, sizeof(int128_t)>,
      StlAllocator<T>>;
  std::vector<T, Allocator> heapValues;

  explicit MinMaxNAccumulator(HashStringAllocator* allocator)
      : heapValues{Allocator(allocator)} {}

  int64_t getN() const {
    return n;
  }

  size_t size() const {
    return heapValues.size();
  }

  void checkAndSetN(DecodedVector& decodedN, vector_size_t row) {
    // Skip null N.
    if (decodedN.isNullAt(row)) {
      return;
    }

    const auto newN = decodedN.valueAt<int64_t>(row);
    VELOX_USER_CHECK_GT(
        newN, 0, "second argument of max/min must be a positive integer");

    VELOX_USER_CHECK_LE(
        newN,
        10'000,
        "second argument of max/min must be less than or equal to 10000");

    if (n) {
      VELOX_USER_CHECK_EQ(
          newN,
          n,
          "second argument of max/min must be a constant for all rows in a group");
    } else {
      n = newN;
    }
  }

  void compareAndAdd(T value, Compare& comparator) {
    if (heapValues.size() < n) {
      heapValues.push_back(value);
      std::push_heap(heapValues.begin(), heapValues.end(), comparator);
    } else {
      const auto& topValue = heapValues.front();
      if (comparator(value, topValue)) {
        std::pop_heap(heapValues.begin(), heapValues.end(), comparator);
        heapValues.back() = value;
        std::push_heap(heapValues.begin(), heapValues.end(), comparator);
      }
    }
  }

  /// Copy all values from 'topValues' into 'rawValues' buffer. The heap remains
  /// unchanged after the call.
  void extractValues(T* rawValues, vector_size_t offset, Compare& comparator) {
    std::sort_heap(heapValues.begin(), heapValues.end(), comparator);
    for (int64_t i = heapValues.size() - 1; i >= 0; --i) {
      rawValues[offset + i] = heapValues[i];
    }
    std::make_heap(heapValues.begin(), heapValues.end(), comparator);
  }
};

template <typename T, typename Compare>
class MinMaxNAggregateBase : public exec::Aggregate {
 protected:
  explicit MinMaxNAggregateBase(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  using AccumulatorType = MinMaxNAccumulator<T, Compare>;

  bool isFixedSize() const override {
    return false;
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (const vector_size_t i : indices) {
      auto group = groups[i];
      new (group + offset_) AccumulatorType(allocator_);
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    decodedValue_.decode(*args[0], rows);
    decodedN_.decode(*args[1], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decodedValue_.isNullAt(i) || decodedN_.isNullAt(i)) {
        return;
      }

      auto* group = groups[i];

      auto* accumulator = value(group);
      accumulator->checkAndSetN(decodedN_, i);

      auto tracker = trackRowSize(group);
      addRawInput(group, i);
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

    auto* accumulator = value(group);
    validateN(args[1], rows, accumulator);

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](vector_size_t i) {
      // Skip null value or N.
      if (!decodedValue_.isNullAt(i) && !decodedN_.isNullAt(i)) {
        addRawInput(group, i);
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

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto valuesArray = (*result)->as<ArrayVector>();
    valuesArray->resize(numGroups);

    const auto numValues =
        countValuesAndSetResultNulls(groups, numGroups, *result);

    auto values = valuesArray->elements();
    values->resize(numValues);

    auto* rawValues = values->asFlatVector<T>()->mutableRawValues();

    auto [rawOffsets, rawSizes] = rawOffsetAndSizes(*valuesArray);

    extractValues(groups, numGroups, rawOffsets, rawSizes, rawValues, nullptr);
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    rowVector->resize(numGroups);

    auto nVector = rowVector->childAt(0);
    nVector->resize(numGroups);

    auto valuesArray = rowVector->childAt(1)->as<ArrayVector>();
    valuesArray->resize(numGroups);

    const auto numValues =
        countValuesAndSetResultNulls(groups, numGroups, *result);

    auto values = valuesArray->elements();
    values->resize(numValues);

    auto* rawNs = nVector->as<FlatVector<int64_t>>()->mutableRawValues();
    auto* rawValues = values->asFlatVector<T>()->mutableRawValues();

    auto [rawOffsets, rawSizes] = rawOffsetAndSizes(*valuesArray);

    extractValues(groups, numGroups, rawOffsets, rawSizes, rawValues, rawNs);
  }

  void destroyInternal(folly::Range<char**> groups) override {
    destroyAccumulators<AccumulatorType>(groups);
  }

 private:
  inline AccumulatorType* value(char* group) {
    return reinterpret_cast<AccumulatorType*>(group + Aggregate::offset_);
  }

  void extractValues(
      char** groups,
      int32_t numGroups,
      vector_size_t* rawOffsets,
      vector_size_t* rawSizes,
      T* rawValues,
      int64_t* rawNs) {
    vector_size_t offset = 0;
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];

      if (!isNull(group)) {
        auto* accumulator = value(group);
        const vector_size_t size = accumulator->size();

        rawOffsets[i] = offset;
        rawSizes[i] = size;

        if (rawNs != nullptr) {
          rawNs[i] = accumulator->n;
        }
        accumulator->extractValues(rawValues, offset, comparator_);

        offset += size;
      }
    }
  }

  void addRawInput(char* group, vector_size_t index) {
    clearNull(group);

    auto* accumulator = value(group);

    accumulator->compareAndAdd(decodedValue_.valueAt<T>(index), comparator_);
  }

  struct IntermediateResult {
    const ArrayVector* valueArray;
    const FlatVector<T>* flatValues;
  };

  void addIntermediateResults(
      char* group,
      vector_size_t index,
      IntermediateResult& result) {
    clearNull(group);

    auto* accumulator = value(group);

    const auto decodedIndex = decodedIntermediates_.index(index);

    accumulator->checkAndSetN(decodedN_, decodedIndex);

    const auto* valueArray = result.valueArray;
    const auto* values = result.flatValues;
    auto* rawValues = values->rawValues();

    const auto numValues = valueArray->sizeAt(decodedIndex);
    const auto valueOffset = valueArray->offsetAt(decodedIndex);

    auto tracker = trackRowSize(group);
    for (auto i = 0; i < numValues; ++i) {
      const auto v = rawValues[valueOffset + i];
      accumulator->compareAndAdd(v, comparator_);
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

    IntermediateResult result{};
    result.valueArray = decodedValue_.base()->template as<ArrayVector>();
    result.flatValues =
        result.valueArray->elements()->template as<FlatVector<T>>();

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
        numValues += accumulator->size();
      }
    }

    return numValues;
  }

  void validateN(
      const VectorPtr& arg,
      const SelectivityVector& rows,
      AccumulatorType* accumulator) {
    decodedN_.decode(*arg, rows);
    if (decodedN_.isConstantMapping()) {
      accumulator->checkAndSetN(decodedN_, rows.begin());
    } else {
      rows.applyToSelected(
          [&](auto row) { accumulator->checkAndSetN(decodedN_, row); });
    }
  }

  Compare comparator_;
  DecodedVector decodedValue_;
  DecodedVector decodedN_;
  DecodedVector decodedIntermediates_;
};

template <typename T>
struct LessThanComparator : public std::less<T> {};
template <>
struct LessThanComparator<float>
    : public util::floating_point::NaNAwareLessThan<float> {};
template <>
struct LessThanComparator<double>
    : public util::floating_point::NaNAwareLessThan<double> {};

template <typename T>
struct GreaterThanComparator : public std::greater<T> {};
template <>
struct GreaterThanComparator<float>
    : public util::floating_point::NaNAwareGreaterThan<float> {};
template <>
struct GreaterThanComparator<double>
    : public util::floating_point::NaNAwareGreaterThan<double> {};

template <typename T>
class MinNAggregate : public MinMaxNAggregateBase<T, LessThanComparator<T>> {
 public:
  explicit MinNAggregate(const TypePtr& resultType)
      : MinMaxNAggregateBase<T, LessThanComparator<T>>(resultType) {}
};

template <typename T>
class MaxNAggregate : public MinMaxNAggregateBase<T, GreaterThanComparator<T>> {
 public:
  explicit MaxNAggregate(const TypePtr& resultType)
      : MinMaxNAggregateBase<T, GreaterThanComparator<T>>(resultType) {}
};

template <template <typename T> class TNumericN>
exec::AggregateRegistrationResult registerMinMax(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite,
    bool registerMin) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .orderableTypeVariable("T")
                           .returnType("T")
                           .intermediateType("T")
                           .argumentType("T")
                           .build());
  for (const auto& type :
       {"tinyint", "integer", "smallint", "bigint", "real", "double"}) {
    // T, bigint -> row(array(T), bigint) -> array(T)
    signatures.push_back(
        exec::AggregateFunctionSignatureBuilder()
            .returnType(fmt::format("array({})", type))
            .intermediateType(fmt::format("row(bigint, array({}))", type))
            .argumentType(type)
            .argumentType("bigint")
            .build());
  }

  // decimal(p,s), bigint -> row(array(decimal(p,s)), bigint) ->
  // array(decimal(p,s))
  signatures.push_back(
      exec::AggregateFunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .argumentType("bigint")
          .intermediateType("row(bigint, array(DECIMAL(a_precision, a_scale)))")
          .returnType("array(DECIMAL(a_precision, a_scale))")
          .build());

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name, registerMin](
          core::AggregationNode::Step step,
          std::vector<TypePtr> argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        const bool nAgg = !resultType->equivalent(*argTypes[0]);
        if (nAgg) {
          // We have either 2 arguments: T, bigint (partial aggregation)
          // or one argument: row(bigint, array(T)) (intermediate or final
          // aggregation). Extract T.
          const auto& inputType = argTypes.size() == 2
              ? argTypes[0]
              : argTypes[0]->childAt(1)->childAt(0);

          switch (inputType->kind()) {
            case TypeKind::TINYINT:
              return std::make_unique<TNumericN<int8_t>>(resultType);
            case TypeKind::SMALLINT:
              return std::make_unique<TNumericN<int16_t>>(resultType);
            case TypeKind::INTEGER:
              return std::make_unique<TNumericN<int32_t>>(resultType);
            case TypeKind::BIGINT:
              return std::make_unique<TNumericN<int64_t>>(resultType);
            case TypeKind::REAL:
              return std::make_unique<TNumericN<float>>(resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<TNumericN<double>>(resultType);
            case TypeKind::TIMESTAMP:
              return std::make_unique<TNumericN<Timestamp>>(resultType);
            case TypeKind::HUGEINT:
              if (inputType->isLongDecimal()) {
                return std::make_unique<TNumericN<int128_t>>(resultType);
              }
              [[fallthrough]];
            default:
              VELOX_UNREACHABLE(
                  "Unknown input type for {} aggregation {}",
                  name,
                  inputType->kindName());
          }
        } else {
          if (registerMin) {
            auto factory = getMinFunctionFactory(
                name,
                CompareFlags::NullHandlingMode::kNullAsIndeterminate,
                TimestampPrecision::kMilliseconds);
            return factory(step, argTypes, resultType, config);
          } else {
            auto factory = getMaxFunctionFactory(
                name,
                CompareFlags::NullHandlingMode::kNullAsIndeterminate,
                TimestampPrecision::kMilliseconds);
            return factory(step, argTypes, resultType, config);
          }
        }
      },
      {false /*orderSensitive*/},
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerMinMaxAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerMinMax<MinNAggregate>(
      prefix + kMin, withCompanionFunctions, overwrite, true);
  registerMinMax<MaxNAggregate>(
      prefix + kMax, withCompanionFunctions, overwrite, false);
}

} // namespace facebook::velox::aggregate::prestosql
