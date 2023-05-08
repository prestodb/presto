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
#include "velox/functions/prestosql/aggregates/DecimalAggregate.h"
#include "velox/type/DecimalUtil.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename TSum>
struct SumCount {
  TSum sum{0};
  int64_t count{0};
};

// Partial aggregation produces a pair of sum and count.
// Count is BIGINT() while sum  and the final aggregates type depends on
// the input types:
//       INPUT TYPE    |     SUM             |     AVG
//   ------------------|---------------------|------------------
//     REAL            |     DOUBLE          |    REAL
//     ALL INTs        |     DOUBLE          |    DOUBLE
//
template <typename TInput, typename TAccumulator, typename TResult>
class AverageAggregate : public exec::Aggregate {
 public:
  explicit AverageAggregate(TypePtr resultType) : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(SumCount<TAccumulator>);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) SumCount<TAccumulator>();
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValuesImpl(groups, numGroups, result);
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    auto sumVector = rowVector->childAt(0)->asFlatVector<TAccumulator>();
    auto countVector = rowVector->childAt(1)->asFlatVector<int64_t>();

    rowVector->resize(numGroups);
    sumVector->resize(numGroups);
    countVector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(rowVector);

    int64_t* rawCounts = countVector->mutableRawValues();
    TAccumulator* rawSums = sumVector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rowVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        auto* sumCount = accumulator(group);
        rawCounts[i] = sumCount->count;
        rawSums[i] = sumCount->sum;
      }
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedRaw_.decode(*args[0], rows);
    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        auto value = decodedRaw_.valueAt<TInput>(0);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(groups[i], TAccumulator(value));
        });
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedRaw_.isNullAt(i)) {
          return;
        }
        updateNonNullValue(
            groups[i], TAccumulator(decodedRaw_.valueAt<TInput>(i)));
      });
    } else if (!exec::Aggregate::numNulls_ && decodedRaw_.isIdentityMapping()) {
      auto data = decodedRaw_.data<TInput>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<false>(groups[i], data[i]);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue(
            groups[i], TAccumulator(decodedRaw_.valueAt<TInput>(i)));
      });
    }
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedRaw_.decode(*args[0], rows);

    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        const TInput value = decodedRaw_.valueAt<TInput>(0);
        const auto numRows = rows.countSelected();
        updateNonNullValue(group, numRows, TAccumulator(value) * numRows);
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decodedRaw_.isNullAt(i)) {
          updateNonNullValue(
              group, TAccumulator(decodedRaw_.valueAt<TInput>(i)));
        }
      });
    } else if (!exec::Aggregate::numNulls_ && decodedRaw_.isIdentityMapping()) {
      const TInput* data = decodedRaw_.data<TInput>();
      TAccumulator totalSum(0);
      rows.applyToSelected([&](vector_size_t i) { totalSum += data[i]; });
      updateNonNullValue<false>(group, rows.countSelected(), totalSum);
    } else {
      TAccumulator totalSum(0);
      rows.applyToSelected(
          [&](vector_size_t i) { totalSum += decodedRaw_.valueAt<TInput>(i); });
      updateNonNullValue(group, rows.countSelected(), totalSum);
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    auto baseRowVector = decodedPartial_.base()->template as<RowVector>();

    if (validateIntermediateInputs_ &&
        (baseRowVector->childAt(0)->mayHaveNulls() ||
         baseRowVector->childAt(1)->mayHaveNulls())) {
      addIntermediateResultsImpl<true>(groups, rows);
      return;
    }
    addIntermediateResultsImpl<false>(groups, rows);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    auto baseRowVector = decodedPartial_.base()->template as<RowVector>();

    if (validateIntermediateInputs_ &&
        (baseRowVector->childAt(0)->mayHaveNulls() ||
         baseRowVector->childAt(1)->mayHaveNulls())) {
      addSingleGroupIntermediateResultsImpl<true>(group, rows);
      return;
    }
    addSingleGroupIntermediateResultsImpl<false>(group, rows);
  }

 private:
  // partial
  template <bool tableHasNulls = true>
  inline void updateNonNullValue(char* group, TAccumulator value) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    accumulator(group)->sum += value;
    accumulator(group)->count += 1;
  }

  template <bool tableHasNulls = true>
  inline void updateNonNullValue(char* group, int64_t count, TAccumulator sum) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    accumulator(group)->sum += sum;
    accumulator(group)->count += count;
  }

  inline SumCount<TAccumulator>* accumulator(char* group) {
    return exec::Aggregate::value<SumCount<TAccumulator>>(group);
  }

  template <bool checkNullFields>
  void addIntermediateResultsImpl(
      char** groups,
      const SelectivityVector& rows) {
    auto baseRowVector = decodedPartial_.base()->template as<RowVector>();
    auto baseSumVector =
        baseRowVector->childAt(0)->template as<SimpleVector<TAccumulator>>();
    auto baseCountVector =
        baseRowVector->childAt(1)->template as<SimpleVector<int64_t>>();

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        if constexpr (checkNullFields) {
          VELOX_USER_CHECK(
              !baseSumVector->isNullAt(decodedIndex) &&
              !baseCountVector->isNullAt(decodedIndex));
        }
        auto count = baseCountVector->valueAt(decodedIndex);
        auto sum = baseSumVector->valueAt(decodedIndex);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(groups[i], count, sum);
        });
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedPartial_.isNullAt(i)) {
          return;
        }
        auto decodedIndex = decodedPartial_.index(i);
        if constexpr (checkNullFields) {
          VELOX_USER_CHECK(
              !baseSumVector->isNullAt(decodedIndex) &&
              !baseCountVector->isNullAt(decodedIndex));
        }
        updateNonNullValue(
            groups[i],
            baseCountVector->valueAt(decodedIndex),
            baseSumVector->valueAt(decodedIndex));
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        if constexpr (checkNullFields) {
          VELOX_USER_CHECK(
              !baseSumVector->isNullAt(decodedIndex) &&
              !baseCountVector->isNullAt(decodedIndex));
        }
        updateNonNullValue(
            groups[i],
            baseCountVector->valueAt(decodedIndex),
            baseSumVector->valueAt(decodedIndex));
      });
    }
  }

  template <bool checkNullFields>
  void addSingleGroupIntermediateResultsImpl(
      char* group,
      const SelectivityVector& rows) {
    auto baseRowVector = decodedPartial_.base()->template as<RowVector>();
    auto baseSumVector =
        baseRowVector->childAt(0)->template as<SimpleVector<TAccumulator>>();
    auto baseCountVector =
        baseRowVector->childAt(1)->template as<SimpleVector<int64_t>>();

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        if constexpr (checkNullFields) {
          VELOX_USER_CHECK(
              !baseSumVector->isNullAt(decodedIndex) &&
              !baseCountVector->isNullAt(decodedIndex));
        }
        const auto numRows = rows.countSelected();
        auto totalCount = baseCountVector->valueAt(decodedIndex) * numRows;
        auto totalSum = baseSumVector->valueAt(decodedIndex) * numRows;
        updateNonNullValue(group, totalCount, totalSum);
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decodedPartial_.isNullAt(i)) {
          auto decodedIndex = decodedPartial_.index(i);
          if constexpr (checkNullFields) {
            VELOX_USER_CHECK(
                !baseSumVector->isNullAt(decodedIndex) &&
                !baseCountVector->isNullAt(decodedIndex));
          }
          updateNonNullValue(
              group,
              baseCountVector->valueAt(decodedIndex),
              baseSumVector->valueAt(decodedIndex));
        }
      });
    } else {
      TAccumulator totalSum(0);
      int64_t totalCount = 0;
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        if constexpr (checkNullFields) {
          VELOX_USER_CHECK(
              !baseSumVector->isNullAt(decodedIndex) &&
              !baseCountVector->isNullAt(decodedIndex));
        }
        totalCount += baseCountVector->valueAt(decodedIndex);
        totalSum += baseSumVector->valueAt(decodedIndex);
      });
      updateNonNullValue(group, totalCount, totalSum);
    }
  }

  void extractValuesImpl(char** groups, int32_t numGroups, VectorPtr* result) {
    auto vector = (*result)->as<FlatVector<TResult>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);

    TResult* rawValues = vector->mutableRawValues();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        auto* sumCount = accumulator(group);
        rawValues[i] = TResult(sumCount->sum) / sumCount->count;
      }
    }
  }

  DecodedVector decodedRaw_;
  DecodedVector decodedPartial_;
};

void checkSumCountRowType(TypePtr type, const std::string& errorMessage) {
  VELOX_CHECK(
      type->kind() == TypeKind::ROW || type->kind() == TypeKind::VARBINARY,
      "{}",
      errorMessage);
  if (type->kind() == TypeKind::VARBINARY) {
    return;
  }
  VELOX_CHECK(
      type->childAt(0)->kind() == TypeKind::DOUBLE ||
          type->childAt(0)->isLongDecimal(),
      "{}",
      errorMessage)
  VELOX_CHECK_EQ(
      type->childAt(1)->kind(), TypeKind::BIGINT, "{}", errorMessage);
}

template <typename TUnscaledType>
class DecimalAverageAggregate : public DecimalAggregate<TUnscaledType> {
 public:
  explicit DecimalAverageAggregate(TypePtr resultType)
      : DecimalAggregate<TUnscaledType>(resultType) {}

  virtual TUnscaledType computeFinalValue(
      LongDecimalWithOverflowState* accumulator) final {
    // Handles round-up of fraction results.
    int128_t average{0};
    DecimalUtil::computeAverage(
        average, accumulator->sum, accumulator->count, accumulator->overflow);
    return TUnscaledType(average);
  }
};

bool registerAverage(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : {"smallint", "integer", "bigint", "double"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("row(double,bigint)")
                             .argumentType(inputType)
                             .build());
  }
  // Real input type in Presto has special case and returns REAL, not DOUBLE.
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("real")
                           .intermediateType("row(double,bigint)")
                           .argumentType("real")
                           .build());

  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .intermediateType("varbinary")
                           .returnType("DECIMAL(a_precision, a_scale)")
                           .build());

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        auto inputType = argTypes[0];
        if (exec::isRawInput(step)) {
          switch (inputType->kind()) {
            case TypeKind::SMALLINT:
              return std::make_unique<
                  AverageAggregate<int16_t, double, double>>(resultType);
            case TypeKind::INTEGER:
              return std::make_unique<
                  AverageAggregate<int32_t, double, double>>(resultType);
            case TypeKind::BIGINT: {
              if (inputType->isShortDecimal()) {
                return std::make_unique<DecimalAverageAggregate<int64_t>>(
                    resultType);
              }
              return std::make_unique<
                  AverageAggregate<int64_t, double, double>>(resultType);
            }
            case TypeKind::HUGEINT: {
              if (inputType->isLongDecimal()) {
                return std::make_unique<DecimalAverageAggregate<int128_t>>(
                    resultType);
              }
              VELOX_NYI();
            }
            case TypeKind::REAL:
              return std::make_unique<AverageAggregate<float, double, float>>(
                  resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<AverageAggregate<double, double, double>>(
                  resultType);
            default:
              VELOX_FAIL(
                  "Unknown input type for {} aggregation {}",
                  name,
                  inputType->kindName());
          }
        } else {
          checkSumCountRowType(
              inputType,
              "Input type for final aggregation must be (sum:double/long decimal, count:bigint) struct");
          switch (resultType->kind()) {
            case TypeKind::REAL:
              return std::make_unique<AverageAggregate<int64_t, double, float>>(
                  resultType);
            case TypeKind::DOUBLE:
            case TypeKind::ROW:
              return std::make_unique<
                  AverageAggregate<int64_t, double, double>>(resultType);
            case TypeKind::BIGINT:
              return std::make_unique<DecimalAverageAggregate<int64_t>>(
                  resultType);
            case TypeKind::HUGEINT:
              return std::make_unique<DecimalAverageAggregate<int128_t>>(
                  resultType);
            case TypeKind::VARBINARY:
              if (inputType->isLongDecimal()) {
                return std::make_unique<DecimalAverageAggregate<int128_t>>(
                    resultType);
              } else if (
                  inputType->isShortDecimal() ||
                  inputType->kind() == TypeKind::VARBINARY) {
                // If the input and out type are VARBINARY, then the
                // LongDecimalWithOverflowState is used and the template type
                // does not matter.
                return std::make_unique<DecimalAverageAggregate<int64_t>>(
                    resultType);
              }
            default:
              VELOX_FAIL(
                  "Unsupported result type for final aggregation: {}",
                  resultType->kindName());
          }
        }
      },
      /*registerCompanionFunctions*/ true);
  return true;
}

} // namespace

void registerAverageAggregate(const std::string& prefix) {
  registerAverage(prefix + kAvg);
}

} // namespace facebook::velox::aggregate::prestosql
