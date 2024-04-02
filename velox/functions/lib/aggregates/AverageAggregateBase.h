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
#include "velox/functions/lib/aggregates/DecimalAggregate.h"
#include "velox/type/DecimalUtil.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::aggregate {

namespace {

/// Translate selected rows of decoded to the corresponding rows of its base
/// vector.
SelectivityVector translateToInnerRows(
    const DecodedVector& decoded,
    const SelectivityVector& rows) {
  VELOX_DCHECK(!decoded.isIdentityMapping());
  if (decoded.isConstantMapping()) {
    auto constantIndex = decoded.index(rows.begin());
    SelectivityVector baseRows{constantIndex + 1, false};
    baseRows.setValid(constantIndex, true);
    baseRows.updateBounds();
    return baseRows;
  } else {
    SelectivityVector baseRows{decoded.base()->size(), false};
    rows.applyToSelected(
        [&](auto row) { baseRows.setValid(decoded.index(row), true); });
    baseRows.updateBounds();
    return baseRows;
  }
}

/// Return the selected rows of the base vector of decoded corresponding to
/// rows. If decoded is not identify mapping, baseRowsHolder contains the
/// selected base rows. Otherwise, baseRowsHolder is unset.
const SelectivityVector* getBaseRows(
    const DecodedVector& decoded,
    const SelectivityVector& rows,
    SelectivityVector& baseRowsHolder) {
  const SelectivityVector* baseRows = &rows;
  if (!decoded.isIdentityMapping() && rows.hasSelections()) {
    baseRowsHolder = translateToInnerRows(decoded, rows);
    baseRows = &baseRowsHolder;
  }
  return baseRows;
}

template <typename TSum>
struct SumCount {
  TSum sum{0};
  int64_t count{0};
};

} // namespace

/// Partial aggregation produces a pair of sum and count.
/// Final aggregation produces the average (arithmetic mean)
/// of all non-null input values.
template <typename TInput, typename TAccumulator, typename TResult>
class AverageAggregateBase : public exec::Aggregate {
 public:
  explicit AverageAggregateBase(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(SumCount<TAccumulator>);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
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

 protected:
  /// Partial.
  template <bool tableHasNulls = true>
  inline void updateNonNullValue(char* group, TAccumulator value) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    accumulator(group)->sum += value;
    accumulator(group)->count =
        checkedPlus<int64_t>(accumulator(group)->count, 1);
  }

  template <bool tableHasNulls = true>
  inline void updateNonNullValue(char* group, int64_t count, TAccumulator sum) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    accumulator(group)->sum += sum;
    accumulator(group)->count =
        checkedPlus<int64_t>(accumulator(group)->count, count);
  }

  inline SumCount<TAccumulator>* accumulator(char* group) {
    return exec::Aggregate::value<SumCount<TAccumulator>>(group);
  }

  template <bool checkNullFields>
  void addIntermediateResultsImpl(
      char** groups,
      const SelectivityVector& rows) {
    auto baseRowVector = decodedPartial_.base()->template as<RowVector>();

    SelectivityVector baseRowsHolder;
    auto* baseRows = getBaseRows(decodedPartial_, rows, baseRowsHolder);

    DecodedVector baseSumDecoded{*baseRowVector->childAt(0), *baseRows};
    DecodedVector baseCountDecoded{*baseRowVector->childAt(1), *baseRows};

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        if constexpr (checkNullFields) {
          VELOX_USER_CHECK(
              !baseSumDecoded.isNullAt(decodedIndex) &&
              !baseCountDecoded.isNullAt(decodedIndex));
        }
        auto count = baseCountDecoded.template valueAt<int64_t>(decodedIndex);
        auto sum = baseSumDecoded.template valueAt<TAccumulator>(decodedIndex);
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
              !baseSumDecoded.isNullAt(decodedIndex) &&
              !baseCountDecoded.isNullAt(decodedIndex));
        }
        updateNonNullValue(
            groups[i],
            baseCountDecoded.template valueAt<int64_t>(decodedIndex),
            baseSumDecoded.template valueAt<TAccumulator>(decodedIndex));
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        if constexpr (checkNullFields) {
          VELOX_USER_CHECK(
              !baseSumDecoded.isNullAt(decodedIndex) &&
              !baseCountDecoded.isNullAt(decodedIndex));
        }
        updateNonNullValue(
            groups[i],
            baseCountDecoded.template valueAt<int64_t>(decodedIndex),
            baseSumDecoded.template valueAt<TAccumulator>(decodedIndex));
      });
    }
  }

  template <bool checkNullFields>
  void addSingleGroupIntermediateResultsImpl(
      char* group,
      const SelectivityVector& rows) {
    auto baseRowVector = decodedPartial_.base()->template as<RowVector>();

    SelectivityVector baseRowsHolder;
    auto* baseRows = getBaseRows(decodedPartial_, rows, baseRowsHolder);

    DecodedVector baseSumDecoded{*baseRowVector->childAt(0), *baseRows};
    DecodedVector baseCountDecoded{*baseRowVector->childAt(1), *baseRows};

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        if constexpr (checkNullFields) {
          VELOX_USER_CHECK(
              !baseSumDecoded.isNullAt(decodedIndex) &&
              !baseCountDecoded.isNullAt(decodedIndex));
        }
        const auto numRows = rows.countSelected();
        auto totalCount = checkedMultiply<int64_t>(
            baseCountDecoded.template valueAt<int64_t>(decodedIndex), numRows);
        auto totalSum =
            baseSumDecoded.template valueAt<TAccumulator>(decodedIndex) *
            numRows;
        updateNonNullValue(group, totalCount, totalSum);
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decodedPartial_.isNullAt(i)) {
          auto decodedIndex = decodedPartial_.index(i);
          if constexpr (checkNullFields) {
            VELOX_USER_CHECK(
                !baseSumDecoded.isNullAt(decodedIndex) &&
                !baseCountDecoded.isNullAt(decodedIndex));
          }
          updateNonNullValue(
              group,
              baseCountDecoded.template valueAt<int64_t>(decodedIndex),
              baseSumDecoded.template valueAt<TAccumulator>(decodedIndex));
        }
      });
    } else {
      TAccumulator totalSum(0);
      int64_t totalCount = 0;
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        if constexpr (checkNullFields) {
          VELOX_USER_CHECK(
              !baseSumDecoded.isNullAt(decodedIndex) &&
              !baseCountDecoded.isNullAt(decodedIndex));
        }
        totalCount = checkedPlus<int64_t>(
            totalCount,
            baseCountDecoded.template valueAt<int64_t>(decodedIndex));
        totalSum += baseSumDecoded.template valueAt<TAccumulator>(decodedIndex);
      });
      updateNonNullValue(group, totalCount, totalSum);
    }
  }

  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) SumCount<TAccumulator>();
    }
  }

  DecodedVector decodedRaw_;
  DecodedVector decodedPartial_;
};

template <typename TUnscaledType>
class DecimalAverageAggregateBase : public DecimalAggregate<TUnscaledType> {
 public:
  explicit DecimalAverageAggregateBase(TypePtr resultType)
      : DecimalAggregate<TUnscaledType>(resultType) {}

  virtual TUnscaledType computeFinalValue(
      functions::aggregate::LongDecimalWithOverflowState* accumulator) final {
    // Handles round-up of fraction results.
    int128_t average{0};
    DecimalUtil::computeAverage(
        average, accumulator->sum, accumulator->count, accumulator->overflow);
    return TUnscaledType(average);
  }
};

/// @brief Checks the input type for final aggregation of average.
/// The input type must be (sum:double/long decimal, count:bigint) struct.
/// @param type input type for final aggregation of average.
void checkAvgIntermediateType(const TypePtr& type);

} // namespace facebook::velox::functions::aggregate
