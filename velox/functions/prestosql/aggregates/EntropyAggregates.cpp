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

namespace facebook::velox::aggregate::prestosql {

namespace {

// Indices into Row Vector, in which we store necessary accumulator data.
constexpr int32_t kSumCIdx{0};
constexpr int32_t kSumCLogCIdx{1};

// Structure storing necessary data to calculate entropy aggregations.
struct EntropyAccumulator {
  EntropyAccumulator() = default;

  EntropyAccumulator(double sumC, double sumCLogC)
      : sumC_(sumC), sumCLogC_(sumCLogC) {}

  double sumC() const {
    return sumC_;
  }

  double sumCLogC() const {
    return sumCLogC_;
  }

  void update(int64_t count) {
    VELOX_CHECK_GE(count, 0, "Entropy count value must be non-negative");
    if (count == 0) {
      return;
    }
    sumC_ += (double)count;
    sumCLogC_ += (double)count * std::log(count);
  }

  void merge(double sumCOther, double sumCLogCOther) {
    sumC_ += sumCOther;
    sumCLogC_ += sumCLogCOther;
  }

  double result() {
    double entropy = 0.0;
    if (sumC() > 0.0) {
      entropy = std::max(
          (std::log(sumC()) - sumCLogC() / sumC()) / std::log(2.0), 0.0);
    }
    return entropy;
  }

 private:
  double sumC_{0};
  double sumCLogC_{0};
};

// T is the input type for partial aggregation. Not used for final aggregation.
template <typename T>
class EntropyAggregate : public exec::Aggregate {
 public:
  explicit EntropyAggregate(TypePtr resultType) : exec::Aggregate(resultType) {}

  int32_t accumulatorAlignmentSize() const override {
    return alignof(EntropyAccumulator);
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(EntropyAccumulator);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedRaw_.decode(*args[0], rows);
    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        auto value = decodedRaw_.valueAt<T>(0);
        rows.applyToSelected(
            [&](vector_size_t i) { updateNonNullValue(groups[i], value); });
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedRaw_.isNullAt(i)) {
          return;
        }
        updateNonNullValue(groups[i], decodedRaw_.valueAt<T>(i));
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue(groups[i], decodedRaw_.valueAt<T>(i));
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
        const T value = decodedRaw_.valueAt<T>(0);
        const auto numRows = rows.countSelected();
        // The "sum" is the constant value times the number of rows.
        // Use double to prevent overflows (this is the same as what is done in
        // updateNonNullValue).
        const auto sum = (double)numRows * (double)value;
        EntropyAccumulator accData(sum, sum * std::log(value));
        updateNonNullValue(group, accData);
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decodedRaw_.isNullAt(i)) {
          updateNonNullValue(group, decodedRaw_.valueAt<T>(i));
        }
      });
    } else {
      EntropyAccumulator accData;
      rows.applyToSelected(
          [&](vector_size_t i) { accData.update(decodedRaw_.valueAt<T>(i)); });
      updateNonNullValue(group, accData);
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPartial_.base());
    auto baseSumCVector =
        baseRowVector->childAt(kSumCIdx)->as<SimpleVector<double>>();
    auto baseSumCLogCVector =
        baseRowVector->childAt(kSumCLogCIdx)->as<SimpleVector<double>>();

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        auto sumC = baseSumCVector->valueAt(decodedIndex);
        auto sumCLogC = baseSumCLogCVector->valueAt(decodedIndex);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(groups[i], sumC, sumCLogC);
        });
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedPartial_.isNullAt(i)) {
          return;
        }
        auto decodedIndex = decodedPartial_.index(i);
        updateNonNullValue(
            groups[i],
            baseSumCVector->valueAt(decodedIndex),
            baseSumCLogCVector->valueAt(decodedIndex));
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        updateNonNullValue(
            groups[i],
            baseSumCVector->valueAt(decodedIndex),
            baseSumCLogCVector->valueAt(decodedIndex));
      });
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPartial_.base());
    auto baseSumCVector =
        baseRowVector->childAt(kSumCIdx)->as<SimpleVector<double>>();
    auto baseSumCLogCVector =
        baseRowVector->childAt(kSumCLogCIdx)->as<SimpleVector<double>>();

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        const auto numRows = rows.countSelected();
        auto sumC = baseSumCVector->valueAt(decodedIndex);
        auto sumCLogC = baseSumCLogCVector->valueAt(decodedIndex);
        EntropyAccumulator accData(numRows * sumC, numRows * sumCLogC);
        updateNonNullValue(group, accData);
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decodedPartial_.isNullAt(i)) {
          auto decodedIndex = decodedPartial_.index(i);
          updateNonNullValue(
              group,
              baseSumCVector->valueAt(decodedIndex),
              baseSumCLogCVector->valueAt(decodedIndex));
        }
      });
    } else {
      EntropyAccumulator accData;
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        accData.merge(
            baseSumCVector->valueAt(decodedIndex),
            baseSumCLogCVector->valueAt(decodedIndex));
      });
      updateNonNullValue(group, accData);
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    auto sumCVector = rowVector->childAt(kSumCIdx)->asFlatVector<double>();
    auto sumCLogCVector =
        rowVector->childAt(kSumCLogCIdx)->asFlatVector<double>();

    rowVector->resize(numGroups);
    sumCVector->resize(numGroups);
    sumCLogCVector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(rowVector);

    double* rawSumCs = sumCVector->mutableRawValues();
    double* rawSumCLogCs = sumCLogCVector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rowVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        EntropyAccumulator* accData = accumulator(group);
        rawSumCs[i] = accData->sumC();
        rawSumCLogCs[i] = accData->sumCLogC();
      }
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->as<FlatVector<double>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);

    double* rawValues = vector->mutableRawValues();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rawValues[i] = 0.0;
      } else {
        clearNull(rawNulls, i);
        EntropyAccumulator* accData = accumulator(group);
        rawValues[i] = accData->result();
      }
    }
  }

 protected:
  inline EntropyAccumulator* accumulator(char* group) {
    return exec::Aggregate::value<EntropyAccumulator>(group);
  }

  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) EntropyAccumulator();
    }
  }

 private:
  // partial
  template <bool tableHasNulls = true>
  inline void updateNonNullValue(char* group, T value) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    EntropyAccumulator* accData = accumulator(group);
    accData->update(value);
  }

  template <bool tableHasNulls = true>
  inline void updateNonNullValue(
      char* group,
      const EntropyAccumulator& accData) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    EntropyAccumulator* thisAccData = accumulator(group);
    thisAccData->merge(accData.sumC(), accData.sumCLogC());
  }

  template <bool tableHasNulls = true>
  inline void updateNonNullValue(char* group, double sumC, double sumCLogC) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    EntropyAccumulator* accData = accumulator(group);
    accData->merge(sumC, sumCLogC);
  }

  DecodedVector decodedRaw_;
  DecodedVector decodedPartial_;
};

// Registration code
void checkRowType(const TypePtr& type, const std::string& errorMessage) {
  VELOX_CHECK_EQ(type->kind(), TypeKind::ROW, "{}", errorMessage);
  VELOX_CHECK_EQ(
      type->childAt(kSumCIdx)->kind(), TypeKind::DOUBLE, "{}", errorMessage);
  VELOX_CHECK_EQ(
      type->childAt(kSumCLogCIdx)->kind(),
      TypeKind::DOUBLE,
      "{}",
      errorMessage);
}

} // namespace

void registerEntropyAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  std::vector<std::string> inputTypes = {"smallint", "integer", "bigint"};
  for (const auto& inputType : inputTypes) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("row(double,double)")
                             .argumentType(inputType)
                             .build());
  }

  auto name = prefix + kEntropy;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        const auto& inputType = argTypes[0];
        if (exec::isRawInput(step)) {
          switch (inputType->kind()) {
            case TypeKind::SMALLINT:
              return std::make_unique<EntropyAggregate<int16_t>>(resultType);
            case TypeKind::INTEGER:
              return std::make_unique<EntropyAggregate<int32_t>>(resultType);
            case TypeKind::BIGINT:
              return std::make_unique<EntropyAggregate<int64_t>>(resultType);
            default:
              VELOX_UNSUPPORTED(
                  "Unsupported input type: {}. "
                  "Expected SMALLINT, INTEGER, BIGINT.",
                  inputType->toString())
          }
        } else {
          checkRowType(
              inputType,
              "Input type for final aggregation must be "
              "(sumC:double, sumCLogC:double) struct");
          // final agg not use template T, int64_t here has no effect.
          return std::make_unique<EntropyAggregate<int64_t>>(resultType);
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
