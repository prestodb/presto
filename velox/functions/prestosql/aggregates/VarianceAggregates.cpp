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
constexpr int32_t kCountIdx{0};
constexpr int32_t kMeanIdx{1};
constexpr int32_t kM2Idx{2};

// Structure storing necessary data to calculate variance-based aggregations.
struct VarianceAccumulator {
  // Default (empty) ctor
  VarianceAccumulator() = default;

  // Fast construct from repetitive values.
  VarianceAccumulator(int64_t count, double value)
      : count_(count), mean_(value), m2_(0.0) {}

  double count() const {
    return count_;
  }

  double mean() const {
    return mean_;
  }

  double m2() const {
    return m2_;
  }

  void update(double value) {
    count_ += 1;
    double delta = value - mean();
    mean_ += delta / count();
    m2_ += delta * (value - mean());
  }

  inline void merge(const VarianceAccumulator& other) {
    merge(other.count(), other.mean(), other.m2());
  }

  void merge(int64_t countOther, double meanOther, double m2Other) {
    if (countOther == 0) {
      return;
    }
    int64_t newCount = countOther + count();
    double newMean =
        ((countOther * meanOther) + (count() * mean())) / (double)newCount;
    double delta = meanOther - mean();
    m2_ += m2Other + delta * delta * countOther * count() / (double)newCount;
    count_ = newCount;
    mean_ = newMean;
  }

 private:
  int64_t count_{0};
  double mean_{0};
  double m2_{0};
};

// 'Population standard deviation' result accessor for the Variance Accumulator.
struct StdDevPopResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    return accData.count() > 0;
  }

  static double result(const VarianceAccumulator& accData) {
    return std::sqrt(accData.m2() / accData.count());
  }
};

// 'Sample standard deviation' result accessor for the Variance Accumulator.
struct StdDevSampResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    return accData.count() >= 2;
  }

  static double result(const VarianceAccumulator& accData) {
    return std::sqrt(accData.m2() / (accData.count() - 1));
  }
};

// 'Population variance' result accessor for the Variance Accumulator.
struct VarPopResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    return accData.count() > 0;
  }

  static double result(const VarianceAccumulator& accData) {
    return accData.m2() / accData.count();
  }
};

// 'Sample variance' result accessor for the Variance Accumulator.
struct VarSampResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    return accData.count() >= 2;
  }

  static double result(const VarianceAccumulator& accData) {
    return accData.m2() / (accData.count() - 1);
  }
};

// Base class for a set of Variance-based aggregations. Not used on its own, the
// classes derived from it are used instead.
// Partial aggregation produces variance struct.
// Final aggregation takes the variance struct and returns a double.
// T is the input type for partial aggregation. Not used for final aggregation.
// TResultAccessor is the type of the static struct that will access the result
// in a certain way from the Variance Accumulator.
template <typename T, typename TResultAccessor>
class VarianceAggregate : public exec::Aggregate {
 public:
  explicit VarianceAggregate(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(VarianceAccumulator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) VarianceAccumulator();
    }
  }

  void finalize(char** /* unused */, int32_t /* unused */) override {}

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    auto countVector = rowVector->childAt(kCountIdx)->asFlatVector<int64_t>();
    auto meanVector = rowVector->childAt(kMeanIdx)->asFlatVector<double>();
    auto m2Vector = rowVector->childAt(kM2Idx)->asFlatVector<double>();

    rowVector->resize(numGroups);
    for (auto& child : rowVector->children()) {
      child->resize(numGroups);
    }
    uint64_t* rawNulls = getRawNulls(rowVector);

    int64_t* rawCounts = countVector->mutableRawValues();
    double* rawMeans = meanVector->mutableRawValues();
    double* rawM2s = m2Vector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rowVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        VarianceAccumulator* accData = accumulator(group);
        rawCounts[i] = accData->count();
        rawMeans[i] = accData->mean();
        rawM2s[i] = accData->m2();
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
    } else if (!exec::Aggregate::numNulls_ && decodedRaw_.isIdentityMapping()) {
      auto data = decodedRaw_.data<T>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<false>(groups[i], data[i]);
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
        VarianceAccumulator accData(numRows, (double)value);
        updateNonNullValue(group, accData);
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decodedRaw_.isNullAt(i)) {
          updateNonNullValue(group, decodedRaw_.valueAt<T>(i));
        }
      });
    } else if (!exec::Aggregate::numNulls_ && decodedRaw_.isIdentityMapping()) {
      const T* data = decodedRaw_.data<T>();
      VarianceAccumulator accData;
      rows.applyToSelected([&](vector_size_t i) { accData.update(data[i]); });
      updateNonNullValue<false>(group, accData);
    } else {
      VarianceAccumulator accData;
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
    auto baseCountVector =
        baseRowVector->childAt(kCountIdx)->as<SimpleVector<int64_t>>();
    auto baseMeanVector =
        baseRowVector->childAt(kMeanIdx)->as<SimpleVector<double>>();
    auto baseM2Vector =
        baseRowVector->childAt(kM2Idx)->as<SimpleVector<double>>();

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        auto count = baseCountVector->valueAt(decodedIndex);
        auto mean = baseMeanVector->valueAt(decodedIndex);
        auto m2 = baseM2Vector->valueAt(decodedIndex);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(groups[i], count, mean, m2);
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
            baseCountVector->valueAt(decodedIndex),
            baseMeanVector->valueAt(decodedIndex),
            baseM2Vector->valueAt(decodedIndex));
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        updateNonNullValue(
            groups[i],
            baseCountVector->valueAt(decodedIndex),
            baseMeanVector->valueAt(decodedIndex),
            baseM2Vector->valueAt(decodedIndex));
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
    auto baseCountVector =
        baseRowVector->childAt(kCountIdx)->as<SimpleVector<int64_t>>();
    auto baseMeanVector =
        baseRowVector->childAt(kMeanIdx)->as<SimpleVector<double>>();
    auto baseM2Vector =
        baseRowVector->childAt(kM2Idx)->as<SimpleVector<double>>();

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        VarianceAccumulator accData;
        for (auto i = 0; i < rows.countSelected(); ++i) {
          accData.merge(
              baseCountVector->valueAt(decodedIndex),
              baseMeanVector->valueAt(decodedIndex),
              baseM2Vector->valueAt(decodedIndex));
        }
        updateNonNullValue(group, accData);
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decodedPartial_.isNullAt(i)) {
          auto decodedIndex = decodedPartial_.index(i);
          updateNonNullValue(
              group,
              baseCountVector->valueAt(decodedIndex),
              baseMeanVector->valueAt(decodedIndex),
              baseM2Vector->valueAt(decodedIndex));
        }
      });
    } else {
      VarianceAccumulator accData;
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        accData.merge(
            baseCountVector->valueAt(decodedIndex),
            baseMeanVector->valueAt(decodedIndex),
            baseM2Vector->valueAt(decodedIndex));
      });
      updateNonNullValue(group, accData);
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
        vector->setNull(i, true);
      } else {
        VarianceAccumulator* accData = accumulator(group);
        if (TResultAccessor::hasResult(*accData)) {
          clearNull(rawNulls, i);
          rawValues[i] = TResultAccessor::result(*accData);
        } else {
          vector->setNull(i, true);
        }
      }
    }
  }

 protected:
  inline VarianceAccumulator* accumulator(char* group) {
    return exec::Aggregate::value<VarianceAccumulator>(group);
  }

 private:
  // partial
  template <bool tableHasNulls = true>
  inline void updateNonNullValue(char* group, T value) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    VarianceAccumulator* thisAccData = accumulator(group);
    thisAccData->update((double)value);
  }

  template <bool tableHasNulls = true>
  inline void updateNonNullValue(
      char* group,
      const VarianceAccumulator& accData) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    VarianceAccumulator* thisAccData = accumulator(group);
    thisAccData->merge(accData);
  }

  template <bool tableHasNulls = true>
  inline void
  updateNonNullValue(char* group, int64_t count, double mean, double m2) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    VarianceAccumulator* thisAccData = accumulator(group);
    thisAccData->merge(count, mean, m2);
  }

  DecodedVector decodedRaw_;
  DecodedVector decodedPartial_;
};

// Implements 'Population Standard Deviation' aggregate.
// T is the input type for partial aggregation. Not used for final aggregation.
template <typename T>
class StdDevPopAggregate
    : public VarianceAggregate<T, StdDevPopResultAccessor> {
 public:
  explicit StdDevPopAggregate(TypePtr resultType)
      : VarianceAggregate<T, StdDevPopResultAccessor>(resultType) {}
};

// Implements 'Sample Standard Deviation' aggregate.
// T is the input type for partial aggregation. Not used for final aggregation.
template <typename T>
class StdDevSampAggregate
    : public VarianceAggregate<T, StdDevSampResultAccessor> {
 public:
  explicit StdDevSampAggregate(TypePtr resultType)
      : VarianceAggregate<T, StdDevSampResultAccessor>(resultType) {}
};

// Implements 'Population Variance' aggregate.
// T is the input type for partial aggregation. Not used for final aggregation.
template <typename T>
class VarPopAggregate : public VarianceAggregate<T, VarPopResultAccessor> {
 public:
  explicit VarPopAggregate(TypePtr resultType)
      : VarianceAggregate<T, VarPopResultAccessor>(resultType) {}
};

// Implements 'Sample Variance' aggregate.
// T is the input type for partial aggregation. Not used for final aggregation.
template <typename T>
class VarSampAggregate : public VarianceAggregate<T, VarSampResultAccessor> {
 public:
  explicit VarSampAggregate(TypePtr resultType)
      : VarianceAggregate<T, VarSampResultAccessor>(resultType) {}
};

// Registration code

void checkSumCountRowType(
    const TypePtr& type,
    const std::string& errorMessage) {
  VELOX_CHECK_EQ(type->kind(), TypeKind::ROW, "{}", errorMessage);
  VELOX_CHECK_EQ(
      type->childAt(kCountIdx)->kind(), TypeKind::BIGINT, "{}", errorMessage);
  VELOX_CHECK_EQ(
      type->childAt(kMeanIdx)->kind(), TypeKind::DOUBLE, "{}", errorMessage);
  VELOX_CHECK_EQ(
      type->childAt(kM2Idx)->kind(), TypeKind::DOUBLE, "{}", errorMessage);
}

template <template <typename TInput> class TClass>
bool registerVarianceAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  std::vector<std::string> inputTypes = {
      "smallint", "integer", "bigint", "real", "double"};
  for (const auto& inputType : inputTypes) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("row(bigint,double,double)")
                             .argumentType(inputType)
                             .build());
  }

  return exec::registerAggregateFunction(
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
              return std::make_unique<TClass<int16_t>>(resultType);
            case TypeKind::INTEGER:
              return std::make_unique<TClass<int32_t>>(resultType);
            case TypeKind::BIGINT:
              return std::make_unique<TClass<int64_t>>(resultType);
            case TypeKind::REAL:
              return std::make_unique<TClass<float>>(resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<TClass<double>>(resultType);
            default:
              VELOX_FAIL(
                  "Unknown input type for {} aggregation: {}",
                  name,
                  inputType->toString());
          }
        } else {
          checkSumCountRowType(
              inputType,
              "Input type for final aggregation must be "
              "(count:bigint, mean:double, m2:double) struct");
          return std::make_unique<TClass<int64_t>>(resultType);
        }
      });
}

} // namespace

void registerVarianceAggregates() {
  registerVarianceAggregate<StdDevSampAggregate>(kStdDev);
  registerVarianceAggregate<StdDevPopAggregate>(kStdDevPop);
  registerVarianceAggregate<StdDevSampAggregate>(kStdDevSamp);
  registerVarianceAggregate<VarSampAggregate>(kVariance);
  registerVarianceAggregate<VarPopAggregate>(kVarPop);
  registerVarianceAggregate<VarSampAggregate>(kVarSamp);
}

} // namespace facebook::velox::aggregate::prestosql
