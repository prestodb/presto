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
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {
// Indices into RowType representing intermediate results of covar_samp and
// covar_pop. Columns appear in alphabetical order.
struct CovarIndices {
  int32_t count;
  int32_t meanX;
  int32_t meanY;
  int32_t c2;
};
constexpr CovarIndices kCovarIndices{1, 2, 3, 0};

// Indices into RowType representing intermediate results of corr. Columns
// appear in alphabetical order.
struct CorrIndices : public CovarIndices {
  int32_t m2X;
  int32_t m2Y;
};
constexpr CorrIndices kCorrIndices{{1, 4, 5, 0}, 2, 3};

// Indices into RowType representing intermediate results of regr_slope and
// regr_intercept.
struct RegrIndices : public CovarIndices {
  int32_t m2X;
  int32_t m2Y;
};
constexpr RegrIndices kRegrIndices{{1, 3, 4, 0}, 2, 5};

struct CovarAccumulator {
  int64_t count() const {
    return count_;
  }

  double meanX() const {
    return meanX_;
  }

  double meanY() const {
    return meanY_;
  }

  double c2() const {
    return c2_;
  }

  void update(double x, double y) {
    count_ += 1;
    double deltaX = x - meanX();
    meanX_ += deltaX / count();
    double deltaY = y - meanY();
    meanY_ += deltaY / count();
    c2_ += deltaX * (y - meanY());
  }

  void merge(
      int64_t countOther,
      double meanXOther,
      double meanYOther,
      double c2Other) {
    if (countOther == 0) {
      return;
    }
    if (count_ == 0) {
      count_ = countOther;
      meanX_ = meanXOther;
      meanY_ = meanYOther;
      c2_ = c2Other;
      return;
    }

    int64_t newCount = countOther + count();
    double deltaMeanX = meanXOther - meanX();
    double deltaMeanY = meanYOther - meanY();
    c2_ += c2Other +
        deltaMeanX * deltaMeanY * count() * countOther / (double)newCount;
    meanX_ += deltaMeanX * countOther / (double)newCount;
    meanY_ += deltaMeanY * countOther / (double)newCount;
    count_ = newCount;
  }

 private:
  int64_t count_{0};
  double meanX_{0};
  double meanY_{0};
  double c2_{0};
};

struct CovarSampResultAccessor {
  static bool hasResult(const CovarAccumulator& accumulator) {
    return accumulator.count() > 1;
  }

  static double result(const CovarAccumulator& accumulator) {
    return accumulator.c2() / (accumulator.count() - 1);
  }
};

struct CovarPopResultAccessor {
  static bool hasResult(const CovarAccumulator& accumulator) {
    return accumulator.count() > 0;
  }

  static double result(const CovarAccumulator& accumulator) {
    return accumulator.c2() / accumulator.count();
  }
};

template <typename T>
SimpleVector<T>* asSimpleVector(
    const RowVector* rowVector,
    int32_t childIndex) {
  return rowVector->childAt(childIndex)->as<SimpleVector<T>>();
}

class CovarIntermediateInput {
 public:
  explicit CovarIntermediateInput(
      const RowVector* rowVector,
      const CovarIndices& indices = kCovarIndices)
      : count_{asSimpleVector<int64_t>(rowVector, indices.count)},
        meanX_{asSimpleVector<double>(rowVector, indices.meanX)},
        meanY_{asSimpleVector<double>(rowVector, indices.meanY)},
        c2_{asSimpleVector<double>(rowVector, indices.c2)} {}

  void mergeInto(CovarAccumulator& accumulator, vector_size_t row) {
    accumulator.merge(
        count_->valueAt(row),
        meanX_->valueAt(row),
        meanY_->valueAt(row),
        c2_->valueAt(row));
  }

 protected:
  SimpleVector<int64_t>* count_;
  SimpleVector<double>* meanX_;
  SimpleVector<double>* meanY_;
  SimpleVector<double>* c2_;
};

template <typename T>
T* mutableRawValues(const RowVector* rowVector, int32_t childIndex) {
  return rowVector->childAt(childIndex)
      ->as<FlatVector<T>>()
      ->mutableRawValues();
}

class CovarIntermediateResult {
 public:
  explicit CovarIntermediateResult(
      const RowVector* rowVector,
      const CovarIndices& indices = kCovarIndices)
      : count_{mutableRawValues<int64_t>(rowVector, indices.count)},
        meanX_{mutableRawValues<double>(rowVector, indices.meanX)},
        meanY_{mutableRawValues<double>(rowVector, indices.meanY)},
        c2_{mutableRawValues<double>(rowVector, indices.c2)} {}

  static std::string type() {
    return "row(double,bigint,double,double)";
  }

  void set(vector_size_t row, const CovarAccumulator& accumulator) {
    count_[row] = accumulator.count();
    meanX_[row] = accumulator.meanX();
    meanY_[row] = accumulator.meanY();
    c2_[row] = accumulator.c2();
  }

 private:
  int64_t* count_;
  double* meanX_;
  double* meanY_;
  double* c2_;
};

struct CorrAccumulator : public CovarAccumulator {
  double m2X() const {
    return m2X_;
  }

  double m2Y() const {
    return m2Y_;
  }

  void update(double x, double y) {
    double oldMeanX = meanX();
    double oldMeanY = meanY();
    CovarAccumulator::update(x, y);

    m2X_ += (x - oldMeanX) * (x - meanX());
    m2Y_ += (y - oldMeanY) * (y - meanY());
  }

  void merge(
      int64_t countOther,
      double meanXOther,
      double meanYOther,
      double c2Other,
      double m2XOther,
      double m2YOther) {
    if (countOther == 0) {
      return;
    }

    if (count() == 0) {
      m2X_ = m2XOther;
      m2Y_ = m2YOther;
    } else {
      auto k = 1.0 * count() / (count() + countOther) * countOther;
      m2X_ += m2XOther + k * std::pow(meanX() - meanXOther, 2);
      m2Y_ += m2YOther + k * std::pow(meanY() - meanYOther, 2);
    }

    CovarAccumulator::merge(countOther, meanXOther, meanYOther, c2Other);
  }

 private:
  double m2X_{0};
  double m2Y_{0};
};

struct CorrResultAccessor {
  static bool hasResult(const CorrAccumulator& accumulator) {
    return !std::isnan(result(accumulator));
  }

  static double result(const CorrAccumulator& accumulator) {
    double stddevX = std::sqrt(accumulator.m2X());
    double stddevY = std::sqrt(accumulator.m2Y());
    return accumulator.c2() / stddevX / stddevY;
  }
};

class CorrIntermediateInput : public CovarIntermediateInput {
 public:
  explicit CorrIntermediateInput(const RowVector* rowVector)
      : CovarIntermediateInput(rowVector, kCorrIndices),
        m2X_{asSimpleVector<double>(rowVector, kCorrIndices.m2X)},
        m2Y_{asSimpleVector<double>(rowVector, kCorrIndices.m2Y)} {}

  void mergeInto(CorrAccumulator& accumulator, vector_size_t row) {
    accumulator.merge(
        count_->valueAt(row),
        meanX_->valueAt(row),
        meanY_->valueAt(row),
        c2_->valueAt(row),
        m2X_->valueAt(row),
        m2Y_->valueAt(row));
  }

 private:
  SimpleVector<double>* m2X_;
  SimpleVector<double>* m2Y_;
};

class CorrIntermediateResult : public CovarIntermediateResult {
 public:
  explicit CorrIntermediateResult(const RowVector* rowVector)
      : CovarIntermediateResult(rowVector, kCorrIndices),
        m2X_{mutableRawValues<double>(rowVector, kCorrIndices.m2X)},
        m2Y_{mutableRawValues<double>(rowVector, kCorrIndices.m2Y)} {}

  static std::string type() {
    return "row(double,bigint,double,double,double,double)";
  }

  void set(vector_size_t row, const CorrAccumulator& accumulator) {
    CovarIntermediateResult::set(row, accumulator);
    m2X_[row] = accumulator.m2X();
    m2Y_[row] = accumulator.m2Y();
  }

 private:
  double* m2X_;
  double* m2Y_;
};

struct RegrAccumulator : public CovarAccumulator {
  double m2X() const {
    return m2X_;
  }

  void update(double x, double y) {
    double oldMeanX = meanX();
    CovarAccumulator::update(x, y);

    m2X_ += (x - oldMeanX) * (x - meanX());
  }

  void merge(
      int64_t countOther,
      double meanXOther,
      double meanYOther,
      double c2Other,
      double m2XOther) {
    if (countOther == 0) {
      return;
    }
    if (count() == 0) {
      m2X_ = m2XOther;
    } else {
      m2X_ += m2XOther +
          1.0 * count() / (count() + countOther) * countOther *
              std::pow(meanX() - meanXOther, 2);
    }
    CovarAccumulator::merge(countOther, meanXOther, meanYOther, c2Other);
  }

 protected:
  double m2X_{0};
};

struct ExtendedRegrAccumulator : public RegrAccumulator {
  double m2Y() const {
    return m2Y_;
  }

  void update(double x, double y) {
    double oldMeanY = meanY();
    RegrAccumulator::update(x, y);
    m2Y_ += (y - oldMeanY) * (y - meanY());
  }

  void merge(
      int64_t countOther,
      double meanXOther,
      double meanYOther,
      double c2Other,
      double m2XOther,
      double m2YOther) {
    if (countOther == 0) {
      return;
    }
    if (count() == 0) {
      m2X_ = m2XOther;
      m2Y_ = m2YOther;
    } else {
      m2X_ += m2XOther +
          1.0 * count() / (count() + countOther) * countOther *
              std::pow(meanX() - meanXOther, 2);

      m2Y_ += m2YOther +
          1.0 * count() / (count() + countOther) * countOther *
              std::pow(meanY() - meanYOther, 2);
    }
    CovarAccumulator::merge(countOther, meanXOther, meanYOther, c2Other);
  }

 private:
  double m2Y_{0};
};

struct RegrCountResultAccessor {
  static bool hasResult(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.count() > 0;
  }

  static double result(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.count();
  }
};

struct RegrAvgyResultAccessor {
  static bool hasResult(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.count() > 0;
  }

  static double result(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.meanY();
  }
};

struct RegrAvgxResultAccessor {
  static bool hasResult(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.count() > 0;
  }

  static double result(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.meanX();
  }
};

struct RegrSxyResultAccessor {
  static bool hasResult(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.count() > 0;
  }

  static double result(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.c2();
  }
};

struct RegrR2ResultAccessor {
  static bool hasResult(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.m2X() != 0;
  }

  static double result(const ExtendedRegrAccumulator& accumulator) {
    if (accumulator.m2X() != 0 && accumulator.m2Y() == 0) {
      return 1;
    }
    return std::pow(accumulator.c2(), 2) /
        (accumulator.m2X() * accumulator.m2Y());
  }
};

struct RegrSyyResultAccessor {
  static bool hasResult(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.count() > 0;
  }

  static double result(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.m2Y();
  }
};

struct RegrSxxResultAccessor {
  static bool hasResult(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.count() > 0;
  }

  static double result(const ExtendedRegrAccumulator& accumulator) {
    return accumulator.m2X();
  }
};

struct RegrSlopeResultAccessor {
  static bool hasResult(const RegrAccumulator& accumulator) {
    return !std::isnan(result(accumulator));
  }

  static double result(const RegrAccumulator& accumulator) {
    return accumulator.c2() / accumulator.m2X();
  }
};

struct RegrInterceptResultAccessor {
  static bool hasResult(const RegrAccumulator& accumulator) {
    return !std::isnan(result(accumulator));
  }

  static double result(const RegrAccumulator& accumulator) {
    double slope = RegrSlopeResultAccessor::result(accumulator);
    return accumulator.meanY() - slope * accumulator.meanX();
  }
};

class RegrIntermediateInput : public CovarIntermediateInput {
 public:
  explicit RegrIntermediateInput(const RowVector* rowVector)
      : CovarIntermediateInput(rowVector, kRegrIndices),
        m2X_{asSimpleVector<double>(rowVector, kRegrIndices.m2X)} {}

  void mergeInto(RegrAccumulator& accumulator, vector_size_t row) {
    accumulator.merge(
        count_->valueAt(row),
        meanX_->valueAt(row),
        meanY_->valueAt(row),
        c2_->valueAt(row),
        m2X_->valueAt(row));
  }

 protected:
  SimpleVector<double>* m2X_;
};

class ExtendedRegrIntermediateInput : public RegrIntermediateInput {
 public:
  explicit ExtendedRegrIntermediateInput(const RowVector* rowVector)
      : RegrIntermediateInput(rowVector),
        m2Y_{asSimpleVector<double>(rowVector, kRegrIndices.m2Y)} {}

  void mergeInto(ExtendedRegrAccumulator& accumulator, vector_size_t row) {
    accumulator.merge(
        count_->valueAt(row),
        meanX_->valueAt(row),
        meanY_->valueAt(row),
        c2_->valueAt(row),
        m2X_->valueAt(row),
        m2Y_->valueAt(row));
  }

 private:
  SimpleVector<double>* m2Y_;
};

class RegrIntermediateResult : public CovarIntermediateResult {
 public:
  explicit RegrIntermediateResult(const RowVector* rowVector)
      : CovarIntermediateResult(rowVector, kRegrIndices),
        m2X_{mutableRawValues<double>(rowVector, kRegrIndices.m2X)} {}

  static std::string type() {
    return "row(double,bigint,double,double,double)";
  }

  void set(vector_size_t row, const RegrAccumulator& accumulator) {
    CovarIntermediateResult::set(row, accumulator);
    m2X_[row] = accumulator.m2X();
  }

 private:
  double* m2X_;
};

class ExtendedRegrIntermediateResult : public RegrIntermediateResult {
 public:
  explicit ExtendedRegrIntermediateResult(const RowVector* rowVector)
      : RegrIntermediateResult(rowVector),
        m2Y_{mutableRawValues<double>(rowVector, kRegrIndices.m2Y)} {}

  static std::string type() {
    return "row(double,bigint,double,double,double,double)";
  }

  void set(vector_size_t row, const ExtendedRegrAccumulator& accumulator) {
    RegrIntermediateResult::set(row, accumulator);
    m2Y_[row] = accumulator.m2Y();
  }

 private:
  double* m2Y_;
};

// @tparam T Type of the raw input and final result. Can be double or float.
template <
    typename T,
    typename TAccumulator,
    typename TIntermediateInput,
    typename TIntermediateResult,
    typename TResultAccessor>
class CovarianceAggregate : public exec::Aggregate {
 public:
  explicit CovarianceAggregate(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TAccumulator);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    if constexpr (
        std::is_same_v<TAccumulator, RegrAccumulator> ||
        std::is_same_v<TAccumulator, ExtendedRegrAccumulator>) {
      // The args order of linear regression function is (y, x), so we need to
      // swap the order
      decodedX_.decode(*args[1], rows);
      decodedY_.decode(*args[0], rows);
    } else {
      decodedX_.decode(*args[0], rows);
      decodedY_.decode(*args[1], rows);
    }

    rows.applyToSelected([&](auto row) {
      if (decodedX_.isNullAt(row) || decodedY_.isNullAt(row)) {
        return;
      }
      auto* group = groups[row];
      exec::Aggregate::clearNull(group);
      accumulator(group)->update(
          decodedX_.valueAt<T>(row), decodedY_.valueAt<T>(row));
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);

    auto baseRowVector = static_cast<const RowVector*>(decodedPartial_.base());
    TIntermediateInput input{baseRowVector};

    rows.applyToSelected([&](auto row) {
      if (decodedPartial_.isNullAt(row)) {
        return;
      }
      auto decodedIndex = decodedPartial_.index(row);
      auto* group = groups[row];
      exec::Aggregate::clearNull(group);
      input.mergeInto(*accumulator(group), decodedIndex);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    if constexpr (
        std::is_same_v<TAccumulator, RegrAccumulator> ||
        std::is_same_v<TAccumulator, ExtendedRegrAccumulator>) {
      // The args order of linear regression function is (y, x), so we need to
      // swap the order
      decodedX_.decode(*args[1], rows);
      decodedY_.decode(*args[0], rows);
    } else {
      decodedX_.decode(*args[0], rows);
      decodedY_.decode(*args[1], rows);
    }

    exec::Aggregate::clearNull(group);
    auto* accumulator = this->accumulator(group);

    rows.applyToSelected([&](auto row) {
      if (decodedX_.isNullAt(row) || decodedY_.isNullAt(row)) {
        return;
      }
      accumulator->update(decodedX_.valueAt<T>(row), decodedY_.valueAt<T>(row));
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);

    exec::Aggregate::clearNull(group);
    auto* accumulator = this->accumulator(group);

    auto baseRowVector = static_cast<const RowVector*>(decodedPartial_.base());
    TIntermediateInput input{baseRowVector};

    rows.applyToSelected([&](auto row) {
      if (decodedPartial_.isNullAt(row)) {
        return;
      }
      auto decodedIndex = decodedPartial_.index(row);
      input.mergeInto(*accumulator, decodedIndex);
    });
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->as<FlatVector<T>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);

    T* rawValues = vector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        auto* accumulator = this->accumulator(group);
        if (TResultAccessor::hasResult(*accumulator)) {
          clearNull(rawNulls, i);
          rawValues[i] = (T)TResultAccessor::result(*accumulator);
        } else {
          vector->setNull(i, true);
        }
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    rowVector->resize(numGroups);
    for (auto& child : rowVector->children()) {
      child->resize(numGroups);
    }

    uint64_t* rawNulls = getRawNulls(rowVector);

    TIntermediateResult covarResult{rowVector};

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rowVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        covarResult.set(i, *accumulator(group));
      }
    }
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) TAccumulator();
    }
  }

 private:
  inline TAccumulator* accumulator(char* group) {
    return exec::Aggregate::value<TAccumulator>(group);
  }

  DecodedVector decodedX_;
  DecodedVector decodedY_;
  DecodedVector decodedPartial_;
};

template <
    typename TAccumulator,
    typename TIntermediateInput,
    typename TIntermediateResult,
    typename TResultAccessor>
exec::AggregateRegistrationResult registerCovariance(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      // (double, double) -> double
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType(TIntermediateResult::type())
          .argumentType("double")
          .argumentType("double")
          .build(),
      // (real, real) -> real
      exec::AggregateFunctionSignatureBuilder()
          .returnType("real")
          .intermediateType(TIntermediateResult::type())
          .argumentType("real")
          .argumentType("real")
          .build(),
  };

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [](core::AggregationNode::Step step,
         const std::vector<TypePtr>& argTypes,
         const TypePtr& resultType,
         const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        auto rawInputType = exec::isRawInput(step)
            ? argTypes[0]
            : (exec::isPartialOutput(step) ? DOUBLE() : resultType);
        switch (rawInputType->kind()) {
          case TypeKind::DOUBLE:
            return std::make_unique<CovarianceAggregate<
                double,
                TAccumulator,
                TIntermediateInput,
                TIntermediateResult,
                TResultAccessor>>(resultType);
          case TypeKind::REAL:
            return std::make_unique<CovarianceAggregate<
                float,
                TAccumulator,
                TIntermediateInput,
                TIntermediateResult,
                TResultAccessor>>(resultType);
          default:
            VELOX_UNSUPPORTED(
                "Unsupported raw input type: {}. Expected DOUBLE or REAL.",
                rawInputType->toString())
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerCovarianceAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerCovariance<
      CovarAccumulator,
      CovarIntermediateInput,
      CovarIntermediateResult,
      CovarPopResultAccessor>(
      prefix + kCovarPop, withCompanionFunctions, overwrite);
  registerCovariance<
      CovarAccumulator,
      CovarIntermediateInput,
      CovarIntermediateResult,
      CovarSampResultAccessor>(
      prefix + kCovarSamp, withCompanionFunctions, overwrite);
  registerCovariance<
      CorrAccumulator,
      CorrIntermediateInput,
      CorrIntermediateResult,
      CorrResultAccessor>(prefix + kCorr, withCompanionFunctions, overwrite);
  registerCovariance<
      RegrAccumulator,
      RegrIntermediateInput,
      RegrIntermediateResult,
      RegrInterceptResultAccessor>(
      prefix + kRegrIntercept, withCompanionFunctions, overwrite);
  registerCovariance<
      RegrAccumulator,
      RegrIntermediateInput,
      RegrIntermediateResult,
      RegrSlopeResultAccessor>(
      prefix + kRegrSlop, withCompanionFunctions, overwrite);
  registerCovariance<
      ExtendedRegrAccumulator,
      ExtendedRegrIntermediateInput,
      ExtendedRegrIntermediateResult,
      RegrCountResultAccessor>(
      prefix + kRegrCount, withCompanionFunctions, overwrite);
  registerCovariance<
      ExtendedRegrAccumulator,
      ExtendedRegrIntermediateInput,
      ExtendedRegrIntermediateResult,
      RegrAvgyResultAccessor>(
      prefix + kRegrAvgy, withCompanionFunctions, overwrite);
  registerCovariance<
      ExtendedRegrAccumulator,
      ExtendedRegrIntermediateInput,
      ExtendedRegrIntermediateResult,
      RegrAvgxResultAccessor>(
      prefix + kRegrAvgx, withCompanionFunctions, overwrite);
  registerCovariance<
      ExtendedRegrAccumulator,
      ExtendedRegrIntermediateInput,
      ExtendedRegrIntermediateResult,
      RegrSxyResultAccessor>(
      prefix + kRegrSxy, withCompanionFunctions, overwrite);
  registerCovariance<
      ExtendedRegrAccumulator,
      ExtendedRegrIntermediateInput,
      ExtendedRegrIntermediateResult,
      RegrSxxResultAccessor>(
      prefix + kRegrSxx, withCompanionFunctions, overwrite);
  registerCovariance<
      ExtendedRegrAccumulator,
      ExtendedRegrIntermediateInput,
      ExtendedRegrIntermediateResult,
      RegrSyyResultAccessor>(
      prefix + kRegrSyy, withCompanionFunctions, overwrite);
  registerCovariance<
      ExtendedRegrAccumulator,
      ExtendedRegrIntermediateInput,
      ExtendedRegrIntermediateResult,
      RegrR2ResultAccessor>(
      prefix + kRegrR2, withCompanionFunctions, overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
