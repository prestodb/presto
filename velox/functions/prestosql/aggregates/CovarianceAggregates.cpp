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
#include "velox/functions/prestosql/aggregates/CovarianceAggregates.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/CovarianceAggregatesBase.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {
// Indices into RowType representing intermediate results of regr_slope and
// regr_intercept.
struct RegrIndices : public CovarIndices {
  int32_t m2X;
  int32_t m2Y;
};
constexpr RegrIndices kRegrIndices{{1, 3, 4, 0}, 2, 5};

struct CovarPopResultAccessor {
  static bool hasResult(const CovarAccumulator& accumulator) {
    return accumulator.count() > 0;
  }

  static double result(const CovarAccumulator& accumulator) {
    return accumulator.c2() / accumulator.count();
  }
};

struct CovarSampResultAccessor {
  static bool hasResult(const CovarAccumulator& accumulator) {
    return accumulator.count() > 1;
  }

  static double result(const CovarAccumulator& accumulator) {
    return accumulator.c2() / (accumulator.count() - 1);
  }
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
                rawInputType->toString());
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
