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

#include "velox/functions/sparksql/aggregates/CovarianceAggregate.h"

#include <limits>
#include "velox/functions/lib/aggregates/CovarianceAggregatesBase.h"

namespace facebook::velox::functions::aggregate::sparksql {

namespace {

template <bool nullOnDivideByZero>
struct CovarSampResultAccessor {
  static bool hasResult(const CovarAccumulator& accumulator) {
    if constexpr (nullOnDivideByZero) {
      return accumulator.count() >= 2;
    } else {
      return accumulator.count() >= 1;
    }
  }

  static double result(const CovarAccumulator& accumulator) {
    if (accumulator.count() == 1) {
      VELOX_CHECK(
          !nullOnDivideByZero,
          "NaN is returned only when m2 is 0 and nullOnDivideByZero is false.");
      return std::numeric_limits<double>::quiet_NaN();
    }
    if (FOLLY_UNLIKELY(std::isinf(accumulator.c2()))) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return accumulator.c2() / (accumulator.count() - 1);
  }
};

template <bool nullOnDivideByZero>
struct CorrResultAccessor {
  static bool hasResult(const CorrAccumulator& accumulator) {
    if constexpr (nullOnDivideByZero) {
      if (accumulator.count() == 1) {
        return false;
      }
      return accumulator.m2X() != 0 && accumulator.m2Y() != 0;
    } else {
      if (accumulator.count() == 1) {
        return true;
      }
      return accumulator.m2X() != 0 && accumulator.m2Y() != 0;
    }
  }

  static double result(const CorrAccumulator& accumulator) {
    if (accumulator.count() == 1) {
      VELOX_CHECK(
          !nullOnDivideByZero,
          "NaN is returned only when m2 is 0 and nullOnDivideByZero is false.");
      return std::numeric_limits<double>::quiet_NaN();
    }
    double stddevX = std::sqrt(accumulator.m2X());
    double stddevY = std::sqrt(accumulator.m2Y());
    return accumulator.c2() / stddevX / stddevY;
  }
};

template <
    typename TAccumulator,
    typename TIntermediateInput,
    typename TIntermediateResult,
    template <bool>
    class TResultAccessor>
exec::AggregateRegistrationResult registerCovariance(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType(TIntermediateResult::type())
          .argumentType("double")
          .argumentType("double")
          .build(),
  };

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [](core::AggregationNode::Step step,
         const std::vector<TypePtr>& argTypes,
         const TypePtr& resultType,
         const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        auto inputType = exec::isRawInput(step)
            ? argTypes[0]
            : (exec::isPartialOutput(step) ? DOUBLE() : resultType);
        VELOX_USER_CHECK_EQ(
            inputType->kind(),
            TypeKind::DOUBLE,
            "Unsupported input type: {}. "
            "Expected DOUBLE.",
            inputType->toString());

        if (config.sparkLegacyStatisticalAggregate()) {
          return std::make_unique<CovarianceAggregate<
              double,
              TAccumulator,
              TIntermediateInput,
              TIntermediateResult,
              TResultAccessor<false>>>(resultType);
        }
        return std::make_unique<CovarianceAggregate<
            double,
            TAccumulator,
            TIntermediateInput,
            TIntermediateResult,
            TResultAccessor<true>>>(resultType);
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
      CovarSampResultAccessor>(
      prefix + "covar_samp", withCompanionFunctions, overwrite);
  registerCovariance<
      CorrAccumulator,
      CorrIntermediateInput,
      CorrIntermediateResult,
      CorrResultAccessor>(prefix + "corr", withCompanionFunctions, overwrite);
}

} // namespace facebook::velox::functions::aggregate::sparksql
