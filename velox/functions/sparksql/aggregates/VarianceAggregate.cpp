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
#include "velox/functions/sparksql/aggregates/VarianceAggregate.h"

#include <limits>
#include "velox/functions/lib/aggregates/VarianceAggregatesBase.h"

namespace facebook::velox::functions::aggregate::sparksql {

namespace {

// 'Sample standard deviation' result accessor for the Variance Accumulator.
//
// @tparam nullOnDivideByZero If true, return NULL instead of NaN when dividing
// by zero during the calculating.
template <bool nullOnDivideByZero>
struct StdDevSampResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    if constexpr (nullOnDivideByZero) {
      return accData.count() >= 2;
    } else {
      return accData.count() >= 1;
    }
  }

  static double result(const VarianceAccumulator& accData) {
    if (accData.count() == 1) {
      VELOX_CHECK(
          !nullOnDivideByZero,
          "NaN is returned only when m2 is 0 and nullOnDivideByZero is false.");
      return std::numeric_limits<double>::quiet_NaN();
    }
    return std::sqrt(accData.m2() / (accData.count() - 1));
  }
};

// 'Sample variance' result accessor for the Variance Accumulator.
//
// @tparam nullOnDivideByZero If true, return NULL instead of NaN when dividing
// by zero during the calculating.
template <bool nullOnDivideByZero>
struct VarSampResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    if constexpr (nullOnDivideByZero) {
      return accData.count() >= 2;
    } else {
      return accData.count() >= 1;
    }
  }

  static double result(const VarianceAccumulator& accData) {
    if (accData.count() == 1) {
      VELOX_CHECK(
          !nullOnDivideByZero,
          "NaN is returned only when m2 is 0 and nullOnDivideByZero is false.");
      return std::numeric_limits<double>::quiet_NaN();
    }
    return accData.m2() / (accData.count() - 1);
  }
};

template <template <bool> typename TResultAccessor>
exec::AggregateRegistrationResult registerVariance(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("row(bigint,double,double)")
          .argumentType("double")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        if (exec::isRawInput(step)) {
          VELOX_USER_CHECK_EQ(
              inputType->kind(),
              TypeKind::DOUBLE,
              "Unsupported input type: {}. "
              "Expected DOUBLE.",
              inputType->toString());
          if (config.sparkLegacyStatisticalAggregate()) {
            return std::make_unique<
                VarianceAggregate<double, TResultAccessor<false>>>(resultType);
          }
          return std::make_unique<
              VarianceAggregate<double, TResultAccessor<true>>>(resultType);
        }
        checkSumCountRowType(
            inputType,
            "Input type for final aggregation must be "
            "(count:bigint, mean:double, m2:double) struct");
        if (config.sparkLegacyStatisticalAggregate()) {
          return std::make_unique<
              VarianceAggregate<double, TResultAccessor<false>>>(resultType);
        }
        return std::make_unique<
            VarianceAggregate<double, TResultAccessor<true>>>(resultType);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerVarianceAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerVariance<StdDevSampResultAccessor>(
      prefix + "stddev", withCompanionFunctions, overwrite);
  registerVariance<StdDevSampResultAccessor>(
      prefix + "stddev_samp", withCompanionFunctions, overwrite);
  registerVariance<VarSampResultAccessor>(
      prefix + "variance", withCompanionFunctions, overwrite);
  registerVariance<VarSampResultAccessor>(
      prefix + "var_samp", withCompanionFunctions, overwrite);
}

} // namespace facebook::velox::functions::aggregate::sparksql
