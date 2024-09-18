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
#include "velox/functions/lib/aggregates/CentralMomentsAggregatesBase.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

struct SkewnessResultAccessor {
  static bool hasResult(const CentralMomentsAccumulator& accumulator) {
    return accumulator.count() >= 3;
  }

  static double result(const CentralMomentsAccumulator& accumulator) {
    return std::sqrt(accumulator.count()) * accumulator.m3() /
        std::pow(accumulator.m2(), 1.5);
  }
};

struct KurtosisResultAccessor {
  static bool hasResult(const CentralMomentsAccumulator& accumulator) {
    return accumulator.count() >= 4;
  }

  static double result(const CentralMomentsAccumulator& accumulator) {
    double count = accumulator.count();
    double m2 = accumulator.m2();
    double m4 = accumulator.m4();
    return ((count - 1) * count * (count + 1)) / ((count - 2) * (count - 3)) *
        m4 / (m2 * m2) -
        3 * ((count - 1) * (count - 1)) / ((count - 2) * (count - 3));
  }
};

template <typename TResultAccessor>
exec::AggregateRegistrationResult registerCentralMoments(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  std::vector<std::string> inputTypes = {
      "smallint", "integer", "bigint", "real", "double"};
  for (const auto& inputType : inputTypes) {
    signatures.push_back(
        exec::AggregateFunctionSignatureBuilder()
            .returnType("double")
            .intermediateType(CentralMomentsIntermediateResult::type())
            .argumentType(inputType)
            .build());
  }

  return exec::registerAggregateFunction(
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
              return std::make_unique<
                  CentralMomentsAggregatesBase<int16_t, TResultAccessor>>(
                  resultType);
            case TypeKind::INTEGER:
              return std::make_unique<
                  CentralMomentsAggregatesBase<int32_t, TResultAccessor>>(
                  resultType);
            case TypeKind::BIGINT:
              return std::make_unique<
                  CentralMomentsAggregatesBase<int64_t, TResultAccessor>>(
                  resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<
                  CentralMomentsAggregatesBase<double, TResultAccessor>>(
                  resultType);
            case TypeKind::REAL:
              return std::make_unique<
                  CentralMomentsAggregatesBase<float, TResultAccessor>>(
                  resultType);
            default:
              VELOX_UNSUPPORTED(
                  "Unsupported input type: {}. "
                  "Expected SMALLINT, INTEGER, BIGINT, DOUBLE or REAL.",
                  inputType->toString());
          }
        } else {
          checkAccumulatorRowType(
              inputType,
              "Input type for final aggregation must be "
              "(count:bigint, m1:double, m2:double, m3:double, m4:double) struct");
          return std::make_unique<CentralMomentsAggregatesBase<
              int64_t /*unused*/,
              TResultAccessor>>(resultType);
        }
      },
      withCompanionFunctions,
      overwrite);
}

void registerCentralMomentsAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerCentralMoments<KurtosisResultAccessor>(
      prefix + kKurtosis, withCompanionFunctions, overwrite);
  registerCentralMoments<SkewnessResultAccessor>(
      prefix + kSkewness, withCompanionFunctions, overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
