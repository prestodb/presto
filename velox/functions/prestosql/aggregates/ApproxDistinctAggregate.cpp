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
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/ApproxDistinctAggregates.h"
#include "velox/functions/prestosql/aggregates/HyperLogLogAggregate.h"
#include "velox/functions/prestosql/types/HyperLogLogRegistration.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

exec::AggregateRegistrationResult registerApproxDistinct(
    const std::string& name,
    bool hllAsFinalResult,
    bool withCompanionFunctions,
    bool overwrite,
    double defaultError) {
  // hllAsRawInput is false for approx_distinct and approx_set.
  bool hllAsRawInput = false;
  auto returnType = hllAsFinalResult ? "hyperloglog" : "bigint";
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  auto inputTypes = hllAsFinalResult
      ? std::vector<std::string>{"bigint", "double", "varchar", "unknown"}
      : std::vector<std::string>{
            "tinyint",
            "smallint",
            "integer",
            "bigint",
            "hugeint",
            "real",
            "double",
            "varchar",
            "varbinary",
            "timestamp",
            "date",
            "unknown"};
  for (const auto& inputType : inputTypes) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(returnType)
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .build());

    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(returnType)
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .argumentType("double")
                             .build());
  }
  if (!hllAsFinalResult) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("bigint")
                             .intermediateType("tinyint")
                             .argumentType("boolean")
                             .build());
  }

  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType(returnType)
                           .intermediateType("varbinary")
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType(returnType)
                           .intermediateType("varbinary")
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("double")
                           .build());
  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name, hllAsFinalResult, hllAsRawInput, defaultError](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        if (argTypes[0]->isUnKnown()) {
          if (hllAsFinalResult) {
            return std::make_unique<HyperLogLogAggregate<UnknownValue, true>>(
                resultType, hllAsRawInput, defaultError);
          } else {
            return std::make_unique<HyperLogLogAggregate<UnknownValue, false>>(
                resultType, hllAsRawInput, defaultError);
          }
        }
        if (exec::isPartialInput(step) && argTypes[0]->isTinyint()) {
          // This condition only applies to approx_distinct(boolean).
          return std::make_unique<HyperLogLogAggregate<bool, false>>(
              resultType, hllAsRawInput, defaultError);
        }
        return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            createHyperLogLogAggregate,
            argTypes[0]->kind(),
            resultType,
            hllAsFinalResult,
            hllAsRawInput,
            defaultError);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerApproxDistinctAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerHyperLogLogType();
  registerApproxDistinct(
      prefix + kApproxDistinct,
      false,
      withCompanionFunctions,
      overwrite,
      common::hll::kDefaultApproxDistinctStandardError);
  // approx_set is companion function for approx_distinct. Don't register
  // companion functions for it.
  registerApproxDistinct(
      prefix + kApproxSet,
      true,
      false,
      overwrite,
      common::hll::kDefaultApproxSetStandardError);
}

} // namespace facebook::velox::aggregate::prestosql
