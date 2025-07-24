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
#include "velox/functions/prestosql/aggregates/SfmSketchAggregate.h"
#include "velox/functions/prestosql/types/SfmSketchRegistration.h"

namespace facebook::velox::aggregate::prestosql {

void registerNoisyApproxSfmAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  // Register the SfmSketch type.
  registerSfmSketchType();

  std::vector<std::string> supportedInputTypes = {
      "bigint", "double", "varchar", "varbinary"};

  auto setBuilder = []() {
    return exec::AggregateFunctionSignatureBuilder()
        .returnType("sfmsketch")
        .intermediateType("varbinary");
  };
  auto countBuilder = []() {
    return exec::AggregateFunctionSignatureBuilder()
        .returnType("bigint")
        .intermediateType("varbinary");
  };

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> setSignatures;
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>
      countSignatures;
  for (const auto& inputType : supportedInputTypes) {
    setSignatures.push_back(setBuilder()
                                .argumentType(inputType)
                                .constantArgumentType("double")
                                .build());
    setSignatures.push_back(setBuilder()
                                .argumentType(inputType)
                                .constantArgumentType("double")
                                .constantArgumentType("bigint")
                                .build());
    setSignatures.push_back(setBuilder()
                                .argumentType(inputType)
                                .constantArgumentType("double")
                                .constantArgumentType("bigint")
                                .constantArgumentType("bigint")
                                .build());
    countSignatures.push_back(countBuilder()
                                  .argumentType(inputType)
                                  .constantArgumentType("double")
                                  .build());
    countSignatures.push_back(countBuilder()
                                  .argumentType(inputType)
                                  .constantArgumentType("double")
                                  .constantArgumentType("bigint")
                                  .build());
    countSignatures.push_back(countBuilder()
                                  .argumentType(inputType)
                                  .constantArgumentType("double")
                                  .constantArgumentType("bigint")
                                  .constantArgumentType("bigint")
                                  .build());
  }

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>
      setFromIndexAndZerosSignatures;

  setFromIndexAndZerosSignatures.push_back(
      setBuilder()
          .argumentType("bigint") // col_index
          .argumentType("bigint") // col_zeros
          .constantArgumentType("double") // epsilon
          .constantArgumentType("bigint") // buckets
          .build());
  setFromIndexAndZerosSignatures.push_back(
      setBuilder()
          .argumentType("bigint") // col_index
          .argumentType("bigint") // col_zeros
          .constantArgumentType("double") // epsilon
          .constantArgumentType("bigint") // buckets
          .constantArgumentType("bigint") // precision
          .build());

  exec::registerAggregateFunction(
      prefix + kNoisyApproxSetSfm,
      setSignatures,
      [prefix](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& /*argTypes*/,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        if (exec::isPartialOutput(step)) {
          return std::make_unique<SfmSketchAggregate<true, false>>(VARBINARY());
        }
        return std::make_unique<SfmSketchAggregate<true, false>>(resultType);
      },
      withCompanionFunctions,
      overwrite);

  exec::registerAggregateFunction(
      prefix + kNoisyApproxDistinctSfm,
      countSignatures,
      [prefix](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& /*argTypes*/,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        if (exec::isPartialOutput(step)) {
          return std::make_unique<SfmSketchAggregate<false, false>>(
              VARBINARY());
        }
        return std::make_unique<SfmSketchAggregate<false, false>>(resultType);
      },
      withCompanionFunctions,
      overwrite);

  exec::registerAggregateFunction(
      prefix + kNoisyApproxSetSfmFromIndexAndZeros,
      setFromIndexAndZerosSignatures,
      [prefix](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& /*argTypes*/,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        if (exec::isPartialOutput(step)) {
          return std::make_unique<SfmSketchAggregate<true, true>>(VARBINARY());
        }
        return std::make_unique<SfmSketchAggregate<true, true>>(resultType);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
