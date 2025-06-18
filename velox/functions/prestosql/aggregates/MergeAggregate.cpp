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
#include "velox/functions/prestosql/aggregates/MergeAggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/HyperLogLogAggregate.h"
#include "velox/functions/prestosql/aggregates/MergeQDigestAggregate.h"
#include "velox/functions/prestosql/aggregates/MergeTDigestAggregate.h"
#include "velox/functions/prestosql/types/HyperLogLogRegistration.h"
#include "velox/functions/prestosql/types/QDigestRegistration.h"
#include "velox/functions/prestosql/types/TDigestRegistration.h"
#include "velox/functions/prestosql/types/TDigestType.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

exec::AggregateRegistrationResult registerMerge(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite,
    double defaultError) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  auto inputTypes = std::vector<std::string>{
      "hyperloglog",
      "tdigest(double)",
      "qdigest(bigint)",
      "qdigest(real)",
      "qdigest(double)"};
  signatures.reserve(inputTypes.size());
  for (const auto& inputType : inputTypes) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .build());
  }
  bool hllAsRawInput = true;
  bool hllAsFinalResult = true;
  return exec::registerAggregateFunction(
      name,
      signatures,
      [name, hllAsFinalResult, hllAsRawInput, defaultError](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        if (*argTypes[0] == *TDIGEST(DOUBLE())) {
          return createMergeTDigestAggregate(resultType);
        }
        if (*argTypes[0] == *QDIGEST(BIGINT()) ||
            *argTypes[0] == *QDIGEST(REAL()) ||
            *argTypes[0] == *QDIGEST(DOUBLE())) {
          return createMergeQDigestAggregate(resultType, argTypes[0]);
        }
        if (argTypes[0]->isUnKnown()) {
          return std::make_unique<HyperLogLogAggregate<UnknownValue, true>>(
              resultType, hllAsRawInput, defaultError);
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

void registerMergeAggregate(
    const std::string& prefix,
    bool /* withCompanionFunctions */,
    bool overwrite) {
  registerHyperLogLogType();
  registerTDigestType();
  registerQDigestType();
  // merge is companion function for approx_distinct. Don't register companion
  // functions for it.
  registerMerge(
      prefix + kMerge,
      false,
      overwrite,
      common::hll::kDefaultApproxSetStandardError);
}

} // namespace facebook::velox::aggregate::prestosql
