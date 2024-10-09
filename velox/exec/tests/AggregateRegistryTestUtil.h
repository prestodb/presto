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
#pragma once

#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"

namespace facebook::velox {

class AggregateFunc : public exec::Aggregate {
 public:
  explicit AggregateFunc(TypePtr resultType) : Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return 0;
  }

  void addRawInput(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void addIntermediateResults(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void addSingleGroupRawInput(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void addSingleGroupIntermediateResults(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void extractValues(
      char** /*groups*/,
      int32_t /*numGroups*/,
      VectorPtr* /*result*/) override {}

  void extractAccumulators(
      char** /*groups*/,
      int32_t /*numGroups*/,
      VectorPtr* /*result*/) override {}
  static std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>
  signatures() {
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
        exec::AggregateFunctionSignatureBuilder()
            .returnType("bigint")
            .intermediateType("array(bigint)")
            .argumentType("bigint")
            .argumentType("double")
            .build(),
        exec::AggregateFunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("T")
            .intermediateType("array(T)")
            .argumentType("T")
            .argumentType("T")
            .build(),
        exec::AggregateFunctionSignatureBuilder()
            .returnType("date")
            .intermediateType("date")
            .build(),
    };
    return signatures;
  }

 protected:
  void initializeNewGroupsInternal(
      char** /*groups*/,
      folly::Range<const vector_size_t*> /*indices*/) override {}
};

inline bool registerAggregateFunc(
    const std::string& name,
    bool overwrite = false) {
  auto signatures = AggregateFunc::signatures();

  return registerAggregateFunction(
             name,
             std::move(signatures),
             [&](core::AggregationNode::Step step,
                 const std::vector<TypePtr>& argTypes,
                 const TypePtr& resultType,
                 const core::QueryConfig& /*config*/)
                 -> std::unique_ptr<exec::Aggregate> {
               if (exec::isPartialOutput(step)) {
                 if (argTypes.empty()) {
                   return std::make_unique<AggregateFunc>(resultType);
                 }
                 return std::make_unique<AggregateFunc>(ARRAY(resultType));
               }
               return std::make_unique<AggregateFunc>(resultType);
             },
             /*registerCompanionFunctions*/ false,
             overwrite)
      .mainFunction;
}

} // namespace facebook::velox
