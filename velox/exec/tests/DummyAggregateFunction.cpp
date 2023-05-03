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

#include "velox/exec/tests/DummyAggregateFunction.h"

namespace facebook::velox::exec::test {

bool registerDummyAggregateFunction(
    const std::string& name,
    const std::vector<AggregateFunctionSignaturePtr>& signatures) {
  registerAggregateFunction(
      name,
      signatures,
      [&](core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_GE(argTypes.size(), 1);
        if (isPartialOutput(step)) {
          return std::make_unique<DummyDicitonaryFunction>(argTypes[0]);
        }
        return std::make_unique<DummyDicitonaryFunction>(resultType);
      },
      /*registerCompanionFunctions*/ true);

  return true;
}

} // namespace facebook::velox::exec::test
