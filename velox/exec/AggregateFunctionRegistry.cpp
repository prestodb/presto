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
#include "velox/exec/AggregateFunctionRegistry.h"

#include "velox/exec/Aggregate.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {

std::pair<TypePtr, TypePtr> resolveAggregateFunction(
    const std::string& name,
    const std::vector<TypePtr>& argTypes) {
  if (auto signatures = getAggregateFunctionSignatures(name)) {
    for (const auto& signature : signatures.value()) {
      SignatureBinder binder(*signature, argTypes);
      if (binder.tryBind()) {
        return std::make_pair(
            binder.tryResolveReturnType(),
            binder.tryResolveType(signature->intermediateType()));
      }
    }

    std::stringstream error;
    error << "Aggregate function signature is not supported: "
          << toString(name, argTypes)
          << ". Supported signatures: " << toString(signatures.value()) << ".";
    VELOX_USER_FAIL(error.str());

  } else {
    VELOX_USER_FAIL("Aggregate function not registered: {}", name);
  }
}

} // namespace facebook::velox::exec
