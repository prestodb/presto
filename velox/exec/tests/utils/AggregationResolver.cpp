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

#include "velox/exec/tests/utils/AggregationResolver.h"

#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionCallToSpecialForm.h"

namespace facebook::velox::exec::test {
namespace {
std::string throwAggregateFunctionDoesntExist(const std::string& name) {
  std::stringstream error;
  error << "Aggregate function doesn't exist: " << name << ".";
  exec::aggregateFunctions().withRLock([&](const auto& functionsMap) {
    if (functionsMap.empty()) {
      error << " Registry of aggregate functions is empty. "
               "Make sure to register some aggregate functions.";
    }
  });
  VELOX_USER_FAIL(error.str());
}

std::string throwAggregateFunctionSignatureNotSupported(
    const std::string& name,
    const std::vector<TypePtr>& types,
    const std::vector<std::shared_ptr<AggregateFunctionSignature>>&
        signatures) {
  std::stringstream error;
  error << "Aggregate function signature is not supported: "
        << toString(name, types)
        << ". Supported signatures: " << toString(signatures) << ".";
  VELOX_USER_FAIL(error.str());
}
} // namespace

TypePtr resolveAggregateType(
    const std::string& aggregateName,
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& rawInputTypes,
    bool nullOnFailure) {
  if (auto signatures = exec::getAggregateFunctionSignatures(aggregateName)) {
    for (const auto& signature : signatures.value()) {
      exec::SignatureBinder binder(*signature, rawInputTypes);
      if (binder.tryBind()) {
        return binder.tryResolveType(
            exec::isPartialOutput(step) ? signature->intermediateType()
                                        : signature->returnType());
      }
    }

    if (nullOnFailure) {
      return nullptr;
    }

    throwAggregateFunctionSignatureNotSupported(
        aggregateName, rawInputTypes, signatures.value());
  }

  // We may be parsing lambda expression used in a lambda aggregate function. In
  // this case, 'aggregateName' would refer to a scalar function.
  //
  // TODO Enhance the parser to allow for specifying separate resolver for
  // lambda expressions.
  if (auto type =
          exec::resolveTypeForSpecialForm(aggregateName, rawInputTypes)) {
    return type;
  }

  if (auto type = parse::resolveScalarFunctionType(
          aggregateName, rawInputTypes, true)) {
    return type;
  }

  if (nullOnFailure) {
    return nullptr;
  }

  throwAggregateFunctionDoesntExist(aggregateName);
  return nullptr;
}

AggregateTypeResolver::AggregateTypeResolver(core::AggregationNode::Step step)
    : step_(step), previousHook_(core::Expressions::getResolverHook()) {
  core::Expressions::setTypeResolverHook(
      [&](const auto& inputs, const auto& expr, bool nullOnFailure) {
        return resolveType(inputs, expr, nullOnFailure);
      });
}

AggregateTypeResolver::~AggregateTypeResolver() {
  core::Expressions::setTypeResolverHook(previousHook_);
}

TypePtr AggregateTypeResolver::resolveType(
    const std::vector<core::TypedExprPtr>& inputs,
    const std::shared_ptr<const core::CallExpr>& expr,
    bool nullOnFailure) const {
  auto functionName = expr->name();

  // Use raw input types (if available) to resolve intermediate and final
  // result types.
  if (exec::isRawInput(step_)) {
    std::vector<TypePtr> types;
    for (auto& input : inputs) {
      types.push_back(input->type());
    }

    return resolveAggregateType(functionName, step_, types, nullOnFailure);
  }

  if (!rawInputTypes_.empty()) {
    return resolveAggregateType(
        functionName, step_, rawInputTypes_, nullOnFailure);
  }

  if (!nullOnFailure) {
    VELOX_USER_FAIL(
        "Cannot resolve aggregation function return type without raw input types: {}",
        functionName);
  }
  return nullptr;
}
} // namespace facebook::velox::exec::test
