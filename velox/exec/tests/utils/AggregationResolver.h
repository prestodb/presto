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

#include "velox/core/ITypedExpr.h"
#include "velox/core/PlanNode.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/TypeResolver.h"

namespace facebook::velox::exec::test {
class AggregateTypeResolver {
 public:
  explicit AggregateTypeResolver(core::AggregationNode::Step step);

  ~AggregateTypeResolver();

  void setRawInputTypes(const std::vector<TypePtr>& types) {
    rawInputTypes_ = types;
  }

 private:
  TypePtr resolveType(
      const std::vector<core::TypedExprPtr>& inputs,
      const std::shared_ptr<const core::CallExpr>& expr,
      bool nullOnFailure) const;

  const core::AggregationNode::Step step_;
  const core::Expressions::TypeResolverHook previousHook_;
  std::vector<TypePtr> rawInputTypes_;
};

/// Resolves the output type for an aggregate function given its name,
/// aggregation step, and input types.
///
/// The resolution process follows these steps:
/// 1. Looks up registered aggregate function signatures by name
/// 2. Attempts to bind the provided raw input types to available signatures
/// 3. Returns the appropriate output type based on the aggregation step:
///    - For partial steps (kPartial, kIntermediate): returns intermediate type
///    - For final steps (kSingle, kFinal): returns the final result type
/// 4. Falls back to scalar function or special form resolution if aggregate
///    function lookup fails (useful for lambda expressions in aggregates)
///
/// @param aggregateName The name of the aggregate function (e.g., "sum",
/// "count", "avg")
/// @param step The aggregation step indicating the stage in multi-phase
/// aggregation
/// @param rawInputTypes The types of the raw input arguments to the aggregate
/// function
/// @param nullOnFailure If true, returns nullptr when type resolution fails;
///                      if false, throws an exception on failure
/// @return The resolved output type for the aggregate function, or nullptr if
///         nullOnFailure is true and resolution fails
/// @throws VeloxUserError if type resolution fails and nullOnFailure is false
TypePtr resolveAggregateType(
    const std::string& aggregateName,
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& rawInputTypes,
    bool nullOnFailure);
} // namespace facebook::velox::exec::test
