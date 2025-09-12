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

#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/SimpleComparisonMatcher.h"

namespace facebook::velox::functions {

/// Creates array_sort function.
///
/// @param ascending If true, sort in ascending order; otherwise, sort in
/// descending order.
/// @param nullsFirst If true, nulls are placed first; otherwise, nulls are
/// placed last.
/// @param throwOnNestedNull If true, throw an exception if a nested null is
/// encountered.
std::shared_ptr<exec::VectorFunction> makeArraySort(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config,
    bool ascending,
    bool nullsFirst,
    bool throwOnNestedNull);

/// Creates array_sort with a lambda function.
///
/// @param ascending If true, sort in ascending order; otherwise, sort in
/// descending order.
/// @param throwOnNestedNull If true, throw an exception if a nested null is
/// encountered.
std::shared_ptr<exec::VectorFunction> makeArraySortLambdaFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config,
    bool ascending,
    bool throwOnNestedNull);

/// Returns signatures for array_sort function.
///
/// @param withComparator If true, includes a signature for sorting with a
/// comparator function.
std::vector<std::shared_ptr<exec::FunctionSignature>> arraySortSignatures(
    bool withComparator);

/// Analyzes array_sort(array, lambda) call to determine whether it can be
/// re-written into a simpler call that specifies sort-by expression.
///
/// For example, rewrites
///     array_sort(a, (x, y) -> if(length(x) < length(y), -1, if(length(x) >
///     length(y), 1, 0))
/// into
///     array_sort(a, x -> length(x))
///
/// Returns the rewritten expression. If rewrite is not possible, throw a user
/// error.
core::TypedExprPtr rewriteArraySortCall(
    const std::string& prefix,
    const core::TypedExprPtr& expr,
    const std::shared_ptr<SimpleComparisonChecker> checker);

} // namespace facebook::velox::functions
