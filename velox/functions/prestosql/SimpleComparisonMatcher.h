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

#include "velox/core/Expressions.h"

namespace facebook::velox::functions::prestosql {

struct SimpleComparison {
  core::TypedExprPtr expr;
  bool isLessThen;
};

/// Given a lambda expression, checks it if represents a simple comparator and
/// returns the summary of the same.
///
/// For example, identifies
///     (x, y) -> if(length(x) < length(y), -1, if(length(x) > length(y), 1, 0))
/// expression as a "less than" comparison over length(x).
///
/// Recognizes different variations of this expression, e.g.
///
///     (x, y) -> if(expr(x) = expr(y), 0, if(expr(x) < expr(y), -1, 1))
///     (x, y) -> if(expr(x) = expr(y), 0, if(expr(y) > expr(x), -1, 1))
///
/// Returns std::nullopt if expression is not recognized as a simple comparator.
///
/// Can be used to re-write generic lambda expressions passed to array_sort into
/// simpler ones that can be evaluated more efficiently.
std::optional<SimpleComparison> isSimpleComparison(
    const std::string& prefix,
    const core::LambdaTypedExpr& expr);

} // namespace facebook::velox::functions::prestosql
