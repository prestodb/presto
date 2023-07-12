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

namespace facebook::velox::functions {

/// Analyzes array_sort(array, lambda) call to determine whether it can be
/// re-written into a simpler call that specifies sort-by expression.
///
/// For example, rewrites
///     array_sort(a, (x, y) -> if(length(x) < length(y), -1, if(length(x) >
///     length(y), 1, 0))
/// into
///     array_sort(a, x -> length(x))
///
/// Returns new expression or nullptr if rewrite is not possible.
core::TypedExprPtr rewriteArraySortCall(
    const std::string& prefix,
    const core::TypedExprPtr& expr);

} // namespace facebook::velox::functions
