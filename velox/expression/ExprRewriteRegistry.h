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

#include <folly/Synchronized.h>
#include "velox/core/Expressions.h"

namespace facebook::velox::expression {

/// An expression re-writer that takes an expression and returns an equivalent
/// expression or nullptr if re-write is not possible.
using ExpressionRewrite =
    std::function<core::TypedExprPtr(const core::TypedExprPtr)>;

class ExprRewriteRegistry {
 public:
  /// Appends a 'rewrite' to 'expressionRewrites'.
  ///
  /// The logic that applies re-writes is very simple and assumes that all
  /// rewrites are independent. For each expression, rewrites are applied in the
  /// order they were registered. The first rewrite that returns non-null result
  /// terminates the re-write for that particular expression.
  void registerRewrite(ExpressionRewrite rewrite);

  /// Clears the registry to remove all registered rewrites.
  void clear();

  core::TypedExprPtr rewrite(const core::TypedExprPtr& expr);

  static ExprRewriteRegistry& instance() {
    static ExprRewriteRegistry kInstance;
    return kInstance;
  }

 private:
  folly::Synchronized<std::vector<ExpressionRewrite>> registry_;
};
} // namespace facebook::velox::expression
