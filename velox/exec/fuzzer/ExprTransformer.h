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

namespace facebook::velox::exec::test {

class ExprTransformer {
 public:
  virtual ~ExprTransformer() = default;

  /// Transforms the given expression into a new expression. This should be
  /// called during the expression generation in expression fuzzer.
  virtual core::TypedExprPtr transform(core::TypedExprPtr) const = 0;

  /// Returns the additional number of levels of nesting introduced by the
  /// transformation.
  virtual int32_t extraLevelOfNesting() const = 0;
};

} // namespace facebook::velox::exec::test
