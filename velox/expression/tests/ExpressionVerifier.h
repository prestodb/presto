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
#include "velox/core/QueryCtx.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::test {

struct ExpressionVerifierOptions {
  bool disableConstantFolding{false};
  std::string reproPersistPath;
};

class ExpressionVerifier {
 public:
  ExpressionVerifier(
      core::ExecCtx* FOLLY_NONNULL execCtx,
      ExpressionVerifierOptions options)
      : execCtx_(execCtx), options_(options) {}

  // Executes an expression and verifies the results/exceptions from common
  // execution path vs simple execution path. Returns:
  //
  //  - true if both paths succeeded and returned the exact same results.
  //  - false if both paths failed with compatible exceptions.
  //  - throws otherwise (incompatible exceptions or different results).
  bool verify(
      const core::TypedExprPtr& plan,
      const RowVectorPtr& rowVector,
      VectorPtr&& resultVector,
      bool canThrow);

 private:
  void persistReproInfo(
      const VectorPtr& inputVector,
      const VectorPtr& resultVector,
      const std::string& sql);

 private:
  core::ExecCtx* FOLLY_NONNULL execCtx_;
  const ExpressionVerifierOptions options_;
};

} // namespace facebook::velox::test
