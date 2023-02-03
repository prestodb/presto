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
#include "velox/expression/tests/FuzzerToolkit.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::test {

struct ExpressionVerifierOptions {
  bool disableConstantFolding{false};
  std::string reproPersistPath;
  bool persistAndRunOnce{false};
};

class ExpressionVerifier {
 public:
  // File names used to persist data required for reproducing a failed test
  // case.
  static constexpr const std::string_view kInputVectorFileName = "input_vector";
  static constexpr const std::string_view kIndicesOfLazyColumnsFileName =
      "indices_of_lazy_columns";
  static constexpr const std::string_view kResultVectorFileName =
      "result_vector";
  static constexpr const std::string_view kExpressionSqlFileName = "sql";
  static constexpr const std::string_view kComplexConstantsFileName =
      "complex_constants";

  ExpressionVerifier(
      core::ExecCtx* FOLLY_NONNULL execCtx,
      ExpressionVerifierOptions options)
      : execCtx_(execCtx), options_(options) {}

  // Executes an expression both using common path (all evaluation
  // optimizations) and simplified path. Additionally, a sorted list of column
  // indices can be passed via 'columnsToWarpInLazy' which specify the
  // columns/children in the input row vector that should be wrapped in a lazy
  // layer before running it through the common evaluation path.
  // Returns:
  //  - result of evaluating the expression if both paths succeeded and returned
  //  the exact same vectors.
  //  - exception thrown by the common path if both paths failed with compatible
  //  exceptions.
  //  - throws otherwise (incompatible exceptions or different results).
  ResultOrError verify(
      const core::TypedExprPtr& plan,
      const RowVectorPtr& rowVector,
      VectorPtr&& resultVector,
      bool canThrow,
      std::vector<column_index_t> columnsToWarpInLazy = {});

 private:
  // Utility method used to serialize the relevant data required to repro a
  // crash.
  void persistReproInfo(
      const VectorPtr& inputVector,
      std::vector<column_index_t> columnsToWarpInLazy,
      const VectorPtr& resultVector,
      const std::string& sql,
      const std::vector<VectorPtr>& complexConstants);

 private:
  core::ExecCtx* FOLLY_NONNULL execCtx_;
  const ExpressionVerifierOptions options_;
};

} // namespace facebook::velox::test
