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

#include "FunctionBaseTest.h"

namespace facebook::velox::functions::test {

typedef int32_t ElementRowsPerIteration;

class LambdaParameterizedBaseTest
    : public FunctionBaseTest,
      public testing::WithParamInterface<ElementRowsPerIteration> {
 public:
  static std::vector<ElementRowsPerIteration> getTestParams() {
    const std::vector<ElementRowsPerIteration> params({0, 2, 1000});
    return params;
  }

  VectorPtr evaluateParameterized(
      const std::string& expression,
      const RowVectorPtr& data) {
    std::vector<core::TypedExprPtr> parseExpr = {
        parseExpression(expression, asRowType(data->type()))};
    auto exprSet = std::make_unique<exec::ExprSet>(
        std::move(parseExpr), &execCtxParametrized_);

    return evaluate(*exprSet, data);
  }

 private:
  std::shared_ptr<core::QueryCtx> queryCtxParametrized_{core::QueryCtx::create(
      executor_.get(),
      core::QueryConfig(
          {{core::QueryConfig::kDebugLambdaFunctionEvaluationBatchSize,
            std::to_string(GetParam())}}))};
  core::ExecCtx execCtxParametrized_{pool_.get(), queryCtxParametrized_.get()};
};

} // namespace facebook::velox::functions::test
