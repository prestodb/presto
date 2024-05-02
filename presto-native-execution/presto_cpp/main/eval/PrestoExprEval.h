/*
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

#include <proxygen/httpserver/ResponseHandler.h>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"

namespace facebook::presto::eval {

class PrestoExprEval {
 public:
  PrestoExprEval(std::shared_ptr<velox::memory::MemoryPool> pool)
      : pool_(pool),
        queryCtx_(std::make_shared<velox::core::QueryCtx>()),
        execCtx_{std::make_unique<velox::core::ExecCtx>(
            pool.get(),
            queryCtx_.get())},
        exprConverter_(pool.get(), &typeParser_) {};

  void registerUris(http::HttpServer& server);

  /// Evaluate expressions sent along /v1/expressions endpoint.
  void evaluateExpression(
      const std::vector<std::unique_ptr<folly::IOBuf>>& body,
      proxygen::ResponseHandler* downstream);

 protected:
  std::string getConstantValue(const velox::VectorPtr& input, const velox::TypePtr& type);

  json exprToRowExpression(std::shared_ptr<velox::exec::Expr> expr);

  const std::shared_ptr<velox::memory::MemoryPool> pool_;
  const std::shared_ptr<velox::core::QueryCtx> queryCtx_;
  const std::unique_ptr<velox::core::ExecCtx> execCtx_;
  VeloxExprConverter exprConverter_;
  TypeParser typeParser_;
  bool isLambda_;
  std::shared_ptr<const velox::core::LambdaTypedExpr> lambdaTypedExpr_;
};
} // namespace facebook::presto::eval
