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

#include "presto_cpp/main/types/ExpressionOptimizer.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/main/types/TypeParser.h"
#include "presto_cpp/main/types/VeloxToPrestoExpr.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/expression/ExprOptimizer.h"

namespace facebook::presto::expression {

constexpr char const* kOptimized = "OPTIMIZED";
constexpr char const* kTimezoneHeader = "X-Presto-Time-Zone";
constexpr char const* kOptimizerLevelHeader =
    "X-Presto-Expression-Optimizer-Level";

void optimizeExpressions(
    const proxygen::HTTPHeaders& httpHeaders,
    const json::array_t& inputRowExpressions,
    proxygen::ResponseHandler* downstream,
    folly::Executor* driverExecutor,
    velox::memory::MemoryPool* pool) {
  static const velox::expression::MakeFailExpr kMakeFailExpr =
      [](const std::string& error,
         const velox::TypePtr& type) -> velox::core::TypedExprPtr {
    return std::make_shared<velox::core::CastTypedExpr>(
        type,
        std::vector<velox::core::TypedExprPtr>{
            std::make_shared<velox::core::CallTypedExpr>(
                velox::UNKNOWN(),
                std::vector<velox::core::TypedExprPtr>{
                    std::make_shared<velox::core::ConstantTypedExpr>(
                        velox::VARCHAR(), error)},
                fmt::format(
                    "{}fail",
                    SystemConfig::instance()->prestoDefaultNamespacePrefix()))},
        false);
  };

  const auto& optimizerLevel =
      httpHeaders.getSingleOrEmpty(kOptimizerLevelHeader);
  VELOX_USER_CHECK_EQ(
      optimizerLevel,
      kOptimized,
      "Optimizer level should be OPTIMIZED, received {}.",
      optimizerLevel);

  const auto& timezone = httpHeaders.getSingleOrEmpty(kTimezoneHeader);
  std::unordered_map<std::string, std::string> config(
      {{velox::core::QueryConfig::kSessionTimezone, timezone},
       {velox::core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
  auto queryConfig = velox::core::QueryConfig{std::move(config)};
  auto queryCtx =
      velox::core::QueryCtx::create(driverExecutor, std::move(queryConfig));

  TypeParser typeParser;
  const VeloxExprConverter veloxExprConverter(pool, &typeParser);
  const expression::VeloxToPrestoExprConverter veloxToPrestoExprConverter(pool);
  const auto numExpr = inputRowExpressions.size();
  json j;
  json result = json::array();

  for (auto i = 0; i < numExpr; i++) {
    const RowExpressionPtr inputRowExpr = inputRowExpressions[i];
    protocol::to_json(j, inputRowExpr);
    auto expr = veloxExprConverter.toVeloxExpr(inputRowExpr);
    auto optimized =
        velox::expression::optimize(expr, queryCtx.get(), pool, kMakeFailExpr);

    auto resultExpression =
        veloxToPrestoExprConverter.getRowExpression(optimized, inputRowExpr);
    protocol::to_json(j, resultExpression);
    result.push_back(resultExpression);
  }

  http::sendOkResponse(downstream, result);
}

} // namespace facebook::presto::expression
