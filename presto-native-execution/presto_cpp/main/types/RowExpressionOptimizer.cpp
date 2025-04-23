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
#include "presto_cpp/main/types/RowExpressionOptimizer.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/FieldReference.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace facebook::presto::expression {

namespace {

const std::string kTimezoneHeader = "X-Presto-Time-Zone";

core::TypedExprPtr tryConstantFold(
    const core::TypedExprPtr& expr,
    std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  try {
    auto data = BaseVector::create<RowVector>(ROW({}), 1, pool);
    velox::core::ExecCtx execCtx{pool, queryCtx.get()};
    velox::exec::ExprSet exprSet({expr}, &execCtx, false);
    velox::exec::EvalCtx evalCtx(&execCtx, &exprSet, data.get());

    velox::SelectivityVector singleRow(1);
    std::vector<velox::VectorPtr> results(1);
    exprSet.eval(singleRow, evalCtx, results);
    return std::make_shared<core::ConstantTypedExpr>(results[0]);
  } catch (VeloxUserError& e) {
    VLOG(2) << e.what() << " <-> " << expr->toString();
    const auto error = std::string(e.what());
    if (error.find("Field not found") != std::string::npos) {
      return expr;
    } else {
      return std::make_shared<core::CallTypedExpr>(
          VARCHAR(),
          std::vector<core::TypedExprPtr>(
              {std::make_shared<core::ConstantTypedExpr>(VARCHAR(), e.what())}),
          "fail");
    }
  } catch (VeloxException& e) {
    VELOX_USER_FAIL(
        "Constant folding expression {} throws VeloxException.",
        expr->toString());
  } catch (std::exception& e) {
    VELOX_USER_FAIL(
        "Constant folding expression {} throws std::exception.",
        expr->toString());
  }
}
} // namespace

core::TypedExprPtr RowExpressionOptimizer::constantFold(
    const core::TypedExprPtr& expr) {
  core::TypedExprPtr result;
  std::vector<core::TypedExprPtr> foldedInputs;
  for (auto& input : expr->inputs()) {
    foldedInputs.push_back(constantFold(input));
  }

  bool isField = false;
  if (auto callExpr =
          std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    result = std::make_shared<core::CallTypedExpr>(
        callExpr->type(), foldedInputs, callExpr->name());
  } else if (
      auto castExpr =
          std::dynamic_pointer_cast<const core::CastTypedExpr>(expr)) {
    VELOX_CHECK(!foldedInputs.empty());
    if (foldedInputs.at(0)->type() == expr->type()) {
      result = foldedInputs.at(0);
    } else {
      result = std::make_shared<core::CastTypedExpr>(
          expr->type(), foldedInputs, castExpr->nullOnFailure());
    }
  } else if (
      auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
    return constantExpr;
  } else if (
      auto field =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
    isField = true;
    result = field;
  } else if (
      auto concatExpr =
          std::dynamic_pointer_cast<const core::ConcatTypedExpr>(expr)) {
    result = concatExpr;
  } else if (
      auto derefExpr =
          std::dynamic_pointer_cast<const core::DereferenceTypedExpr>(expr)) {
    auto inputExpr = derefExpr->inputs().at(0);
    auto idx = derefExpr->index();
    if (auto callExprInput =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(inputExpr)) {
      VELOX_CHECK_EQ(callExprInput->name(), "row_constructor");
      result = inputExpr->inputs().at(idx);
    } else {
      result = inputExpr;
    }
  } else {
    VELOX_FAIL("Unable to constant fold TypedExpr {}", expr->toString());
  }

  auto folded = !isField ? tryConstantFold(result, queryCtx_, pool_) : result;
  return folded;
}

core::TypedExprPtr RowExpressionOptimizer::optimizeIfExpression(
    const core::CallTypedExprPtr& expr) {
  auto condition = expr->inputs().at(0);
  auto folded = constantFold(condition);

  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(folded)) {
    if (auto constVector = constantExpr->toConstantVector(pool_)) {
      if (constVector->isNullAt(0) ||
          constVector->as<ConstantVector<bool>>()->valueAt(0)) {
        return expr->inputs().at(1);
      }
      return expr->inputs().at(2);
    }
  }
  return expr;
}

template <bool isAnd>
core::TypedExprPtr RowExpressionOptimizer::optimizeConjunctExpression(
    const core::CallTypedExprPtr& expr) {
  bool allInputsConstant = true;
  bool hasNullInput = false;
  std::vector<core::TypedExprPtr> optimizedInputs;
  core::TypedExprPtr nullInput = nullptr;
  for (const auto& input : expr->inputs()) {
    auto folded = constantFold(input);
    if (auto constantExpr =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(folded)) {
      auto constantVector = constantExpr->toConstantVector(pool_);
      if (!constantVector->isNullAt(0)) {
        if constexpr (isAnd) {
          if (!constantVector->as<ConstantVector<bool>>()->valueAt(0)) {
            return constantExpr;
          }
        } else {
          if (constantVector->as<ConstantVector<bool>>()->valueAt(0)) {
            return constantExpr;
          }
        }
      } else if (!hasNullInput) {
        hasNullInput = true;
        nullInput = input;
      }
    } else {
      allInputsConstant = false;
      optimizedInputs.push_back(input);
    }
  }

  if (allInputsConstant && hasNullInput) {
    return nullInput;
  } else if (optimizedInputs.empty()) {
    return expr->inputs().front();
  } else if (optimizedInputs.size() == 1) {
    return optimizedInputs.front();
  }
  return std::make_shared<core::CallTypedExpr>(
      expr->type(), optimizedInputs, expr->name());
}

core::TypedExprPtr RowExpressionOptimizer::addCoalesceArgument(
    const core::TypedExprPtr& input,
    std::set<core::TypedExprPtr, TypedExprComparator>& optimizedTypedExprs,
    std::vector<core::TypedExprPtr>& deduplicatedInputs) {
  auto folded = constantFold(input);
  // First non-NULL constant input to COALESCE returns non-NULL value.
  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(folded)) {
    auto constantVector = constantExpr->toConstantVector(pool_);
    if (!constantVector->isNullAt(0)) {
      if (optimizedTypedExprs.find(folded) == optimizedTypedExprs.end()) {
        optimizedTypedExprs.insert(folded);
        deduplicatedInputs.push_back(input);
      }
      return input;
    }
  } else if (optimizedTypedExprs.find(folded) == optimizedTypedExprs.end()) {
    optimizedTypedExprs.insert(folded);
    deduplicatedInputs.push_back(input);
  }

  return nullptr;
}

core::TypedExprPtr RowExpressionOptimizer::optimizeCoalesceExpression(
    const core::CallTypedExprPtr& expr,
    std::set<core::TypedExprPtr, TypedExprComparator>& inputTypedExprSet,
    std::vector<core::TypedExprPtr>& deduplicatedInputs) {
  for (const auto& input : expr->inputs()) {
    // If the argument is a COALESCE expression, the arguments of inner COALESCE
    // can be combined with the arguments of outer COALESCE expression.
    if (const auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(input)) {
      if (call->name() == "coalesce") {
        // If the inner COALESCE has a constant expression, return.
        if (auto optimizedCoalesceSubExpr = optimizeCoalesceExpression(
                call, inputTypedExprSet, deduplicatedInputs)) {
          return optimizedCoalesceSubExpr;
        }
      } else if (
          auto optimized = addCoalesceArgument(
              input, inputTypedExprSet, deduplicatedInputs)) {
        return optimized;
      }
    } else if (
        auto optimized =
            addCoalesceArgument(input, inputTypedExprSet, deduplicatedInputs)) {
      return optimized;
    }
  }
  return nullptr;
}

core::TypedExprPtr RowExpressionOptimizer::optimizeExpression(
    const core::TypedExprPtr& expr) {
  if (auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    auto name = call->name();
    if (name == "if") {
      return optimizeIfExpression(call);
    } else if (name == "and") {
      return optimizeConjunctExpression<true>(call);
    } else if (name == "or") {
      return optimizeConjunctExpression<false>(call);
    } else if (name == "coalesce") {
      std::set<core::TypedExprPtr, TypedExprComparator> inputTypedExprSet;
      std::vector<core::TypedExprPtr> deduplicatedInputs;
      optimizeCoalesceExpression(call, inputTypedExprSet, deduplicatedInputs);

      if (deduplicatedInputs.empty()) {
        return call->inputs().front();
      } else if (deduplicatedInputs.size() == 1) {
        return deduplicatedInputs.front();
      } else {
        return std::make_shared<core::CallTypedExpr>(
            call->type(), deduplicatedInputs, call->name());
      }
    }
  }

  return expr;
}

json::array_t RowExpressionOptimizer::optimizeExpressions(
    const json::array_t& input) {
  const auto numExpr = input.size();
  json::array_t output = json::array();
  for (auto i = 0; i < numExpr; i++) {
    VLOG(2) << "Optimizing input RowExpression JSON: " << input[i].dump();
    std::shared_ptr<protocol::RowExpression> inputRowExpr = input[i];
    auto expr = veloxExprConverter_.toVeloxExpr(inputRowExpr);
    auto optimized = optimizeExpression(expr);
    VLOG(2) << "Optimized: " << optimized->toString();
    auto folded = constantFold(optimized);
    json resultJson;
    VLOG(2) << "Folded: " << folded->toString();
    resultJson = rowExpressionConverter_.veloxToPrestoRowExpression(
        folded, inputRowExpr);
    VLOG(2) << "Optimized Presto RowExpression JSON: " << resultJson.dump();
    output.push_back(resultJson);
  }
  return output;
}

std::pair<json, bool> RowExpressionOptimizer::optimize(
    const proxygen::HTTPHeaders& httpHeaders,
    const json::array_t& input) {
  try {
    const auto& timezone = httpHeaders.getSingleOrEmpty(kTimezoneHeader);
    std::unordered_map<std::string, std::string> config(
        {{core::QueryConfig::kSessionTimezone, timezone},
         {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
    queryCtx_ =
        core::QueryCtx::create(nullptr, core::QueryConfig{std::move(config)});

    return {optimizeExpressions(input), true};
  } catch (const VeloxUserError& e) {
    VLOG(1) << "VeloxUserError during expression evaluation: " << e.what();
    return {e.what(), false};
  } catch (const VeloxException& e) {
    VLOG(1) << "VeloxException during expression evaluation: " << e.what();
    return {e.what(), false};
  } catch (const std::exception& e) {
    VLOG(1) << "std::exception during expression evaluation: " << e.what();
    return {e.what(), false};
  }
}

} // namespace facebook::presto::expression
