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
const std::string kOptimizerLevelHeader = "X-Presto-Expression-Optimizer-Level";
const std::string kEvaluated = "EVALUATED";

template <TypeKind KIND>
std::shared_ptr<const core::ConstantTypedExpr> getConstantExpr(
    const TypePtr& type,
    const DecodedVector& decoded,
    memory::MemoryPool* pool) {
  if constexpr (
      KIND == TypeKind::ROW || KIND == TypeKind::UNKNOWN ||
      KIND == TypeKind::ARRAY || KIND == TypeKind::MAP) {
    VELOX_FAIL("Invalid result type {}", type->toString());
  } else {
    using T = typename TypeTraits<KIND>::NativeType;
    auto constVector = std::make_shared<ConstantVector<T>>(
        pool, decoded.size(), decoded.isNullAt(0), type, decoded.valueAt<T>(0));
    return std::make_shared<core::ConstantTypedExpr>(constVector);
  }
}

core::TypedExprPtr exprToTypedExpr(const exec::ExprPtr& expr) {
  if (expr->isConstant()) {
    auto constantExpr =
        std::dynamic_pointer_cast<const exec::ConstantExpr>(expr);
    VELOX_CHECK_NOT_NULL(constantExpr);
    return std::make_shared<core::ConstantTypedExpr>(constantExpr->value());
  } else if (
      auto field =
          std::dynamic_pointer_cast<const exec::FieldReference>(expr)) {
    return std::make_shared<core::FieldAccessTypedExpr>(
        field->type(), field->name());
  } else {
    std::vector<core::TypedExprPtr> typedExprInputs;
    for (const auto& input : expr->inputs()) {
      typedExprInputs.push_back(exprToTypedExpr(input));
    }
    return std::make_shared<core::CallTypedExpr>(
        expr->type(), typedExprInputs, expr->name());
  }
}
} // namespace

core::TypedExprPtr RowExpressionOptimizer::compileExpression(
    const std::shared_ptr<protocol::RowExpression>& inputRowExpr) {
  auto typedExpr = veloxExprConverter_.toVeloxExpr(inputRowExpr);
  exec::ExprSet exprSet{{typedExpr}, execCtx_.get(), true};
  const auto& folded = exprSet.expr(0);
  return exprToTypedExpr(folded);
}

RowExpressionPtr RowExpressionOptimizer::optimizeIfSpecialForm(
    const SpecialFormExpressionPtr& specialFormExpr) {
  auto condition = specialFormExpr->arguments[0];
  auto expr = compileExpression(condition);

  if (auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(expr)) {
    if (auto constVector = constantExpr->value()->as<ConstantVector<bool>>()) {
      if (constVector->valueAt(0)) {
        return specialFormExpr->arguments[1];
      }
      return specialFormExpr->arguments[2];
    }
  }
  return specialFormExpr;
}

RowExpressionPtr RowExpressionOptimizer::optimizeIsNullSpecialForm(
    const SpecialFormExpressionPtr& specialFormExpr) {
  auto expr = compileExpression(specialFormExpr);
  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
    if (constantExpr->valueVector()->isNullAt(0)) {
      return rowExpressionConverter_.getConstantRowExpression(constantExpr);
    }
  }
  return specialFormExpr;
}

RowExpressionPtr RowExpressionOptimizer::optimizeAndSpecialForm(
    const SpecialFormExpressionPtr& specialFormExpr) {
  auto left = specialFormExpr->arguments[0];
  auto right = specialFormExpr->arguments[1];
  auto leftExpr = compileExpression(left);
  bool isLeftNull = false;
  bool leftValue = false;

  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(leftExpr)) {
    isLeftNull = constantExpr->valueVector()->isNullAt(0);
    if (!isLeftNull) {
      if (auto constVector =
              constantExpr->valueVector()->as<ConstantVector<bool>>()) {
        if (!constVector->valueAt(0)) {
          return rowExpressionConverter_.getConstantRowExpression(constantExpr);
        } else {
          leftValue = true;
        }
      }
    }
  }

  auto rightExpr = compileExpression(right);
  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(rightExpr)) {
    if (constantExpr->valueVector()->isNullAt(0) && (isLeftNull || leftValue)) {
      return rowExpressionConverter_.getConstantRowExpression(constantExpr);
    }
    if (auto constVector =
            constantExpr->valueVector()->as<ConstantVector<bool>>()) {
      if (constVector->valueAt(0)) {
        return left;
      } else {
        return rowExpressionConverter_.getConstantRowExpression(constantExpr);
      }
    }
  }
  if (leftValue) {
    return right;
  }
  return specialFormExpr;
}

RowExpressionPtr RowExpressionOptimizer::optimizeOrSpecialForm(
    const SpecialFormExpressionPtr& specialFormExpr) {
  auto left = specialFormExpr->arguments[0];
  auto right = specialFormExpr->arguments[1];
  auto leftExpr = compileExpression(left);
  bool isLeftNull = false;
  bool leftValue = true;

  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(leftExpr)) {
    isLeftNull = constantExpr->valueVector()->isNullAt(0);
    if (!isLeftNull) {
      if (auto constVector =
              constantExpr->valueVector()->as<ConstantVector<bool>>()) {
        if (constVector->valueAt(0)) {
          return rowExpressionConverter_.getConstantRowExpression(constantExpr);
        } else {
          leftValue = false;
        }
      }
    }
  }

  auto rightExpr = compileExpression(right);
  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(rightExpr)) {
    if (constantExpr->valueVector()->isNullAt(0) &&
        (isLeftNull || !leftValue)) {
      return rowExpressionConverter_.getConstantRowExpression(constantExpr);
    }
    if (auto constVector =
            constantExpr->valueVector()->as<ConstantVector<bool>>()) {
      if (!constVector->valueAt(0)) {
        return left;
      } else {
        return rowExpressionConverter_.getConstantRowExpression(constantExpr);
      }
    }
  }
  if (!leftValue) {
    return right;
  }
  return specialFormExpr;
}

RowExpressionPtr RowExpressionOptimizer::addCoalesceArgument(
    const RowExpressionPtr& input,
    std::set<core::TypedExprPtr, TypedExprComparator>& optimizedTypedExprs,
    std::vector<RowExpressionPtr>& optimizedRowExpressions) {
  auto compiledExpr = compileExpression(input);
  // First non-NULL constant input to COALESCE returns non-NULL value.
  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
              compiledExpr)) {
    if (!constantExpr->valueVector()->isNullAt(0)) {
      if (optimizedTypedExprs.find(compiledExpr) == optimizedTypedExprs.end()) {
        optimizedTypedExprs.insert(compiledExpr);
        optimizedRowExpressions.push_back(input);
      }
      return input;
    }
  } else if (
      optimizedTypedExprs.find(compiledExpr) == optimizedTypedExprs.end()) {
    optimizedTypedExprs.insert(compiledExpr);
    optimizedRowExpressions.push_back(input);
  }

  return nullptr;
}

RowExpressionPtr RowExpressionOptimizer::optimizeCoalesceSpecialForm(
    const SpecialFormExpressionPtr& specialFormExpr,
    std::set<core::TypedExprPtr, TypedExprComparator>& optimizedTypedExprSet,
    std::vector<RowExpressionPtr>& optimizedRowExpressions) {
  auto inputArgs = specialFormExpr->arguments;
  for (auto& inputRowExpr : inputArgs) {
    // If the argument is a COALESCE expression, the arguments of inner COALESCE
    // can be combined with the arguments of outer COALESCE expression.
    if (const auto special =
            std::dynamic_pointer_cast<protocol::SpecialFormExpression>(
                inputRowExpr)) {
      if (special->form == protocol::Form::COALESCE) {
        // If the inner COALESCE has a constant expression, return.
        if (auto optimizedCoalesceSubExpr = optimizeCoalesceSpecialForm(
                special, optimizedTypedExprSet, optimizedRowExpressions)) {
          return optimizedCoalesceSubExpr;
        }
      } else if (
          auto optimized = addCoalesceArgument(
              inputRowExpr, optimizedTypedExprSet, optimizedRowExpressions)) {
        return optimized;
      }
    } else if (
        auto optimized = addCoalesceArgument(
            inputRowExpr, optimizedTypedExprSet, optimizedRowExpressions)) {
      return optimized;
    }
  }
  return nullptr;
}

RowExpressionPtr RowExpressionOptimizer::optimizeSpecialForm(
    const SpecialFormExpressionPtr& specialFormExpr) {
  switch (specialFormExpr->form) {
    case protocol::Form::NULL_IF:
      VELOX_USER_FAIL("NULL_IF specialForm not supported");
      break;
    case protocol::Form::IF:
      return optimizeIfSpecialForm(specialFormExpr);
    case protocol::Form::IS_NULL:
      return optimizeIsNullSpecialForm(specialFormExpr);
    case protocol::Form::AND:
      return optimizeAndSpecialForm(specialFormExpr);
    case protocol::Form::OR:
      return optimizeOrSpecialForm(specialFormExpr);
    case protocol::Form::COALESCE: {
      std::set<core::TypedExprPtr, TypedExprComparator> optimizedTypedExprSet;
      std::vector<RowExpressionPtr> optimizedRowExpressions;
      optimizeCoalesceSpecialForm(
          specialFormExpr, optimizedTypedExprSet, optimizedRowExpressions);

      if (optimizedRowExpressions.empty()) {
        return specialFormExpr->arguments.front();
      } else if (optimizedRowExpressions.size() == 1) {
        return optimizedRowExpressions.front();
      } else {
        specialFormExpr->arguments.clear();
        specialFormExpr->arguments = optimizedRowExpressions;
      }
    }
    case protocol::Form::IN:
    case protocol::Form::DEREFERENCE:
    case protocol::Form::SWITCH:
    case protocol::Form::WHEN:
    case protocol::Form::ROW_CONSTRUCTOR:
    case protocol::Form::BIND:
    default:
      break;
  }

  return specialFormExpr;
}

json RowExpressionOptimizer::evaluateNonDeterministicConstantExpr(
    exec::ExprSet& exprSet) {
  auto expr = exprSet.expr(0);
  VELOX_CHECK(!expr->isDeterministic());
  std::vector<TypePtr> compiledExprInputTypes;
  std::vector<VectorPtr> compiledExprInputs;
  for (const auto& exprInput : expr->inputs()) {
    VELOX_CHECK(
        exprInput->isConstant(),
        "Inputs to non-deterministic expression to be evaluated must be constant");
    const auto inputAsConstExpr =
        std::dynamic_pointer_cast<const exec::ConstantExpr>(exprInput);
    compiledExprInputs.emplace_back(inputAsConstExpr->value());
    compiledExprInputTypes.emplace_back(exprInput->type());
  }

  const auto inputVector = std::make_shared<RowVector>(
      pool_,
      ROW(std::move(compiledExprInputTypes)),
      nullptr,
      1,
      compiledExprInputs);
  exec::EvalCtx evalCtx(execCtx_.get(), &exprSet, inputVector.get());
  std::vector<VectorPtr> results(1);
  SelectivityVector rows(1);
  exprSet.eval(rows, evalCtx, results);
  auto res = results.front();
  DecodedVector decoded(*res, rows);
  const auto constantExpr = VELOX_DYNAMIC_TYPE_DISPATCH(
      getConstantExpr, res->typeKind(), res->type(), decoded, pool_);
  return rowExpressionConverter_.getConstantRowExpression(constantExpr);
}

json::array_t RowExpressionOptimizer::optimizeExpressions(
    const json::array_t& input,
    const std::string& optimizerLevel) {
  const auto numExpr = input.size();
  json::array_t output = json::array();
  for (auto i = 0; i < numExpr; i++) {
    VLOG(2) << "Optimizing input RowExpression JSON: " << input[i].dump();
    std::shared_ptr<protocol::RowExpression> inputRowExpr = input[i];
    if (const auto special =
            std::dynamic_pointer_cast<protocol::SpecialFormExpression>(
                inputRowExpr)) {
      inputRowExpr = optimizeSpecialForm(special);
      json sj;
      protocol::to_json(sj, inputRowExpr);
      VLOG(2) << "Optimized SpecialFormExpression JSON: " << sj.dump();
    }

    auto typedExpr = veloxExprConverter_.toVeloxExpr(inputRowExpr);
    exec::ExprSet exprSet{{typedExpr}, execCtx_.get(), true};
    const auto& folded = exprSet.expr(0);
    auto constantFoldedTypedExpr = exprToTypedExpr(folded);
    json resultJson;
    if (optimizerLevel == kEvaluated) {
      if (folded->isConstant()) {
        resultJson = rowExpressionConverter_.veloxToPrestoRowExpression(
            constantFoldedTypedExpr, inputRowExpr);
      } else {
        // Velox does not evaluate expressions that are non-deterministic
        // during compilation with constant folding enabled. Presto might
        // require such non-deterministic expressions to be evaluated as well,
        // this would be indicated by the header field
        // 'X-Presto-Expression-Optimizer-Level' in the http request made to
        // the native sidecar. When this field is set to 'EVALUATED',
        // non-deterministic expressions with constant inputs are also
        // evaluated.
        exec::ExprSet nonDeterministicExprSet{
            {typedExpr}, execCtx_.get(), true};
        resultJson =
            evaluateNonDeterministicConstantExpr(nonDeterministicExprSet);
      }
    } else {
      resultJson = rowExpressionConverter_.veloxToPrestoRowExpression(
          constantFoldedTypedExpr, inputRowExpr);
    }

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
    const auto& optimizerLevel =
        httpHeaders.getSingleOrEmpty(kOptimizerLevelHeader);
    std::unordered_map<std::string, std::string> config(
        {{core::QueryConfig::kSessionTimezone, timezone},
         {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
    auto queryCtx =
        core::QueryCtx::create(nullptr, core::QueryConfig{std::move(config)});
    execCtx_ = std::make_unique<core::ExecCtx>(pool_, queryCtx.get());

    return {optimizeExpressions(input, optimizerLevel), true};
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
