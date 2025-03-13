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
#include "velox/expression/Expr.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace facebook::presto::expression {

namespace {

const std::string kTimezoneHeader = "X-Presto-Time-Zone";
const std::string kOptimizerLevelHeader = "X-Presto-Expression-Optimizer-Level";
const std::string kEvaluated = "EVALUATED";

template <TypeKind KIND>
std::shared_ptr<exec::ConstantExpr> getConstantExpr(
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
    return std::make_shared<exec::ConstantExpr>(constVector);
  }
}
} // namespace

exec::ExprPtr RowExpressionOptimizer::compileExpression(
    const std::shared_ptr<protocol::RowExpression>& inputRowExpr) {
  auto typedExpr = veloxExprConverter_.toVeloxExpr(inputRowExpr);
  exec::ExprSet exprSet{{typedExpr}, execCtx_.get(), true};
  return exprSet.expr(0);
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
          std::dynamic_pointer_cast<const exec::ConstantExpr>(expr)) {
    if (constantExpr->value()->isNullAt(0)) {
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
          std::dynamic_pointer_cast<const exec::ConstantExpr>(leftExpr)) {
    isLeftNull = constantExpr->value()->isNullAt(0);
    if (!isLeftNull) {
      if (auto constVector =
              constantExpr->value()->as<ConstantVector<bool>>()) {
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
          std::dynamic_pointer_cast<const exec::ConstantExpr>(rightExpr)) {
    if (constantExpr->value()->isNullAt(0) && (isLeftNull || leftValue)) {
      return rowExpressionConverter_.getConstantRowExpression(constantExpr);
    }
    if (auto constVector = constantExpr->value()->as<ConstantVector<bool>>()) {
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
          std::dynamic_pointer_cast<const exec::ConstantExpr>(leftExpr)) {
    isLeftNull = constantExpr->value()->isNullAt(0);
    if (!isLeftNull) {
      if (auto constVector =
              constantExpr->value()->as<ConstantVector<bool>>()) {
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
          std::dynamic_pointer_cast<const exec::ConstantExpr>(rightExpr)) {
    if (constantExpr->value()->isNullAt(0) && (isLeftNull || !leftValue)) {
      return rowExpressionConverter_.getConstantRowExpression(constantExpr);
    }
    if (auto constVector = constantExpr->value()->as<ConstantVector<bool>>()) {
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

RowExpressionPtr RowExpressionOptimizer::optimizeCoalesceSpecialForm(
    const SpecialFormExpressionPtr& specialFormExpr) {
  auto argsNoNulls = specialFormExpr->arguments;
  argsNoNulls.erase(
      std::remove_if(
          argsNoNulls.begin(),
          argsNoNulls.end(),
          [&](const auto& arg) {
            auto compiledExpr = compileExpression(arg);
            if (auto constantExpr =
                    std::dynamic_pointer_cast<const exec::ConstantExpr>(
                        compiledExpr)) {
              if (constantExpr->value()->isNullAt(0)) {
                return true;
              }
            }
            return false;
          }),
      argsNoNulls.end());
  if (argsNoNulls.size() < specialFormExpr->arguments.size()) {
    specialFormExpr->arguments = argsNoNulls;
  }
  /*
    struct RowExpressionComparator {
      using is_transparent = void;  // important

      bool operator()(const std::shared_ptr<RowExpressionPtr>& a, const
    std::shared_ptr<RowExpressionPtr>& b) const { return *a < *b;
      }
    };
    auto cmp = [](RowExpressionPtr a, RowExpressionPtr b) {
      return (*a->sourceLocation == *b->sourceLocation);
    };
    std::unordered_set<RowExpressionPtr, decltype(cmp)> dedupArgs;
    for (const auto& arg: specialFormExpr->arguments) {
      if (dedupArgs.find(arg) == dedupArgs.end()) {
        dedupArgs.insert(arg);
      }
    }
    if (dedupArgs.size() < specialFormExpr->arguments.size()) {
      specialFormExpr->arguments.clear();
      for (const auto& arg : dedupArgs) {
        specialFormExpr->arguments.push_back(arg);
      }
    }*/

  if (argsNoNulls.empty()) {
    return specialFormExpr->arguments[0];
  } else if (argsNoNulls.size() == 1) {
    return argsNoNulls[0];
  }
  return specialFormExpr;
}

RowExpressionPtr RowExpressionOptimizer::optimizeSpecialForm(
    const std::shared_ptr<protocol::SpecialFormExpression>& specialFormExpr) {
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
    case protocol::Form::COALESCE:
      return optimizeCoalesceSpecialForm(specialFormExpr);
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
  const auto constExpr = VELOX_DYNAMIC_TYPE_DISPATCH(
      getConstantExpr, res->typeKind(), res->type(), decoded, pool_);
  return rowExpressionConverter_.getConstantRowExpression(constExpr);
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
    }

    auto typedExpr = veloxExprConverter_.toVeloxExpr(inputRowExpr);
    exec::ExprSet exprSet{{typedExpr}, execCtx_.get(), true};
    auto constantFolded = exprSet.expr(0);
    json resultJson;
    if (optimizerLevel == kEvaluated) {
      if (constantFolded->isConstant()) {
        resultJson = rowExpressionConverter_.veloxToPrestoRowExpression(
            constantFolded, input[i]);
      } else {
        // Velox does not evaluate expressions that are non-deterministic
        // during compilation with constant folding enabled. Presto might
        // require such non-deterministic expressions to be evaluated as well,
        // this would be indicated by the header field
        // 'X-Presto-Expression-Optimizer-Level' in the http request made to
        // the native sidecar. When this field is set to 'EVALUATED',
        // non-deterministic expressions with constant inputs are also
        // evaluated.
        exec::ExprSet exprSet{{typedExpr}, execCtx_.get(), true};
        resultJson = evaluateNonDeterministicConstantExpr(exprSet);
      }
    } else {
      resultJson = rowExpressionConverter_.veloxToPrestoRowExpression(
          constantFolded, input[i]);
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
