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
#include "presto_cpp/main/expression/RowExpressionOptimizer.h"
#include <proxygen/httpserver/ResponseBuilder.h>
#include "presto_cpp/main/common/Utils.h"
#include "velox/common/encode/Base64.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprCompiler.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/VectorFunction.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace facebook::presto::expression {

namespace {
const std::string kConstant = "constant";
const std::string kBoolean = "boolean";
const std::string kVariable = "variable";
const std::string kCall = "call";
const std::string kStatic = "$static";
const std::string kSpecial = "special";
const std::string kRowConstructor = "ROW_CONSTRUCTOR";
const std::string kSwitch = "SWITCH";
const std::string kWhen = "WHEN";

const std::string kTimezoneHeader = "X-Presto-Time-Zone";
const std::string kCurrentUserHeader = "X-Presto-User";
const std::string kOptimizerLevelHeader = "X-Presto-Expression-Optimizer-Level";
const std::string kCurrentUser = "$current_user";
const std::string kEvaluated = "EVALUATED";
const std::string kOptimized = "OPTIMIZED";

protocol::TypeSignature getTypeSignature(const TypePtr& type) {
  std::string typeSignature;
  if (type->parameters().empty()) {
    typeSignature = type->toString();
    boost::algorithm::to_lower(typeSignature);
  } else if (type->isDecimal()) {
    typeSignature = type->toString();
  } else {
    std::vector<TypePtr> childTypes;
    if (type->isRow()) {
      typeSignature = "row(";
      childTypes = asRowType(type)->children();
    } else if (type->isArray()) {
      typeSignature = "array(";
      childTypes = type->asArray().children();
    } else if (type->isMap()) {
      typeSignature = "map(";
      const auto mapType = type->asMap();
      childTypes = {mapType.keyType(), mapType.valueType()};
    } else {
      VELOX_USER_FAIL("Invalid type {}", type->toString());
    }

    if (!childTypes.empty()) {
      auto numChildren = childTypes.size();
      for (auto i = 0; i < numChildren - 1; i++) {
        typeSignature += fmt::format("{},", getTypeSignature(childTypes[i]));
      }
      typeSignature += getTypeSignature(childTypes[numChildren - 1]);
    }
    typeSignature += ")";
  }

  return typeSignature;
}

json toVariableReferenceExpression(
    const std::shared_ptr<const exec::FieldReference>& fieldReference) {
  protocol::VariableReferenceExpression vexpr;
  vexpr.name = fieldReference->name();
  vexpr._type = kVariable;
  vexpr.type = getTypeSignature(fieldReference->type());
  json res;
  protocol::to_json(res, vexpr);

  return res;
}

bool isPrestoSpecialForm(const std::string& name) {
  static const std::unordered_set<std::string> kPrestoSpecialForms = {
      "if",
      "null_if",
      "switch",
      "when",
      "is_null",
      "coalesce",
      "in",
      "and",
      "or",
      "dereference",
      "row_constructor",
      "bind"};
  return kPrestoSpecialForms.count(name) != 0;
}

json::array_t getInputExpressions(
    const std::vector<std::unique_ptr<folly::IOBuf>>& body) {
  std::ostringstream oss;
  for (auto& buf : body) {
    oss << std::string((const char*)buf->data(), buf->length());
  }
  return json::parse(oss.str());
}

template <TypeKind KIND>
std::shared_ptr<exec::ConstantExpr> getConstantExpr(
    const TypePtr& type,
    const DecodedVector& decoded,
    memory::MemoryPool* pool) {
  if constexpr (
      KIND == TypeKind::ROW || KIND == TypeKind::UNKNOWN ||
      KIND == TypeKind::ARRAY || KIND == TypeKind::MAP) {
    VELOX_USER_FAIL("Invalid result type {}", type->toString());
  } else {
    using T = typename TypeTraits<KIND>::NativeType;
    auto constVector = std::make_shared<ConstantVector<T>>(
        pool, decoded.size(), decoded.isNullAt(0), type, decoded.valueAt<T>(0));
    return std::make_shared<exec::ConstantExpr>(constVector);
  }
}
} // namespace

// ValueBlock in ConstantExpression requires only the column from the serialized
// PrestoPage without the page header.
std::string RowExpressionConverter::getValueBlock(const VectorPtr& vector) {
  std::ostringstream output;
  serde_->serializeSingleColumn(vector, nullptr, pool_, &output);
  const auto serialized = output.str();
  const auto serializedSize = serialized.size();
  return encoding::Base64::encode(serialized.c_str(), serializedSize);
}

std::shared_ptr<protocol::ConstantExpression>
RowExpressionConverter::getConstantRowExpression(
    const std::shared_ptr<const exec::ConstantExpr>& constantExpr) {
  protocol::ConstantExpression cexpr;
  cexpr.type = getTypeSignature(constantExpr->type());
  cexpr.valueBlock.data = getValueBlock(constantExpr->value());
  return std::make_shared<protocol::ConstantExpression>(cexpr);
}

// TODO: Remove this once native plugin supports evaluation of current_user.
std::shared_ptr<protocol::ConstantExpression>
RowExpressionConverter::getCurrentUser(const std::string& currentUser) {
  protocol::ConstantExpression cexpr;
  cexpr.type = getTypeSignature(VARCHAR());
  cexpr.valueBlock.data = getValueBlock(
      BaseVector::createConstant(VARCHAR(), currentUser, 1, pool_));
  return std::make_shared<protocol::ConstantExpression>(cexpr);
}

// When the second value in the returned pair is true, the arguments for switch
// special form are returned. Otherwise, the switch expression has been
// simplified and the first value corresponding to the switch case that always
// evaluates to true is returned.
std::pair<json::array_t, bool> RowExpressionConverter::getSwitchSpecialFormArgs(
    const exec::ExprPtr& expr,
    const json& input) {
  json::array_t inputArgs = input["arguments"];
  auto numArgs = inputArgs.size();
  json::array_t result = json::array();
  const std::vector<exec::ExprPtr> exprInputs = expr->inputs();
  const auto numInputs = exprInputs.size();

  auto getWhenSpecialForm = [&](const json::array_t& whenArgs,
                                const vector_size_t idx) -> json {
    json when;
    when["@type"] = kSpecial;
    when["form"] = kWhen;
    when["arguments"] = whenArgs;
    when["returnType"] = getTypeSignature(exprInputs[idx + 1]->type());
    return when;
  };

  // The searched form of the conditional expression needs to be handled
  // differently from the simple form. The searched form can be detected by the
  // presence of a boolean value in the first argument. This default boolean
  // argument is not present in the Velox switch expression, so it is added to
  // the arguments of output switch expression unchanged.
  if (inputArgs[0].at("@type") == kConstant &&
      inputArgs[0].at("type") == kBoolean) {
    result.emplace_back(inputArgs[0]);
    for (auto i = 0; i < numInputs - 1; i += 2) {
      const vector_size_t argsIdx = i / 2 + 1;
      json::array_t inputWhenArgs = inputArgs[argsIdx].at("arguments");
      json::array_t whenArgs;
      whenArgs.emplace_back(
          veloxToPrestoRowExpression(exprInputs[i], inputWhenArgs[0]));
      whenArgs.emplace_back(
          veloxToPrestoRowExpression(exprInputs[i + 1], inputWhenArgs[1]));

      result.emplace_back(getWhenSpecialForm(whenArgs, i));
    }
  } else {
    // The case 'expression' in simple form of conditional cannot be inferred
    // from Velox since it could evaluate all when clauses to true or false, so
    // we get it from the input json.
    result.emplace_back(inputArgs[0]);
    for (auto i = 0; i < numInputs - 1; i += 2) {
      json::array_t whenArgs;
      const vector_size_t argsIdx = i / 2 + 1;
      const auto& caseValue = exprInputs[i + 1];
      json::array_t inputWhenArgs = inputArgs[argsIdx].at("arguments");

      if (exprInputs[i]->isConstant()) {
        auto constantExpr =
            std::dynamic_pointer_cast<const exec::ConstantExpr>(exprInputs[i]);
        if (auto constVector =
                constantExpr->value()->as<ConstantVector<bool>>()) {
          if (constVector->valueAt(0)) {
            if (result.size() == 1) {
              // This is the first case statement that evaluates to true, so
              // return the expression corresponding to this case.
              return {
                  json::array({veloxToPrestoRowExpression(
                      caseValue, inputWhenArgs[1])}),
                  false};
            } else {
              // If the case has been constant folded to false in the Velox
              // switch expression, we do not have access to the expression
              // inputs in Velox anymore. So we return the corresponding
              // argument from the input switch expression.
              result.emplace_back(inputArgs[argsIdx]);
            }
          } else {
            // Skip cases that evaluate to false from the output switch
            // expression's arguments.
            continue;
          }
        } else {
          whenArgs.emplace_back(getConstantRowExpression(constantExpr));
        }
      } else {
        VELOX_USER_CHECK(!exprInputs[i]->inputs().empty());
        const auto& matchExpr = exprInputs[i]->inputs().back();
        whenArgs.emplace_back(
            veloxToPrestoRowExpression(matchExpr, inputWhenArgs[0]));
      }

      whenArgs.emplace_back(
          veloxToPrestoRowExpression(caseValue, inputWhenArgs[1]));
      result.emplace_back(getWhenSpecialForm(whenArgs, i));
    }
  }

  // Else clause.
  if (numInputs % 2 != 0) {
    result.push_back(veloxToPrestoRowExpression(
        exprInputs[numInputs - 1], inputArgs[numArgs - 1]));
  }
  return {result, true};
}

json RowExpressionConverter::getSpecialForm(
    const exec::ExprPtr& expr,
    const json& input) {
  json res;
  res["@type"] = kSpecial;
  auto form = expr->name();
  // Presto requires the field form to be in upper case.
  std::transform(form.begin(), form.end(), form.begin(), ::toupper);
  res["form"] = form;
  auto exprInputs = expr->inputs();
  res["arguments"] = json::array();

  // Arguments for switch expression include special form expression 'when'
  // so it is constructed separately. If the switch expression evaluation found
  // a case that always evaluates to true, the second value in pair switchResult
  // will be false and the first value in pair will contain the value
  // corresponding to the simplified case.
  if (form == kSwitch) {
    auto switchResult = getSwitchSpecialFormArgs(expr, input);
    if (switchResult.second) {
      res["arguments"] = switchResult.first;
    } else {
      return switchResult.first.front();
    }
  } else {
    json::array_t inputArguments = input["arguments"];
    const auto numInputs = exprInputs.size();
    VELOX_USER_CHECK_LE(numInputs, inputArguments.size());
    for (auto i = 0; i < numInputs; i++) {
      res["arguments"].push_back(
          veloxToPrestoRowExpression(exprInputs[i], inputArguments[i]));
    }
  }
  res["returnType"] = getTypeSignature(expr->type());

  return res;
}

json RowExpressionConverter::toConstantRowExpression(
    const exec::ExprPtr& expr) {
  auto constantExpr = std::dynamic_pointer_cast<const exec::ConstantExpr>(expr);
  VELOX_USER_CHECK_NOT_NULL(constantExpr);

  json res;
  // Constant velox expressions of ROW type map to ROW_CONSTRUCTOR special form
  // expression in Presto.
  if (expr->type()->isRow()) {
    res["@type"] = kSpecial;
    res["form"] = kRowConstructor;
    res["returnType"] = getTypeSignature(expr->type());
    auto value = constantExpr->value();
    auto* constVector = value->as<ConstantVector<ComplexType>>();
    auto* rowVector = constVector->valueVector()->as<RowVector>();
    auto type = asRowType(constantExpr->type());
    auto size = rowVector->children().size();

    protocol::ConstantExpression cexpr;
    json j;
    res["arguments"] = json::array();
    for (auto i = 0; i < size; i++) {
      cexpr.type = getTypeSignature(type->childAt(i));
      cexpr.valueBlock.data = getValueBlock(rowVector->childAt(i));
      protocol::to_json(j, cexpr);
      res["arguments"].push_back(j);
    }
  } else {
    auto constantRowExpr = getConstantRowExpression(constantExpr);
    protocol::to_json(res, constantRowExpr);
  }
  return res;
}

json RowExpressionConverter::toCallRowExpression(
    const exec::ExprPtr& expr,
    const json& input) {
  json res;
  res["@type"] = kCall;
  protocol::Signature signature;
  std::string exprName = expr->name();
  if (veloxToPrestoOperatorMap_.find(exprName) !=
      veloxToPrestoOperatorMap_.end()) {
    exprName = veloxToPrestoOperatorMap_.at(exprName);
  }
  signature.name = exprName;
  res["displayName"] = exprName;
  signature.kind = protocol::FunctionKind::SCALAR;
  signature.typeVariableConstraints = {};
  signature.longVariableConstraints = {};
  signature.returnType = getTypeSignature(expr->type());

  std::vector<protocol::TypeSignature> argumentTypes;
  auto exprInputs = expr->inputs();
  auto numArgs = exprInputs.size();
  argumentTypes.reserve(numArgs);
  for (auto i = 0; i < numArgs; i++) {
    argumentTypes.emplace_back(getTypeSignature(exprInputs[i]->type()));
  }
  signature.argumentTypes = argumentTypes;
  signature.variableArity = false;

  protocol::BuiltInFunctionHandle builtInFunctionHandle;
  builtInFunctionHandle._type = kStatic;
  builtInFunctionHandle.signature = signature;
  res["functionHandle"] = builtInFunctionHandle;
  res["returnType"] = getTypeSignature(expr->type());
  res["arguments"] = json::array();
  for (const auto& exprInput : exprInputs) {
    res["arguments"].push_back(veloxToPrestoRowExpression(exprInput, input));
  }

  return res;
}

json RowExpressionConverter::veloxToPrestoRowExpression(
    const exec::ExprPtr& expr,
    const json& input) {
  if (expr->isConstant()) {
    if (expr->inputs().empty()) {
      return toConstantRowExpression(expr);
    } else {
      // Inputs to constant expressions are constant, eg: divide(1, 2).
      return input;
    }
  }

  if (auto field =
          std::dynamic_pointer_cast<const exec::FieldReference>(expr)) {
    return toVariableReferenceExpression(field);
  }

  if (expr->isSpecialForm() || expr->vectorFunction()) {
    // Check if special form expression or call expression.
    auto exprName = expr->name();
    boost::algorithm::to_lower(exprName);
    if (isPrestoSpecialForm(exprName)) {
      return getSpecialForm(expr, input);
    } else {
      return toCallRowExpression(expr, input);
    }
  }

  VELOX_NYI(
      "Conversion of Velox Expr {} to Presto RowExpression is not supported",
      expr->toString());
}

exec::ExprPtr RowExpressionOptimizer::compileExpression(
    const std::shared_ptr<protocol::RowExpression>& inputRowExpr) {
  auto typedExpr = veloxExprConverter_.toVeloxExpr(inputRowExpr);
  exec::ExprSet exprSet{{typedExpr}, execCtx_.get()};
  auto compiledExprs =
      exec::compileExpressions({typedExpr}, execCtx_.get(), &exprSet, true);
  return compiledExprs[0];
}

RowExpressionPtr RowExpressionOptimizer::optimizeAndSpecialForm(
    const SpecialFormExpressionPtr& specialFormExpr) {
  auto left = specialFormExpr->arguments[0];
  auto right = specialFormExpr->arguments[1];
  auto leftExpr = compileExpression(left);
  bool isLeftNull;

  if (auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(leftExpr)) {
    isLeftNull = constantExpr->value()->isNullAt(0);
    if (!isLeftNull) {
      if (auto constVector =
              constantExpr->value()->as<ConstantVector<bool>>()) {
        if (!constVector->valueAt(0)) {
          return rowExpressionConverter_.getConstantRowExpression(constantExpr);
        } else {
          return right;
        }
      }
    }
  }

  auto rightExpr = compileExpression(right);
  if (auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(rightExpr)) {
    if (isLeftNull && constantExpr->value()->isNullAt(0)) {
      return rowExpressionConverter_.getConstantRowExpression(constantExpr);
    }
    if (auto constVector = constantExpr->value()->as<ConstantVector<bool>>()) {
      if (constVector->valueAt(0)) {
        return left;
      }
      return right;
    }
  }

  return specialFormExpr;
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

RowExpressionPtr RowExpressionOptimizer::optimizeOrSpecialForm(
    const SpecialFormExpressionPtr& specialFormExpr) {
  auto left = specialFormExpr->arguments[0];
  auto right = specialFormExpr->arguments[1];
  auto leftExpr = compileExpression(left);
  bool isLeftNull;

  if (auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(leftExpr)) {
    isLeftNull = constantExpr->value()->isNullAt(0);
    if (!isLeftNull) {
      if (auto constVector =
              constantExpr->value()->as<ConstantVector<bool>>()) {
        if (constVector->valueAt(0)) {
          return rowExpressionConverter_.getConstantRowExpression(constantExpr);
        }
        return right;
      }
    }
  }

  auto rightExpr = compileExpression(right);
  if (auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(rightExpr)) {
    if (isLeftNull && constantExpr->value()->isNullAt(0)) {
      return rowExpressionConverter_.getConstantRowExpression(constantExpr);
    }
    if (auto constVector = constantExpr->value()->as<ConstantVector<bool>>()) {
      if (!constVector->valueAt(0)) {
        return left;
      }
      return right;
    }
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
              return constantExpr->value()->isNullAt(0);
            }
            return false;
          }),
      argsNoNulls.end());

  if (argsNoNulls.empty()) {
    return specialFormExpr->arguments[0];
  }
  specialFormExpr->arguments = argsNoNulls;
  return specialFormExpr;
}

RowExpressionPtr RowExpressionOptimizer::optimizeSpecialForm(
    const std::shared_ptr<protocol::SpecialFormExpression>& specialFormExpr) {
  switch (specialFormExpr->form) {
    case protocol::Form::IF:
      return optimizeIfSpecialForm(specialFormExpr);
    case protocol::Form::NULL_IF:
      VELOX_USER_FAIL("NULL_IF specialForm not supported");
      break;
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

json::array_t RowExpressionOptimizer::optimizeExpressions(
    const json::array_t& input,
    const std::string& optimizerLevel,
    const std::string& currentUser) {
  const auto numExpr = input.size();
  json::array_t output = json::array();
  for (auto i = 0; i < numExpr; i++) {
    // TODO: current_user to be evaluated in the native plugin and will not be
    //  sent to the sidecar.
    if (input[i].contains("displayName") &&
        input[i].at("displayName") == kCurrentUser) {
      output.emplace_back(rowExpressionConverter_.getCurrentUser(currentUser));
      continue;
    }

    std::shared_ptr<protocol::RowExpression> inputRowExpr = input[i];
    if (const auto special =
            std::dynamic_pointer_cast<protocol::SpecialFormExpression>(
                inputRowExpr)) {
      inputRowExpr = optimizeSpecialForm(special);
    }
    auto typedExpr = veloxExprConverter_.toVeloxExpr(inputRowExpr);
    exec::ExprSet exprSet{{typedExpr}, execCtx_.get()};
    auto compiledExprs =
        exec::compileExpressions({typedExpr}, execCtx_.get(), &exprSet, true);
    auto compiledExpr = compiledExprs[0];
    json resultJson;

    if (optimizerLevel == kEvaluated) {
      if (compiledExpr->isConstant()) {
        resultJson = rowExpressionConverter_.veloxToPrestoRowExpression(
            compiledExpr, input[i]);
      } else {
        // Evaluate non-deterministic expressions with constant inputs.
        VELOX_USER_CHECK(!compiledExpr->isDeterministic());
        std::vector<TypePtr> compiledExprInputTypes;
        std::vector<VectorPtr> compiledExprInputs;
        for (const auto& exprInput : compiledExpr->inputs()) {
          VELOX_USER_CHECK(
              exprInput->isConstant(),
              "Inputs to non-deterministic expression to be evaluated must be constant");
          const auto inputAsConstExpr =
              std::dynamic_pointer_cast<const exec::ConstantExpr>(exprInput);
          compiledExprInputs.emplace_back(inputAsConstExpr->value());
          compiledExprInputTypes.emplace_back(exprInput->type());
        }

        const auto inputVector = std::make_shared<RowVector>(
            pool_.get(),
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
        VELOX_USER_CHECK(decoded.size() == 1);
        const auto constExpr = VELOX_DYNAMIC_TYPE_DISPATCH(
            getConstantExpr,
            res->typeKind(),
            res->type(),
            decoded,
            pool_.get());
        resultJson =
            rowExpressionConverter_.getConstantRowExpression(constExpr);
      }
    } else if (optimizerLevel == kOptimized) {
      resultJson = rowExpressionConverter_.veloxToPrestoRowExpression(
          compiledExpr, input[i]);
    } else {
      VELOX_NYI("Invalid optimizer level: {}", optimizerLevel);
    }

    output.push_back(resultJson);
  }
  return output;
}

std::pair<json, bool> RowExpressionOptimizer::optimize(
    proxygen::HTTPMessage* message,
    const json::array_t& input) {
  try {
    auto timezone = message->getHeaders().getSingleOrEmpty(kTimezoneHeader);
    auto currentUser =
        message->getHeaders().getSingleOrEmpty(kCurrentUserHeader);
    auto optimizerLevel =
        message->getHeaders().getSingleOrEmpty(kOptimizerLevelHeader);
    std::unordered_map<std::string, std::string> config(
        {{core::QueryConfig::kSessionTimezone, timezone},
         {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
    auto queryCtx =
        core::QueryCtx::create(nullptr, core::QueryConfig{std::move(config)});
    execCtx_ = std::make_unique<core::ExecCtx>(pool_.get(), queryCtx.get());

    return {optimizeExpressions(input, optimizerLevel, currentUser), true};
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
