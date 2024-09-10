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
#include "presto_cpp/main/expression/RowExpressionEvaluator.h"
#include <proxygen/httpserver/ResponseBuilder.h>
#include "presto_cpp/main/common/Utils.h"
#include "velox/common/encode/Base64.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprCompiler.h"
#include "velox/expression/FieldReference.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace facebook::presto::expression {

namespace {

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
  vexpr._type = "variable";
  vexpr.type = getTypeSignature(fieldReference->type());
  json res;
  protocol::to_json(res, vexpr);

  return res;
}

bool isPrestoSpecialForm(const std::string& name) {
  static const std::unordered_set<std::string> kPrestoSpecialForms = {
      "and",
      "coalesce",
      "if",
      "in",
      "is_null",
      "or",
      "switch",
      "when",
      "null_if"};
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
} // namespace

// ValueBlock in ConstantExpression requires only the column from the serialized
// PrestoPage without the page header.
std::string RowExpressionConverter::getValueBlock(const VectorPtr& vector) {
  std::ostringstream output;
  serde_->serializeSingleColumn(vector, nullptr, pool_.get(), &output);
  const auto serialized = output.str();
  const auto serializedSize = serialized.size();
  return velox::encoding::Base64::encode(serialized.c_str(), serializedSize);
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
      BaseVector::createConstant(VARCHAR(), currentUser, 1, pool_.get()));
  return std::make_shared<protocol::ConstantExpression>(cexpr);
}

json RowExpressionConverter::getRowConstructorSpecialForm(
    const exec::ExprPtr& expr,
    const json& input) {
  json res;
  res["@type"] = "special";
  res["form"] = "ROW_CONSTRUCTOR";
  res["returnType"] = getTypeSignature(expr->type());

  res["arguments"] = json::array();
  auto exprInputs = expr->inputs();
  if (!exprInputs.empty()) {
    for (const auto& exprInput : exprInputs) {
      res["arguments"].push_back(veloxExprToRowExpression(exprInput, input));
    }
  } else if (
      auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(expr)) {
    auto value = constantExpr->value();
    auto* constVector = value->as<ConstantVector<ComplexType>>();
    auto* rowVector = constVector->valueVector()->as<RowVector>();
    auto type = asRowType(constantExpr->type());
    auto children = rowVector->children();
    auto size = children.size();

    json j;
    protocol::ConstantExpression cexpr;
    for (auto i = 0; i < size; i++) {
      cexpr.type = getTypeSignature(type->childAt(i));
      cexpr.valueBlock.data = getValueBlock(rowVector->childAt(i));
      protocol::to_json(j, cexpr);
      res["arguments"].push_back(j);
    }
  }

  return res;
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
    when["@type"] = "special";
    when["form"] = "WHEN";
    when["arguments"] = whenArgs;
    when["returnType"] = getTypeSignature(exprInputs[idx + 1]->type());
    return when;
  };

  // The searched form of the conditional expression needs to be handled
  // differently from the simple form. The searched form can be detected by the
  // presence of a boolean value in the first argument. This default boolean
  // argument is not present in the Velox switch expression, so it is added to
  // the arguments of output switch expression unchanged.
  if (inputArgs[0].at("@type") == "constant" &&
      inputArgs[0].at("type") == "boolean") {
    result.emplace_back(inputArgs[0]);
    for (auto i = 0; i < numInputs - 1; i += 2) {
      const vector_size_t argsIdx = i / 2 + 1;
      json::array_t inputWhenArgs = inputArgs[argsIdx].at("arguments");
      json::array_t whenArgs;
      whenArgs.emplace_back(
          veloxExprToRowExpression(exprInputs[i], inputWhenArgs[0]));
      whenArgs.emplace_back(
          veloxExprToRowExpression(exprInputs[i + 1], inputWhenArgs[1]));

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
                  json::array(
                      {veloxExprToRowExpression(caseValue, inputWhenArgs[1])}),
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
            veloxExprToRowExpression(matchExpr, inputWhenArgs[0]));
      }

      whenArgs.emplace_back(
          veloxExprToRowExpression(caseValue, inputWhenArgs[1]));
      result.emplace_back(getWhenSpecialForm(whenArgs, i));
    }
  }

  // Else clause.
  if (numInputs % 2 != 0) {
    result.push_back(veloxExprToRowExpression(
        exprInputs[numInputs - 1], inputArgs[numArgs - 1]));
  }
  return {result, true};
}

json RowExpressionConverter::getSpecialForm(
    const exec::ExprPtr& expr,
    const json& input) {
  json res;
  res["@type"] = "special";
  std::string form;
  if (input.contains("form")) {
    form = input["form"];
  } else {
    // If input json is a call expression instead of a special form, for cases
    // like 'is_null', the key 'form' will not be present in the input json.
    form = expr->name();
  }
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
  if (form == "SWITCH") {
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
          veloxExprToRowExpression(exprInputs[i], inputArguments[i]));
    }
  }
  res["returnType"] = getTypeSignature(expr->type());

  return res;
}

json RowExpressionConverter::toConstantRowExpression(
    const velox::exec::ExprPtr& expr) {
  json res;
  auto constantExpr = std::dynamic_pointer_cast<const exec::ConstantExpr>(expr);
  VELOX_USER_CHECK_NOT_NULL(constantExpr);
  auto constantRowExpr = getConstantRowExpression(constantExpr);
  protocol::to_json(res, constantRowExpr);
  return res;
}

json RowExpressionConverter::toCallRowExpression(
    const exec::ExprPtr& expr,
    const json& input) {
  json res;
  res["@type"] = "call";
  protocol::Signature signature;
  std::string exprName = expr->name();
  if (veloxToPrestoOperatorMap_.find(expr->name()) !=
      veloxToPrestoOperatorMap_.end()) {
    exprName = veloxToPrestoOperatorMap_.at(expr->name());
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
  builtInFunctionHandle._type = "$static";
  builtInFunctionHandle.signature = signature;
  res["functionHandle"] = builtInFunctionHandle;
  res["returnType"] = getTypeSignature(expr->type());
  res["arguments"] = json::array();
  for (const auto& exprInput : exprInputs) {
    res["arguments"].push_back(veloxExprToRowExpression(exprInput, input));
  }

  return res;
}

json RowExpressionConverter::veloxExprToRowExpression(
    const exec::ExprPtr& expr,
    const json& input) {
  if (expr->type()->isRow()) {
    // Velox constant expressions of ROW type map to special form expression
    // row_constructor in Presto.
    return getRowConstructorSpecialForm(expr, input);
  } else if (expr->isConstant()) {
    if (expr->inputs().empty()) {
      return toConstantRowExpression(expr);
    } else {
      // Inputs to constant expressions are constant, eg: divide(1, 2).
      return input;
    }
  } else if (
      auto field =
          std::dynamic_pointer_cast<const exec::FieldReference>(expr)) {
    return toVariableReferenceExpression(field);
  } else if (expr->isSpecialForm() || expr->vectorFunction()) {
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

RowExpressionPtr RowExpressionEvaluator::optimizeAndSpecialForm(
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

RowExpressionPtr RowExpressionEvaluator::optimizeIfSpecialForm(
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

RowExpressionPtr RowExpressionEvaluator::optimizeIsNullSpecialForm(
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

RowExpressionPtr RowExpressionEvaluator::optimizeOrSpecialForm(
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

RowExpressionPtr RowExpressionEvaluator::optimizeCoalesceSpecialForm(
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

RowExpressionPtr RowExpressionEvaluator::optimizeSpecialForm(
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

exec::ExprPtr RowExpressionEvaluator::compileExpression(
    const std::shared_ptr<protocol::RowExpression>& inputRowExpr) {
  auto typedExpr = veloxExprConverter_.toVeloxExpr(inputRowExpr);
  exec::ExprSet exprSet{{typedExpr}, execCtx_.get()};
  auto compiledExprs =
      exec::compileExpressions({typedExpr}, execCtx_.get(), &exprSet, true);
  return compiledExprs[0];
}

json::array_t RowExpressionEvaluator::evaluateExpressions(
    const json::array_t& input,
    const std::string& currentUser) {
  const auto numExpr = input.size();
  json::array_t output = json::array();
  for (auto i = 0; i < numExpr; i++) {
    // TODO: current_user to be evaluated in the native plugin and will not be
    //  sent to the sidecar.
    if (input[i].contains("displayName") &&
        input[i].at("displayName") == "$current_user") {
      output.emplace_back(rowExpressionConverter_.getCurrentUser(currentUser));
      continue;
    }

    std::shared_ptr<protocol::RowExpression> inputRowExpr = input[i];
    if (const auto special =
            std::dynamic_pointer_cast<protocol::SpecialFormExpression>(
                inputRowExpr)) {
      inputRowExpr = optimizeSpecialForm(special);
    }
    const auto compiledExpr = compileExpression(inputRowExpr);
    json resultJson = rowExpressionConverter_.veloxExprToRowExpression(
        compiledExpr, input[i]);
    output.push_back(resultJson);
  }

  return output;
}

void RowExpressionEvaluator::evaluate(
    proxygen::HTTPMessage* message,
    const std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream) {
  try {
    auto timezone =
        message->getHeaders().getSingleOrEmpty("X-Presto-Time-Zone");
    auto currentUser = message->getHeaders().getSingleOrEmpty("X-Presto-User");
    std::unordered_map<std::string, std::string> config(
        {{core::QueryConfig::kSessionTimezone, timezone},
         {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
    auto queryCtx =
        core::QueryCtx::create(nullptr, core::QueryConfig{std::move(config)});
    execCtx_ =
        std::make_unique<velox::core::ExecCtx>(pool_.get(), queryCtx.get());

    json::array_t inputList = getInputExpressions(body);
    json output = evaluateExpressions(inputList, currentUser);
    proxygen::ResponseBuilder(downstream)
        .status(http::kHttpOk, "OK")
        .header(
            proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationJson)
        .body(output.dump())
        .sendWithEOM();
  } catch (const velox::VeloxUserError& e) {
    VLOG(1) << "VeloxUserError during expression evaluation: " << e.what();
    http::sendErrorResponse(downstream, e.what());
  } catch (const velox::VeloxException& e) {
    VLOG(1) << "VeloxException during expression evaluation: " << e.what();
    http::sendErrorResponse(downstream, e.what());
  } catch (const std::exception& e) {
    VLOG(1) << "std::exception during expression evaluation: " << e.what();
    http::sendErrorResponse(downstream, e.what());
  }
}

} // namespace facebook::presto::expression
