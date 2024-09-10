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
    const std::shared_ptr<const exec::FieldReference>& fieldReference,
    const json& input) {
  protocol::VariableReferenceExpression vexpr;
  vexpr.name = fieldReference->name();
  vexpr._type = "variable";
  vexpr.type = getTypeSignature(fieldReference->type());

  json res;
  protocol::to_json(res, vexpr);
  if (input.contains("sourceLocation")) {
    res["sourceLocation"] = input["sourceLocation"];
  }
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

  if (input.contains("sourceLocation")) {
    res["sourceLocation"] = input["sourceLocation"];
  }
  return res;
}

json RowExpressionConverter::getWhenSpecialForm(
    const std::vector<exec::ExprPtr>& exprInputs,
    const vector_size_t& idx,
    json::array_t inputArgs,
    bool isSearchedForm) {
  json res;
  res["@type"] = "special";
  res["form"] = "WHEN";
  const auto& equalExprInputs = exprInputs[idx]->inputs();
  // expressions to the left and right of WHEN.
  const auto& leftExpr =
      isSearchedForm ? equalExprInputs[1] : equalExprInputs[0];
  const auto& rightExpr = exprInputs[idx + 1];
  const vector_size_t argsIdx = idx / 2 + 1;
  json::array_t whenArgs = inputArgs[argsIdx].at("arguments");

  json::array_t args;
  if (!leftExpr->inputs().empty()) {
    args.emplace_back(veloxExprToCallExpr(
        leftExpr->inputs()[0], whenArgs[0].at("arguments")[0]));
  } else {
    args.emplace_back(veloxExprToRowExpression(leftExpr, whenArgs[0]));
  }
  args.emplace_back(veloxExprToRowExpression(rightExpr, whenArgs[1]));
  res["arguments"] = args;
  res["returnType"] = getTypeSignature(rightExpr->type());

  if (inputArgs[argsIdx].contains("sourceLocation")) {
    res["sourceLocation"] = inputArgs[argsIdx].at("sourceLocation");
  }
  return res;
}

json::array_t RowExpressionConverter::getSwitchSpecialFormArgs(
    const exec::ExprPtr& expr,
    const json& input) {
  json::array_t inputArgs = input["arguments"];
  auto numArgs = inputArgs.size();
  bool isSearchedForm = false;
  if (typeParser_.parse(inputArgs[0]["type"]) == BOOLEAN() &&
      inputArgs[0]["@type"] == "constant") {
    isSearchedForm = true;
  }
  const std::vector<exec::ExprPtr> exprInputs = expr->inputs();
  const auto numInputs = exprInputs.size();

  json::array_t result = json::array();
  if (isSearchedForm) {
    auto variableExpr = exprInputs[0]->inputs()[0];
    result.push_back(veloxExprToRowExpression(variableExpr, inputArgs[0]));
    for (auto i = 0; i < numInputs - 1; i += 2) {
      result.push_back(getWhenSpecialForm(exprInputs, i, inputArgs, true));
    }
  } else {
    auto variableExpr = exprInputs[0]->inputs()[1];
    result.push_back(veloxExprToRowExpression(variableExpr, inputArgs[0]));
    for (auto i = 0; i < numInputs - 1; i += 2) {
      result.push_back(getWhenSpecialForm(exprInputs, i, inputArgs, false));
    }
  }
  result.push_back(inputArgs[numArgs - 1]);

  return result;
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
  // which needs to be constructed separately.
  if (form == "SWITCH") {
    res["arguments"] = getSwitchSpecialFormArgs(expr, input);
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

  if (input.contains("sourceLocation")) {
    res["sourceLocation"] = input["sourceLocation"];
  }
  return res;
}

json RowExpressionConverter::veloxExprToCallExpr(
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
      json res;
      auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(expr);
      VELOX_USER_CHECK_NOT_NULL(constantExpr);
      auto constantRowExpr = getConstantRowExpression(constantExpr);
      protocol::to_json(res, constantRowExpr);
      return res;
    } else {
      // Inputs to constant expressions are constant, eg: divide(1, 2).
      return input;
    }
  } else if (
      auto field =
          std::dynamic_pointer_cast<const exec::FieldReference>(expr)) {
    // variable
    return toVariableReferenceExpression(field, input);
  } else if (expr->isSpecialForm() || expr->vectorFunction()) {
    // Check if special form expression or call expression.
    auto exprName = expr->name();
    boost::algorithm::to_lower(exprName);
    if (isPrestoSpecialForm(exprName)) {
      return getSpecialForm(expr, input);
    } else {
      return veloxExprToCallExpr(expr, input);
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
    json::array_t& input) {
  auto numExpr = input.size();
  json::array_t output = json::array();

  for (auto i = 0; i < numExpr; i++) {
    std::shared_ptr<protocol::RowExpression> inputRowExpr = input[i];
    VLOG(2) << input[i].dump();
    if (const auto special =
            std::dynamic_pointer_cast<protocol::SpecialFormExpression>(
                inputRowExpr)) {
      inputRowExpr = optimizeSpecialForm(special);
    }
    const auto compiledExpr = compileExpression(inputRowExpr);
    json resultJson = rowExpressionConverter_.veloxExprToRowExpression(
        compiledExpr, input[i]);
    VLOG(2) << resultJson.dump();
    output.push_back(resultJson);
  }

  return output;
}

void RowExpressionEvaluator::evaluate(
    const std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream) {
  try {
    json::array_t inputList = getInputExpressions(body);
    json output = evaluateExpressions(inputList);

    proxygen::ResponseBuilder(downstream)
        .status(http::kHttpOk, "OK")
        .header(
            proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationJson)
        .body(output.dump())
        .sendWithEOM();
  } catch (const velox::VeloxUserError& e) {
    VLOG(2) << e.what();
    http::sendErrorResponse(downstream, e.what());
  } catch (const velox::VeloxException& e) {
    VLOG(2) << e.what();
    http::sendErrorResponse(downstream, e.what());
  } catch (const std::exception& e) {
    VLOG(2) << e.what();
    http::sendErrorResponse(downstream, e.what());
  }
}

} // namespace facebook::presto::expression
