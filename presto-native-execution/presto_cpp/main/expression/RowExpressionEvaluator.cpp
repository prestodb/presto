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

protocol::TypeSignature parseType(const TypePtr& type) {
  std::string resultTypeStr;
  if (type->parameters().empty()) {
    resultTypeStr = type->toString();
    boost::algorithm::to_lower(resultTypeStr);
  } else if (type->isDecimal()) {
    resultTypeStr = type->toString();
  } else {
    std::vector<TypePtr> childTypes;
    if (type->isRow()) {
      resultTypeStr = "row(";
      childTypes = asRowType(type)->children();
    } else if (type->isArray()) {
      resultTypeStr = "array(";
      childTypes = type->asArray().children();
    } else if (type->isMap()) {
      resultTypeStr = "map(";
      childTypes = {type->asMap().keyType(), type->asMap().valueType()};
    } else {
      VELOX_USER_FAIL("Invalid type {}", type->toString());
    }

    if (!childTypes.empty()) {
      auto numChildren = childTypes.size();
      for (auto i = 0; i < numChildren - 1; i++) {
        resultTypeStr += fmt::format("{},", parseType(childTypes[i]));
      }
      resultTypeStr += parseType(childTypes[numChildren - 1]);
    }
    resultTypeStr += ")";
  }
  return resultTypeStr;
}

json toVariableReferenceExpression(
    const std::shared_ptr<const exec::FieldReference>& fieldReference) {
  protocol::VariableReferenceExpression vexpr;
  vexpr.name = fieldReference->name();
  vexpr._type = "variable";
  vexpr.type = parseType(fieldReference->type());

  json res;
  protocol::to_json(res, vexpr);
  return res;
}

bool isSpecialForm(const std::string& name) {
  static const std::unordered_set<std::string> kSpecialForms = {
      "and",
      "cast",
      "coalesce",
      "if",
      "in",
      "is_null",
      "or",
      "switch",
      "try",
      "try_cast"};
  return kSpecialForms.count(name) != 0;
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

// ValueBlock in ConstantExpression requires only the column to be serialized
// in PrestoPage format, without the page header.
std::string RowExpressionEvaluator::serializeValueBlock(
    const VectorPtr& vector) {
  auto numRows = vector->size();
  const IndexRange allRows{0, numRows};
  auto ranges = folly::Range(&allRows, 1);
  const auto arena = std::make_unique<StreamArena>(pool_.get());
  auto stream = std::make_unique<serializer::presto::VectorStream>(
      vector->type(),
      std::nullopt,
      std::nullopt,
      arena.get(),
      numRows,
      options_);
  Scratch scratch;
  serde_->serializeColumn(vector, ranges, stream.get(), scratch);
  IOBufOutputStream ostream(*pool_);
  stream->flush(&ostream);

  auto resultBuf = ostream.getIOBuf();
  auto bufferLength = resultBuf->computeChainDataLength();
  resultBuf->gather(bufferLength);
  return velox::encoding::Base64::encode(
      reinterpret_cast<const char*>(resultBuf->data()), bufferLength);
}

std::shared_ptr<protocol::ConstantExpression>
RowExpressionEvaluator::getConstantRowExpression(
    const std::shared_ptr<const exec::ConstantExpr>& constantExpr) {
  protocol::ConstantExpression cexpr;
  cexpr.type = parseType(constantExpr->type());
  cexpr.valueBlock.data = serializeValueBlock(constantExpr->value());
  return std::make_shared<protocol::ConstantExpression>(cexpr);
}

// Compiles protocol::RowExpression into a velox expression, with constant
// folding enabled.
exec::ExprPtr RowExpressionEvaluator::compileExpression(
    const std::shared_ptr<protocol::RowExpression>& inputRowExpr) {
  auto typedExpr = exprConverter_.toVeloxExpr(inputRowExpr);
  exec::ExprSet exprSet{{typedExpr}, execCtx_.get()};
  auto compiledExprs =
      exec::compileExpressions({typedExpr}, execCtx_.get(), &exprSet, true);
  return compiledExprs[0];
}

json RowExpressionEvaluator::getSpecialFormRowConstructor(
    const exec::ExprPtr& expr,
    const json& input) {
  json res;
  res["@type"] = "special";
  res["form"] = "ROW_CONSTRUCTOR";
  res["returnType"] = parseType(expr->type());

  res["arguments"] = json::array();
  auto exprInputs = expr->inputs();
  if (!exprInputs.empty()) {
    for (const auto& expr : exprInputs) {
      res["arguments"].push_back(veloxExprToRowExpr(expr, input));
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
      cexpr.type = parseType(type->childAt(i));
      cexpr.valueBlock.data = serializeValueBlock(rowVector->childAt(i));
      protocol::to_json(j, cexpr);
      res["arguments"].push_back(cexpr);
    }
  }

  if (input.contains("sourceLocation")) {
    res["sourceLocation"] = input["sourceLocation"];
  }
  return res;
}

json RowExpressionEvaluator::getSpecialForm(
    const exec::ExprPtr& expr,
    const json& input) {
  json res;
  res["@type"] = "special";
  std::string form;
  if (input.contains("form")) {
    form = input["form"];
  } else {
    // is_null is a special form expression in Presto but a call expression in
    // Velox.
    VELOX_USER_CHECK_EQ(expr->name(), "is_null");
    form = expr->name();
  }
  // Presto requires the field form to be in upper case.
  std::transform(form.begin(), form.end(), form.begin(), ::toupper);
  res["form"] = form;

  auto exprInputs = expr->inputs();
  res["arguments"] = json::array();
  for (const auto& expr : exprInputs) {
    res["arguments"].push_back(veloxExprToRowExpr(expr, input));
  }
  res["returnType"] = parseType(expr->type());

  if (input.contains("sourceLocation")) {
    res["sourceLocation"] = input["sourceLocation"];
  }
  return res;
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
          return getConstantRowExpression(constantExpr);
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
      return getConstantRowExpression(constantExpr);
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
      return getConstantRowExpression(constantExpr);
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
          return getConstantRowExpression(constantExpr);
        }
        return right;
      }
    }
  }

  auto rightExpr = compileExpression(right);
  if (auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(rightExpr)) {
    if (isLeftNull && constantExpr->value()->isNullAt(0)) {
      return getConstantRowExpression(constantExpr);
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

// Optimizes special form expressions. Optimization rules borrowed from
// Presto function visitSpecialForm, in
// presto-main/src/main/java/com/facebook/presto/sql/planner/RowExpressionInterpreter.java.
RowExpressionPtr RowExpressionEvaluator::optimizeSpecialForm(
    const std::shared_ptr<protocol::SpecialFormExpression>& specialFormExpr) {
  switch (specialFormExpr->form) {
    case protocol::Form::IF:
      return optimizeIfSpecialForm(specialFormExpr);
    case protocol::Form::NULL_IF:
      VELOX_USER_FAIL("NULL_IF not supported in specialForm");
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

json RowExpressionEvaluator::veloxExprToCallExpr(
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
  signature.returnType = parseType(expr->type());

  std::vector<protocol::TypeSignature> argumentTypes;
  auto exprInputs = expr->inputs();
  auto numArgs = exprInputs.size();
  argumentTypes.reserve(numArgs);
  for (auto i = 0; i < numArgs; i++) {
    argumentTypes.emplace_back(parseType(exprInputs[i]->type()));
  }
  signature.argumentTypes = argumentTypes;
  signature.variableArity = false;

  protocol::BuiltInFunctionHandle builtInFunctionHandle;
  builtInFunctionHandle._type = "$static";
  builtInFunctionHandle.signature = signature;
  res["functionHandle"] = builtInFunctionHandle;
  res["returnType"] = parseType(expr->type());
  res["arguments"] = json::array();
  for (const auto& expr : exprInputs) {
    res["arguments"].push_back(veloxExprToRowExpr(expr, input));
  }

  return res;
}

json RowExpressionEvaluator::veloxExprToRowExpr(
    const exec::ExprPtr& expr,
    const json& input) {
  json res;
  if (input.contains("sourceLocation")) {
    res["sourceLocation"] = input["sourceLocation"];
  }

  if (expr->type()->isRow()) {
    // Velox constant expressions of ROW type map to special form expression
    // row_constructor in Presto.
    return getSpecialFormRowConstructor(expr, input);
  } else if (expr->isConstant()) {
    if (expr->inputs().empty()) {
      auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(expr);
      VELOX_USER_CHECK_NOT_NULL(constantExpr);
      auto constantRowExpr = getConstantRowExpression(constantExpr);
      protocol::to_json(res, constantRowExpr);
      return res;
    } else {
      // divide(0, 0) is a constant expression with constant inputs.
      return input;
    }
  } else if (
      auto field =
          std::dynamic_pointer_cast<const exec::FieldReference>(expr)) {
    // variable
    return toVariableReferenceExpression(field);
  } else if (expr->isSpecialForm()) {
    // special
    return getSpecialForm(expr, input);
  } else if (auto func = expr->vectorFunction()) {
    // Check if special form expression or call expression.
    if (isSpecialForm(expr->name())) {
      // special
      return getSpecialForm(expr, input);
    } else {
      return veloxExprToCallExpr(expr, input);
    }
  } else {
    VELOX_USER_FAIL(
        "Unable to convert Velox Expr to Presto RowExpression: {}",
        expr->toString());
  }

  return res;
}

json::array_t RowExpressionEvaluator::evaluateExpression(json::array_t& input) {
  auto numExpr = input.size();
  json::array_t output = json::array();

  for (auto i = 0; i < numExpr; i++) {
    std::shared_ptr<protocol::RowExpression> inputRowExpr = input[i];
    if (const auto special =
            std::dynamic_pointer_cast<protocol::SpecialFormExpression>(
                inputRowExpr)) {
      inputRowExpr = optimizeSpecialForm(special);
    }
    const auto compiledExpr = compileExpression(inputRowExpr);
    json resultJson = veloxExprToRowExpr(compiledExpr, input[i]);
    output.push_back(resultJson);
  }

  return output;
}

void RowExpressionEvaluator::evaluate(
    const std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream) {
  try {
    json::array_t inputList = getInputExpressions(body);
    json output = evaluateExpression(inputList);

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
