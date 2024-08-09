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
#include "presto_cpp/main/eval/PrestoExprEval.h"
#include <proxygen/httpserver/ResponseBuilder.h>
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/encode/Base64.h"
#include "velox/core/Expressions.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprCompiler.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/LambdaExpr.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace facebook::presto::eval {

namespace {

// ValueBlock in ConstantExpression requires only the column to be serialized
// in PrestoPage format, without the page header.
const std::string serializeValueBlock(
    const VectorPtr& vector,
    memory::MemoryPool* pool) {
  auto numRows = vector->size();
  std::unique_ptr<serializer::presto::PrestoVectorSerde> serde =
      std::make_unique<serializer::presto::PrestoVectorSerde>();
  const IndexRange allRows{0, numRows};
  auto ranges = folly::Range(&allRows, 1);
  const auto arena = std::make_unique<StreamArena>(pool);
  serializer::presto::PrestoOptions paramOptions;
  auto stream = std::make_unique<serializer::presto::VectorStream>(
      vector->type(),
      std::nullopt,
      std::nullopt,
      arena.get(),
      numRows,
      paramOptions);
  Scratch scratch;
  serde->serializeColumn(vector, ranges, stream.get(), scratch);
  IOBufOutputStream ostream(*pool);
  stream->flush(&ostream);
  auto resultBuf = ostream.getIOBuf();
  auto bufLen = resultBuf->length();
  resultBuf->gather(bufLen);
  return velox::encoding::Base64::encode(
      reinterpret_cast<const char*>(resultBuf->data()), bufLen);
}

json fieldReferenceToVariableRefExpr(exec::FieldReference* fieldReference) {
  protocol::VariableReferenceExpression vexpr;
  vexpr.name = fieldReference->name();
  vexpr._type = "variable";
  vexpr.type = fieldReference->type()->toString();
  json res;
  protocol::to_json(res, vexpr);
  return res;
}

std::shared_ptr<protocol::ConstantExpression> getConstantExpr(
    std::shared_ptr<exec::ConstantExpr> constantExpr,
    memory::MemoryPool* pool) {
  protocol::ConstantExpression rexpr;
  rexpr.type = constantExpr->type()->toString();
  rexpr.valueBlock.data = serializeValueBlock(constantExpr->value(), pool);
  return std::make_shared<protocol::ConstantExpression>(rexpr);
}
} // namespace

std::shared_ptr<velox::exec::Expr> PrestoExprEval::compileExpression(
    std::shared_ptr<protocol::RowExpression> inputRowExpr) {
  auto typedExpr = exprConverter_.toVeloxExpr(inputRowExpr);
  if (auto lambdaExpr = core::TypedExprs::asLambda(typedExpr)) {
    lambdaTypedExpr_ = lambdaExpr;
    isLambda_ = true;
  } else {
    isLambda_ = false;
  }

  exec::ExprSet exprSet{{typedExpr}, execCtx_.get()};
  // Constant folds the expression.
  auto compiledExprs =
      exec::compileExpressions({typedExpr}, execCtx_.get(), &exprSet, true);
  return compiledExprs[0];
}

// Optimizes special form expressions. Rules borrowed from visitSpecialForm
// in prestodb RowExpressionInterpreter.java.
std::shared_ptr<protocol::RowExpression> PrestoExprEval::optimizeSpecialForm(
    std::shared_ptr<protocol::SpecialFormExpression> specialFormExpr) {
  switch (specialFormExpr->form) {
    case protocol::Form::IF: {
      auto condition = specialFormExpr->arguments[0];
      auto expr = compileExpression(condition);

      if (auto constantExpr =
              std::dynamic_pointer_cast<exec::ConstantExpr>(expr)) {
        if (auto constVector =
                constantExpr->value()->as<ConstantVector<bool>>()) {
          if (constVector->valueAt(0)) {
            return specialFormExpr->arguments[1];
          }
          return specialFormExpr->arguments[2];
        }
      }
      break;
    }
    case protocol::Form::NULL_IF:
      VELOX_UNREACHABLE("NULL_IF not supported in specialForm");
      break;
    case protocol::Form::IS_NULL: {
      auto value = specialFormExpr->arguments[0];
      auto expr = compileExpression(specialFormExpr);

      if (auto constantExpr =
              std::dynamic_pointer_cast<exec::ConstantExpr>(expr)) {
        if (constantExpr->value()->isNullAt(0)) {
          return getConstantExpr(constantExpr, pool_.get());
        }
      }
      break;
    }
    case protocol::Form::AND: {
      auto left = specialFormExpr->arguments[0];
      auto right = specialFormExpr->arguments[1];
      auto lexpr = compileExpression(left);
      bool isLeftNull;

      if (auto constantExpr =
              std::dynamic_pointer_cast<exec::ConstantExpr>(lexpr)) {
        isLeftNull = constantExpr->value()->isNullAt(0);
        if (auto constVector =
                constantExpr->value()->as<ConstantVector<bool>>()) {
          if (constVector->valueAt(0) == false) {
            return getConstantExpr(constantExpr, pool_.get());
          } else {
            return right;
          }
        }
      }

      auto rexpr = compileExpression(right);
      if (auto constantExpr =
              std::dynamic_pointer_cast<exec::ConstantExpr>(rexpr)) {
        if (isLeftNull && constantExpr->value()->isNullAt(0)) {
          return getConstantExpr(constantExpr, pool_.get());
        }
        if (auto constVector =
                constantExpr->value()->as<ConstantVector<bool>>()) {
          if (constVector->valueAt(0) == true) {
            return left;
          }
          return right;
        }
      }
      break;
    }
    case protocol::Form::OR: {
      auto left = specialFormExpr->arguments[0];
      auto right = specialFormExpr->arguments[1];
      auto lexpr = compileExpression(left);
      bool isLeftNull;

      if (auto constantExpr =
              std::dynamic_pointer_cast<exec::ConstantExpr>(lexpr)) {
        isLeftNull = constantExpr->value()->isNullAt(0);
        if (auto constVector =
                constantExpr->value()->as<ConstantVector<bool>>()) {
          if (constVector->valueAt(0) == true) {
            return getConstantExpr(constantExpr, pool_.get());
          }
          return right;
        }
      }

      auto rexpr = compileExpression(right);
      if (auto constantExpr =
              std::dynamic_pointer_cast<exec::ConstantExpr>(rexpr)) {
        if (isLeftNull && constantExpr->value()->isNullAt(0)) {
          return getConstantExpr(constantExpr, pool_.get());
        }
        if (auto constVector =
                constantExpr->value()->as<ConstantVector<bool>>()) {
          if (constVector->valueAt(0) == false) {
            return left;
          }
          return right;
        }
      }
      break;
    }
    case protocol::Form::IN: {
      auto args = specialFormExpr->arguments;
      VELOX_USER_CHECK(args.size() >= 2, "values must not be empty");
      auto target = args[0];
      if (target == nullptr) {
        return nullptr;
      }
      break;
    }
    case protocol::Form::DEREFERENCE: {
      auto args = specialFormExpr->arguments;
      VELOX_USER_CHECK(args.size() == 2);
      auto base = args[0];
      if (base == nullptr) {
        return nullptr;
      }
      break;
    }
  }
  return specialFormExpr;
}

json PrestoExprEval::exprToRowExpression(
    std::shared_ptr<velox::exec::Expr> expr) {
  json res;
  if (expr->isConstant()) {
    // constant
    res["@type"] = "constant";
    auto constantExpr = std::dynamic_pointer_cast<exec::ConstantExpr>(expr);
    auto type = constantExpr->type();
    VectorPtr constVector = constantExpr->value();
    res["valueBlock"] = serializeValueBlock(constVector, pool_.get());
    res["type"] = expr->type()->toString();
  } else if (auto fexpr = expr->as<exec::FieldReference>()) {
    // variable
    res = fieldReferenceToVariableRefExpr(fexpr);
  } else if (expr->isSpecialForm()) {
    // special
    res["@type"] = "special";
    auto inputs = expr->inputs();
    res["arguments"] = json::array();
    for (auto input : inputs) {
      res["arguments"].push_back(exprToRowExpression(input));
    }
    res["form"] = "BIND";
    res["returnType"] = expr->type()->toString();
  } else if (auto lambda = std::dynamic_pointer_cast<exec::LambdaExpr>(expr)) {
    // lambda
    res["@type"] = "lambda";
    auto inputs = lambda->distinctFields();
    res["arguments"] = json::array();
    res["argumentTypes"] = json::array();
    auto numInputs = inputs.size();
    for (auto i = 0; i < numInputs; i++) {
      res["arguments"].push_back(fieldReferenceToVariableRefExpr(inputs[i]));
      // TODO: Recheck type conversion.
      res["argumentTypes"].push_back(lambda->type()->childAt(i)->toString());
    }
    VELOX_USER_CHECK(isLambda_, "Not a lambda expression");
    res["body"] = lambdaTypedExpr_->body()->toString();
  } else if (auto func = expr->vectorFunction()) {
    // call
    protocol::BuiltInFunctionHandle builtInFunctionHandle;
    builtInFunctionHandle._type = "$static";
    protocol::Signature signature;
    signature.kind = protocol::FunctionKind::SCALAR;
    signature.returnType = expr->type()->toString();

    // TODO: Fix show tables, possibly add inverse map for mapScalarFunction,
    //  eg: for presto.default.eq, to return presto.default.$operator$equal ?
    signature.name = expr->name();
    std::vector<protocol::TypeSignature> argumentTypes;
    auto inputs = expr->inputs();
    auto numArgs = inputs.size();
    argumentTypes.reserve(numArgs);
    for (auto i = 0; i < numArgs; i++) {
      argumentTypes.emplace_back(inputs[i]->type()->toString());
    }
    signature.typeVariableConstraints = {};
    signature.longVariableConstraints = {};
    signature.variableArity = false;

    res["@type"] = "call";
    res["displayName"] = expr->name();
    builtInFunctionHandle.signature = signature;
    res["functionHandle"] = builtInFunctionHandle;
    for (auto input : inputs) {
      auto rexpr = exprToRowExpression(input);
      res["arguments"].push_back(rexpr);
    }
    res["returnType"] = expr->type()->toString();
  } else {
    VELOX_NYI(
        "Unable to convert velox Expr {} to presto RowExpression",
        expr->name());
  }

  return res;
}

void PrestoExprEval::evaluateExpression(
    const std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream) {
  std::ostringstream oss;
  for (auto& buf : body) {
    oss << std::string((const char*)buf->data(), buf->length());
  }
  auto input = json::parse(oss.str());
  auto numExpr = input.size();
  nlohmann::json output = json::array();

  for (auto i = 0; i < numExpr; i++) {
    std::shared_ptr<protocol::RowExpression> inputRowExpr = input[i];
    if (auto special =
            std::dynamic_pointer_cast<protocol::SpecialFormExpression>(
                inputRowExpr)) {
      inputRowExpr = optimizeSpecialForm(special);
    }
    auto compiledExpr = compileExpression(inputRowExpr);
    auto resultJson = exprToRowExpression(compiledExpr);
    output.push_back(resultJson);
  }

  proxygen::ResponseBuilder(downstream)
      .status(http::kHttpOk, "")
      .header(
          proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationJson)
      .body(output.dump())
      .sendWithEOM();
}

void PrestoExprEval::registerUris(http::HttpServer& server) {
  server.registerPost(
      "/v1/expressions",
      [&](proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& body,
          proxygen::ResponseHandler* downstream) {
        return evaluateExpression(body, downstream);
      });
}
} // namespace facebook::presto::eval
