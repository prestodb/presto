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
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/encode/Base64.h"
#include "velox/core/Expressions.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprCompiler.h"
#include "velox/expression/LambdaExpr.h"
#include "velox/expression/FieldReference.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace facebook::presto::eval {
void PrestoExprEval::registerUris(http::HttpServer& server) {
  server.registerPost(
      "/v1/expressions",
      [&](proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& body,
          proxygen::ResponseHandler* downstream) {
        return evaluateExpression(body, downstream);
      });
}

namespace {
json fieldReferenceToVariableRefExpr(exec::FieldReference* fieldReference) {
  json res;
  res["@type"] = "variable";
  res["sourceLocation"] = "sampleSource";
  res["name"] = fieldReference->name();
  res["type"] = fieldReference->type()->toString();
  return res;
}
}

json PrestoExprEval::exprToRowExpression(std::shared_ptr<velox::exec::Expr> expr) {
  json res;
  if (expr->isConstant()) {
    // constant
    res["@type"] = "constant";
    auto constantExpr = std::dynamic_pointer_cast<exec::ConstantExpr>(expr);
    auto valStr = constantExpr->value()->toString();
    auto encStr = encoding::Base64::encode(valStr);
    res["valueBlock"] = encStr;
    res["type"] = expr->type()->toString();
  } else if (expr->isSpecialForm()) {
    // special
    res["@type"] = "special";
    res["sourceLocation"] = "sampleSource";
    auto inputs = expr->inputs();
    res["arguments"] = json::array();
    for (auto input: inputs) {
      res["arguments"].push_back(exprToRowExpression(input));
    }
    res["form"] = "BIND";
    res["type"] = expr->type()->toString();
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
    res["@type"] = "call";
    res["sourceLocation"] = "sampleSource";
    res["displayName"] = expr->name();
    res["functionHandle"] = expr->toString();
    res["returnType"] = expr->type()->toString();
    auto fields = expr->distinctFields();
    for (auto field: fields) {
      // TODO: Check why static cast and dynamic cast are not working.
      res["arguments"].push_back(fieldReferenceToVariableRefExpr(field));
    }
    auto inputs = expr->inputs();
    for (auto input: inputs) {
      res["arguments"].push_back(exprToRowExpression(input));
    }
  } else {
    VELOX_NYI("Unable to convert velox expr to rowexpr");
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

  auto inputJsonArray = input.at("inputs");
  auto len = inputJsonArray.size();
  nlohmann::json output;
  output["outputs"] = json::array();

  for (auto i = 0; i < len; i++) {
    std::shared_ptr<protocol::RowExpression> inputRowExpr = inputJsonArray[i];
    auto typedExpr = exprConverter_.toVeloxExpr(inputRowExpr);
//    parse::ParseOptions options;
//    auto untyped = parse::parseExpr("a = gte(cos(sin(1.2)),0.1)", options);
//    typedExpr =
//        core::Expressions::inferTypes(untyped, ROW({"a"}, {BOOLEAN()}), pool_.get());

    if (auto lambdaExpr = core::TypedExprs::asLambda(typedExpr)) {
      lambdaTypedExpr_ = lambdaExpr;
      isLambda_ = true;
    } else {
      isLambda_ = false;
    }

    exec::ExprSet exprSet{{typedExpr}, execCtx_.get()};
    auto compiledExprs =
        exec::compileExpressions({typedExpr}, execCtx_.get(), &exprSet, true);
    auto compiledExpr = compiledExprs[0];
    auto res = exprToRowExpression(compiledExpr);
    output["outputs"].push_back(res);
  }

  proxygen::ResponseBuilder(downstream)
      .status(http::kHttpOk, "")
      .header(
          proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationJson)
      .body(output.dump())
      .sendWithEOM();
}
} // namespace facebook::presto::eval
