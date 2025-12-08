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
#include "presto_cpp/main/types/VeloxToPrestoExpr.h"
#include <boost/algorithm/string.hpp>
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "velox/core/ITypedExpr.h"
#include "velox/expression/ExprConstants.h"
#include "velox/vector/ConstantVector.h"

using namespace facebook::presto;

namespace facebook::presto::expression {

using VariableReferenceExpressionPtr =
    std::shared_ptr<protocol::VariableReferenceExpression>;

namespace {

constexpr char const* kSpecial = "special";

protocol::TypeSignature getTypeSignature(const velox::TypePtr& type) {
  std::string signature = type->toString();
  if (type->isPrimitiveType()) {
    boost::algorithm::to_lower(signature);
  } else {
    // `Type::toString()` API in Velox returns a capitalized string, which could
    // contain extraneous characters like `""`, `:` for certain types like ROW.
    // This string should be converted to lower case and modified to get a valid
    // protocol::TypeSignature. Eg: for type `ROW(a BIGINT, b VARCHAR)`, Velox
    // `Type::toString()` API returns the string `ROW<a:BIGINT, b:VARCHAR>`. The
    // corresponding protocol::TypeSignature is `row(a bigint, b varchar)`.
    boost::algorithm::erase_all(signature, "\"\"");
    boost::algorithm::replace_all(signature, ":", " ");
    boost::algorithm::replace_all(signature, "<", "(");
    boost::algorithm::replace_all(signature, ">", ")");
    boost::algorithm::to_lower(signature);
  }
  return signature;
}

VariableReferenceExpressionPtr getVariableReferenceExpression(
    const velox::core::FieldAccessTypedExpr* field) {
  constexpr char const* kVariable = "variable";
  protocol::VariableReferenceExpression vexpr;
  vexpr.name = field->name();
  vexpr._type = kVariable;
  vexpr.type = getTypeSignature(field->type());
  return std::make_shared<protocol::VariableReferenceExpression>(vexpr);
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
  return kPrestoSpecialForms.contains(name);
}

json getWhenSpecialForm(
    const velox::TypePtr& type,
    const json::array_t& whenArgs) {
  constexpr char const* kWhen = "WHEN";
  json when;
  when["@type"] = kSpecial;
  when["form"] = kWhen;
  when["arguments"] = whenArgs;
  when["returnType"] = getTypeSignature(type);
  return when;
}

const std::unordered_map<std::string, std::string>& veloxToPrestoOperatorMap() {
  static std::unordered_map<std::string, std::string> veloxToPrestoOperatorMap =
      {{"cast", "presto.default.$operator$cast"}};
  for (const auto& entry : prestoOperatorMap()) {
    veloxToPrestoOperatorMap[entry.second] = entry.first;
  }
  return veloxToPrestoOperatorMap;
}
} // namespace

std::string VeloxToPrestoExprConverter::getValueBlock(
    const velox::VectorPtr& vector) const {
  std::ostringstream output;
  serde_->serializeSingleColumn(vector, nullptr, pool_, &output);
  const auto serialized = output.str();
  const auto serializedSize = serialized.size();
  return velox::encoding::Base64::encode(serialized.c_str(), serializedSize);
}

ConstantExpressionPtr VeloxToPrestoExprConverter::getConstantExpression(
    const velox::core::ConstantTypedExpr* constantExpr) const {
  protocol::ConstantExpression cexpr;
  cexpr.type = getTypeSignature(constantExpr->type());
  cexpr.valueBlock.data = getValueBlock(constantExpr->toConstantVector(pool_));
  return std::make_shared<protocol::ConstantExpression>(cexpr);
}

std::vector<RowExpressionPtr>
VeloxToPrestoExprConverter::getSwitchSpecialFormExpressionArgs(
    const velox::core::CallTypedExpr* switchExpr) const {
  std::vector<RowExpressionPtr> result;
  const auto& switchInputs = switchExpr->inputs();
  const auto numInputs = switchInputs.size();
  for (auto i = 0; i < numInputs - 1; i += 2) {
    const json::array_t resultWhenArgs = {
        getRowExpression(switchInputs[i]),
        getRowExpression(switchInputs[i + 1])};
    result.emplace_back(
        getWhenSpecialForm(switchInputs[i + 1]->type(), resultWhenArgs));
  }

  // Else clause.
  if (numInputs % 2 != 0) {
    result.emplace_back(getRowExpression(switchInputs[numInputs - 1]));
  }
  return result;
}

SpecialFormExpressionPtr VeloxToPrestoExprConverter::getSpecialFormExpression(
    const velox::core::CallTypedExpr* expr) const {
  VELOX_CHECK(
      isPrestoSpecialForm(expr->name()),
      "Not a special form expression: {}.",
      expr->toString());

  protocol::SpecialFormExpression result;
  result._type = kSpecial;
  result.returnType = getTypeSignature(expr->type());
  auto name = expr->name();
  // Presto requires the field form to be in upper case.
  std::transform(name.begin(), name.end(), name.begin(), ::toupper);
  protocol::Form form;
  protocol::from_json(name, form);
  result.form = form;

  // Arguments for switch expression include 'WHEN' special form expression(s)
  // so they are constructed separately.
  static constexpr char const* kSwitch = "SWITCH";
  if (name == kSwitch) {
    result.arguments = getSwitchSpecialFormExpressionArgs(expr);
  } else {
    // Presto special form expressions that are not of type `SWITCH`, such as
    // `IN`, `AND`, `OR` etc,. are handled in this clause. The list of Presto
    // special form expressions can be found in `kPrestoSpecialForms` in the
    // helper function `isPrestoSpecialForm`.
    auto exprInputs = expr->inputs();
    for (const auto& input : exprInputs) {
      result.arguments.push_back(getRowExpression(input));
    }
  }

  return std::make_shared<protocol::SpecialFormExpression>(result);
}

SpecialFormExpressionPtr
VeloxToPrestoExprConverter::getRowConstructorExpression(
    const velox::core::ConstantTypedExpr* constantExpr) const {
  static constexpr char const* kRowConstructor = "ROW_CONSTRUCTOR";
  json result;
  result["@type"] = kSpecial;
  result["form"] = kRowConstructor;
  result["returnType"] = getTypeSignature(constantExpr->valueVector()->type());

  const auto& constVector = constantExpr->toConstantVector(pool_);
  const auto* rowVector = constVector->valueVector()->as<velox::RowVector>();
  VELOX_CHECK_NOT_NULL(
      rowVector,
      "Constant vector not of row type: {}.",
      constVector->type()->toString());
  VELOX_CHECK(
      constantExpr->type()->isRow(),
      "Constant expression not of ROW type: {}.",
      constantExpr->type()->toString());
  const auto type = asRowType(constantExpr->type());

  protocol::ConstantExpression cexpr;
  json j;
  result["arguments"] = json::array();
  for (const auto& child : rowVector->children()) {
    cexpr.type = getTypeSignature(child->type());
    cexpr.valueBlock.data = getValueBlock(child);
    protocol::to_json(j, cexpr);
    result["arguments"].push_back(j);
  }
  return result;
}

SpecialFormExpressionPtr VeloxToPrestoExprConverter::getDereferenceExpression(
    const velox::core::DereferenceTypedExpr* dereferenceExpr) const {
  static constexpr char const* kDereference = "DEREFERENCE";
  json result;
  result["@type"] = kSpecial;
  result["form"] = kDereference;
  result["returnType"] = getTypeSignature(dereferenceExpr->type());

  json j;
  result["arguments"] = json::array();
  const auto dereferenceInputs = std::vector<velox::core::TypedExprPtr>{
      dereferenceExpr->inputs().at(0),
      std::make_shared<velox::core::ConstantTypedExpr>(
          velox::BIGINT(), static_cast<int64_t>(dereferenceExpr->index()))};
  for (const auto& input : dereferenceInputs) {
    const auto rowExpr = getRowExpression(input);
    protocol::to_json(j, rowExpr);
    result["arguments"].push_back(j);
  }

  return result;
}

CallExpressionPtr VeloxToPrestoExprConverter::getCallExpression(
    const velox::core::CallTypedExpr* expr) const {
  static constexpr char const* kCall = "call";
  static constexpr char const* kStatic = "$static";

  json result;
  result["@type"] = kCall;
  protocol::Signature signature;
  std::string exprName = expr->name();
  if (veloxToPrestoOperatorMap().find(exprName) !=
      veloxToPrestoOperatorMap().end()) {
    exprName = veloxToPrestoOperatorMap().at(exprName);
  }
  signature.name = exprName;
  result["displayName"] = exprName;
  signature.kind = protocol::FunctionKind::SCALAR;
  signature.typeVariableConstraints = {};
  signature.longVariableConstraints = {};
  signature.returnType = getTypeSignature(expr->type());

  std::vector<protocol::TypeSignature> argumentTypes;
  auto exprInputs = expr->inputs();
  argumentTypes.reserve(exprInputs.size());
  for (const auto& input : exprInputs) {
    argumentTypes.emplace_back(getTypeSignature(input->type()));
  }
  signature.argumentTypes = argumentTypes;
  signature.variableArity = false;

  protocol::BuiltInFunctionHandle builtInFunctionHandle;
  builtInFunctionHandle._type = kStatic;
  builtInFunctionHandle.signature = signature;
  result["functionHandle"] = builtInFunctionHandle;
  result["returnType"] = getTypeSignature(expr->type());
  result["arguments"] = json::array();
  for (const auto& exprInput : exprInputs) {
    result["arguments"].push_back(getRowExpression(exprInput));
  }

  return result;
}

RowExpressionPtr VeloxToPrestoExprConverter::getRowExpression(
    const velox::core::TypedExprPtr& expr,
    const RowExpressionPtr& inputRowExpr) const {
  switch (expr->kind()) {
    case velox::core::ExprKind::kConstant: {
      const auto* constantExpr =
          expr->asUnchecked<velox::core::ConstantTypedExpr>();
      // ConstantTypedExpr of ROW type maps to SpecialFormExpression of type
      // ROW_CONSTRUCTOR in Presto.
      if (expr->type()->isRow()) {
        return getRowConstructorExpression(constantExpr);
      }
      return getConstantExpression(constantExpr);
    }
    case velox::core::ExprKind::kFieldAccess: {
      const auto* field =
          expr->asUnchecked<velox::core::FieldAccessTypedExpr>();
      return getVariableReferenceExpression(field);
    }
    case velox::core::ExprKind::kDereference: {
      const auto* dereferenceTypedExpr =
          expr->asUnchecked<velox::core::DereferenceTypedExpr>();
      return getDereferenceExpression(dereferenceTypedExpr);
    }
    case velox::core::ExprKind::kCast: {
      // Velox CastTypedExpr maps to Presto CallExpression.
      const auto* castExpr = expr->asUnchecked<velox::core::CastTypedExpr>();
      auto call = std::make_shared<velox::core::CallTypedExpr>(
          expr->type(), castExpr->inputs(), velox::expression::kCast);
      return getCallExpression(call.get());
    }
    case velox::core::ExprKind::kCall: {
      const auto* callTypedExpr =
          expr->asUnchecked<velox::core::CallTypedExpr>();
      // Check if special form expression or call expression.
      auto exprName = callTypedExpr->name();
      boost::algorithm::to_lower(exprName);
      if (isPrestoSpecialForm(exprName)) {
        return getSpecialFormExpression(callTypedExpr);
      }
      return getCallExpression(callTypedExpr);
    }
    // Presto does not have a RowExpression type for kConcat and kInput Velox
    // expressions. Presto's expression optimizer does not support optimization
    // of lambda expressions.
    case velox::core::ExprKind::kConcat:
      [[fallthrough]];
    case velox::core::ExprKind::kInput:
      [[fallthrough]];
    case velox::core::ExprKind::kLambda:
      [[fallthrough]];
    default: {
      // Log Velox to Presto expression conversion error and return the
      // unoptimized input RowExpression.
      LOG(ERROR) << fmt::format(
          "Unable to convert Velox expression: {} of kind: {} to Presto RowExpression.",
          expr->toString(),
          velox::core::ExprKindName::toName(expr->kind()));
      return inputRowExpr;
    }
  }
}

} // namespace facebook::presto::expression
