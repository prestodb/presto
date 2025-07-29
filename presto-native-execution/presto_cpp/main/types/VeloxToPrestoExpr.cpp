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
using namespace facebook::velox;

namespace facebook::presto::expression {

namespace {
const std::string kVariable = "variable";
const std::string kCall = "call";
const std::string kStatic = "$static";
const std::string kSpecial = "special";
const std::string kRowConstructor = "ROW_CONSTRUCTOR";
const std::string kSwitch = "SWITCH";
const std::string kWhen = "WHEN";

protocol::TypeSignature getTypeSignature(const TypePtr& type) {
  std::string typeSignature = type->toString();
  if (type->isPrimitiveType()) {
    boost::algorithm::to_lower(typeSignature);
  } else {
    // toString for Row type results in characters like `"":` for constants,
    // which need to be removed from protocol::TypeSignature.
    boost::algorithm::erase_all(typeSignature, "\"\":");
    boost::algorithm::replace_all(typeSignature, "<", "(");
    boost::algorithm::replace_all(typeSignature, ">", ")");
    boost::algorithm::to_lower(typeSignature);
  }
  return typeSignature;
}

std::shared_ptr<protocol::VariableReferenceExpression>
getVariableReferenceExpression(const core::FieldAccessTypedExpr* field) {
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
  return kPrestoSpecialForms.count(name) != 0;
}

json getWhenSpecialForm(const TypePtr& type, const json::array_t& whenArgs) {
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
    const VectorPtr& vector) const {
  std::ostringstream output;
  serde_->serializeSingleColumn(vector, nullptr, pool_, &output);
  const auto serialized = output.str();
  const auto serializedSize = serialized.size();
  return encoding::Base64::encode(serialized.c_str(), serializedSize);
}

ConstantExpressionPtr VeloxToPrestoExprConverter::getConstantExpression(
    const core::ConstantTypedExpr* constantExpr) {
  protocol::ConstantExpression cexpr;
  cexpr.type = getTypeSignature(constantExpr->type());
  cexpr.valueBlock.data = getValueBlock(constantExpr->toConstantVector(pool_));
  return std::make_shared<protocol::ConstantExpression>(cexpr);
}

std::vector<RowExpressionPtr>
VeloxToPrestoExprConverter::getSwitchSpecialFormExpressionArgs(
    const core::CallTypedExpr* switchExpr) {
  std::vector<RowExpressionPtr> result;
  const auto& switchInputs = switchExpr->inputs();
  const auto numInputs = switchInputs.size();

  for (auto i = 0; i < numInputs - 1; i += 2) {
    json::array_t resultWhenArgs;
    if (const auto* constantExpr =
            switchInputs[i]->asUnchecked<core::ConstantTypedExpr>()) {
      resultWhenArgs.emplace_back(getConstantExpression(constantExpr));
    } else {
      VELOX_CHECK(!switchInputs[i]->inputs().empty());
      resultWhenArgs.emplace_back(getRowExpression(switchInputs[i]));
    }

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
    const core::CallTypedExpr* expr) {
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
  if (name == kSwitch) {
    result.arguments = getSwitchSpecialFormExpressionArgs(expr);
  } else {
    // Presto special form expressions that are not of type `SWITCH`, such as
    // `IN`, `AND`, `OR` etc,. are handled in this clause. The list of Presto
    // special form expressions can be found in `kPrestoSpecialForms` in the
    // helper function `isPrestoSpecialForm`.
    auto exprInputs = expr->inputs();
    const auto numInputs = exprInputs.size();
    for (auto i = 0; i < numInputs; i++) {
      result.arguments.push_back(getRowExpression(exprInputs[i]));
    }
  }

  return std::make_shared<protocol::SpecialFormExpression>(result);
}

SpecialFormExpressionPtr
VeloxToPrestoExprConverter::getRowConstructorExpression(
    const core::ConstantTypedExpr* constantExpr) {
  json result;
  result["@type"] = kSpecial;
  result["form"] = kRowConstructor;
  result["returnType"] = getTypeSignature(constantExpr->valueVector()->type());
  const auto& value = constantExpr->valueVector();
  const auto* constVector = value->as<ConstantVector<ComplexType>>();
  const auto* rowVector = constVector->valueVector()->as<RowVector>();
  const auto type = asRowType(constantExpr->type());
  const auto numInputs = rowVector->children().size();

  protocol::ConstantExpression cexpr;
  json j;
  result["arguments"] = json::array();
  for (auto i = 0; i < numInputs; i++) {
    cexpr.type = getTypeSignature(type->childAt(i));
    cexpr.valueBlock.data = getValueBlock(rowVector->childAt(i));
    protocol::to_json(j, cexpr);
    result["arguments"].push_back(j);
  }
  return result;
}

CallExpressionPtr VeloxToPrestoExprConverter::getCallExpression(
    const core::CallTypedExpr* expr) {
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
  result["functionHandle"] = builtInFunctionHandle;
  result["returnType"] = getTypeSignature(expr->type());
  result["arguments"] = json::array();
  for (const auto& exprInput : exprInputs) {
    result["arguments"].push_back(getRowExpression(exprInput));
  }

  return result;
}

RowExpressionPtr VeloxToPrestoExprConverter::getRowExpression(
    const core::TypedExprPtr& expr) {
  switch (expr->kind()) {
    case core::ExprKind::kConstant: {
      const auto* constantExpr = expr->asUnchecked<core::ConstantTypedExpr>();
      // ConstantTypedExpr of ROW type maps to SpecialFormExpression of type
      // ROW_CONSTRUCTOR in Presto.
      if (expr->type()->isRow()) {
        return getRowConstructorExpression(constantExpr);
      }
      return getConstantExpression(constantExpr);
    }
    case core::ExprKind::kFieldAccess: {
      const auto* field = expr->asUnchecked<core::FieldAccessTypedExpr>();
      return getVariableReferenceExpression(field);
    }
    case core::ExprKind::kDereference: {
      const auto* dereferenceTypedExpr =
          expr->asUnchecked<core::DereferenceTypedExpr>();
      return getRowExpression(dereferenceTypedExpr->inputs().at(0));
    }
    case core::ExprKind::kCast: {
      // Velox CastTypedExpr maps to Presto CallExpression.
      const auto* castExpr = expr->asUnchecked<core::CastTypedExpr>();
      auto call = std::make_shared<core::CallTypedExpr>(
          expr->type(), castExpr->inputs(), velox::expression::kCast);
      return getCallExpression(call.get());
    }
    case core::ExprKind::kCall: {
      const auto* callTypedExpr = expr->asUnchecked<core::CallTypedExpr>();
      // Check if special form expression or call expression.
      auto exprName = callTypedExpr->name();
      boost::algorithm::to_lower(exprName);
      if (isPrestoSpecialForm(exprName)) {
        return getSpecialFormExpression(callTypedExpr);
      }
      return getCallExpression(callTypedExpr);
    }
    case core::ExprKind::kConcat:
      [[fallthrough]];
    case core::ExprKind::kInput:
      [[fallthrough]];
    case core::ExprKind::kLambda:
      break;
  }

  LOG(ERROR) << fmt::format(
      "Unable to convert Velox expression: {} of kind: {} to Presto RowExpression.",
      expr->toString(),
      core::ExprKindName::toName(expr->kind()));
  return nullptr;
}

} // namespace facebook::presto::expression
