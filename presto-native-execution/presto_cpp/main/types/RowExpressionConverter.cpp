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
#include "presto_cpp/main/types/RowExpressionConverter.h"
#include "velox/expression/FieldReference.h"

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
const std::string kCoalesce = "COALESCE";
const std::string kRowConstructor = "ROW_CONSTRUCTOR";
const std::string kSwitch = "SWITCH";
const std::string kWhen = "WHEN";

protocol::TypeSignature getTypeSignature(const TypePtr& type) {
  std::string typeSignature;
  if (type->parameters().empty()) {
    typeSignature = type->toString();
    boost::algorithm::to_lower(typeSignature);
  } else if (type->isDecimal()) {
    typeSignature = type->toString();
  } else {
    std::string complexTypeString;
    std::vector<TypePtr> childTypes;
    if (type->isRow()) {
      complexTypeString = "row";
      childTypes = asRowType(type)->children();
    } else if (type->isArray()) {
      complexTypeString = "array";
      childTypes = type->asArray().children();
    } else if (type->isMap()) {
      complexTypeString = "map";
      const auto mapType = type->asMap();
      childTypes = {mapType.keyType(), mapType.valueType()};
    } else {
      VELOX_USER_FAIL("Invalid type {}", type->toString());
    }

    typeSignature = complexTypeString + "(";
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
    const std::shared_ptr<const exec::FieldReference>& field) {
  protocol::VariableReferenceExpression vexpr;
  vexpr.name = field->name();
  vexpr._type = kVariable;
  vexpr.type = getTypeSignature(field->type());
  json result;
  protocol::to_json(result, vexpr);

  return result;
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

json getWhenSpecialForm(
    const exec::ExprPtr& input,
    const json::array_t& whenArgs) {
  json when;
  when["@type"] = kSpecial;
  when["form"] = kWhen;
  when["arguments"] = whenArgs;
  when["returnType"] = getTypeSignature(input->type());
  return when;
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

SwitchFormArguments RowExpressionConverter::getSimpleSwitchFormArgs(
    const json::array_t& inputArgs,
    const exec::ExprPtr& switchExpr) {
  SwitchFormArguments result;
  // The switch case's 'expression' in simple form of the conditional cannot be
  // inferred from Velox since it could evaluate all 'when' clauses to true or
  // false, so we get it from the input json.
  result.arguments.emplace_back(inputArgs[0]);
  const auto& switchInputs = switchExpr->inputs();
  const auto numInputs = switchInputs.size();

  for (auto i = 0; i < numInputs - 1; i += 2) {
    json::array_t whenArgs;
    const vector_size_t argsIdx = i / 2 + 1;
    const auto& caseValue = switchInputs[i + 1];
    json::array_t inputWhenArgs = inputArgs[argsIdx].at("arguments");

    if (switchInputs[i]->isConstant()) {
      auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(switchInputs[i]);
      if (auto constVector =
              constantExpr->value()->as<ConstantVector<bool>>()) {
        // If this is the first switch case that evaluates to true, return the
        // expression corresponding to this case.
        if (constVector->valueAt(0) && result.arguments.size() == 1) {
          return {
              true,
              veloxToPrestoRowExpression(caseValue, inputWhenArgs[1]),
              json::array()};
        } else {
          // Skip switch cases that evaluate to false.
          continue;
        }
      } else {
        whenArgs.emplace_back(getConstantRowExpression(constantExpr));
      }
    } else {
      VELOX_USER_CHECK(!switchInputs[i]->inputs().empty());
      const auto& matchExpr = switchInputs[i]->inputs().back();
      whenArgs.emplace_back(
          veloxToPrestoRowExpression(matchExpr, inputWhenArgs[0]));
    }

    whenArgs.emplace_back(
        veloxToPrestoRowExpression(caseValue, inputWhenArgs[1]));
    result.arguments.emplace_back(
        getWhenSpecialForm(switchInputs[i + 1], whenArgs));
  }

  // Else clause.
  if (numInputs % 2 != 0) {
    result.arguments.emplace_back(veloxToPrestoRowExpression(
        switchInputs[numInputs - 1], inputArgs.back()));
  }
  return result;
}

SwitchFormArguments RowExpressionConverter::getSwitchSpecialFormArgs(
    const exec::ExprPtr& switchExpr,
    const json& input) {
  json::array_t inputArgs = input["arguments"];
  // The searched form of the conditional expression needs to be handled
  // differently from the simple form. The searched form can be detected by the
  // presence of a boolean value in the first argument. This default boolean
  // argument is not present in the Velox switch expression, so it is added to
  // the arguments of output switch expression unchanged.
  if (inputArgs[0].at("@type") == kConstant &&
      inputArgs[0].at("type") == kBoolean) {
    SwitchFormArguments result;
    const auto& switchInputs = switchExpr->inputs();
    const auto numInputs = switchInputs.size();
    result.arguments = {inputArgs[0]};
    for (auto i = 0; i < numInputs - 1; i += 2) {
      const vector_size_t argsIdx = i / 2 + 1;
      json::array_t inputWhenArgs = inputArgs[argsIdx].at("arguments");
      json::array_t whenArgs;
      whenArgs.emplace_back(
          veloxToPrestoRowExpression(switchInputs[i], inputWhenArgs[0]));
      whenArgs.emplace_back(
          veloxToPrestoRowExpression(switchInputs[i + 1], inputWhenArgs[1]));
      result.arguments.emplace_back(
          getWhenSpecialForm(switchInputs[i + 1], whenArgs));
    }

    // Else clause.
    if (numInputs % 2 != 0) {
      result.arguments.emplace_back(veloxToPrestoRowExpression(
          switchInputs[numInputs - 1], inputArgs.back()));
    }
    return result;
  }

  return getSimpleSwitchFormArgs(inputArgs, switchExpr);
}

json RowExpressionConverter::getSpecialForm(
    const exec::ExprPtr& expr,
    const json& input) {
  json result;
  result["@type"] = kSpecial;
  result["returnType"] = getTypeSignature(expr->type());
  auto form = expr->name();
  // Presto requires the field form to be in upper case.
  std::transform(form.begin(), form.end(), form.begin(), ::toupper);
  result["form"] = form;

  // Arguments for switch expression include special form expression 'when'
  // so they are constructed separately. If the switch expression evaluation
  // found a case that always evaluates to true, the field 'isSimplified' in the
  // result will be true and the field 'caseExpression' contains the value
  // corresponding to the simplified switch case. Otherwise, 'isSimplified' will
  // be false and the field 'arguments' will contain the 'when' clauses needed
  // by switch SpecialFormExpression in Presto.
  if (form == kSwitch) {
    auto switchResult = getSwitchSpecialFormArgs(expr, input);
    if (switchResult.isSimplified) {
      return switchResult.caseExpression;
    } else {
      result["arguments"] = switchResult.arguments;
    }
  } else {
    json::array_t inputArguments = input["arguments"];
    auto exprInputs = expr->inputs();
    const auto numInputs = exprInputs.size();
    if (form == kCoalesce) {
      VELOX_USER_CHECK_LE(numInputs, inputArguments.size());
    } else {
      VELOX_USER_CHECK_EQ(numInputs, inputArguments.size());
    }
    result["arguments"] = json::array();
    for (auto i = 0; i < numInputs; i++) {
      result["arguments"].push_back(
          veloxToPrestoRowExpression(exprInputs[i], inputArguments[i]));
    }
  }

  return result;
}

json RowExpressionConverter::getRowConstructorSpecialForm(
    std::shared_ptr<const exec::ConstantExpr>& constantExpr) {
  json result;
  result["@type"] = kSpecial;
  result["form"] = kRowConstructor;
  result["returnType"] = getTypeSignature(constantExpr->type());
  auto value = constantExpr->value();
  auto* constVector = value->as<ConstantVector<ComplexType>>();
  auto* rowVector = constVector->valueVector()->as<RowVector>();
  auto type = asRowType(constantExpr->type());
  auto size = rowVector->children().size();

  protocol::ConstantExpression cexpr;
  json j;
  result["arguments"] = json::array();
  for (auto i = 0; i < size; i++) {
    cexpr.type = getTypeSignature(type->childAt(i));
    cexpr.valueBlock.data = getValueBlock(rowVector->childAt(i));
    protocol::to_json(j, cexpr);
    result["arguments"].push_back(j);
  }
  return result;
}

json RowExpressionConverter::toConstantRowExpression(
    const exec::ExprPtr& expr) {
  auto constantExpr = std::dynamic_pointer_cast<const exec::ConstantExpr>(expr);
  VELOX_USER_CHECK_NOT_NULL(constantExpr);
  // Constant velox expressions of ROW type map to ROW_CONSTRUCTOR special form
  // expression in Presto.
  if (expr->type()->isRow()) {
    return getRowConstructorSpecialForm(constantExpr);
  }

  json result;
  auto constantRowExpr = getConstantRowExpression(constantExpr);
  protocol::to_json(result, constantRowExpr);
  return result;
}

json RowExpressionConverter::toCallRowExpression(
    const exec::ExprPtr& expr,
    const json& input) {
  json result;
  result["@type"] = kCall;
  protocol::Signature signature;
  std::string exprName = expr->name();
  if (veloxToPrestoOperatorMap_.find(exprName) !=
      veloxToPrestoOperatorMap_.end()) {
    exprName = veloxToPrestoOperatorMap_.at(exprName);
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
    result["arguments"].push_back(veloxToPrestoRowExpression(exprInput, input));
  }

  return result;
}

json RowExpressionConverter::veloxToPrestoRowExpression(
    const exec::ExprPtr& expr,
    const json& input) {
  if (expr->isConstant()) {
    if (expr->inputs().empty()) {
      return toConstantRowExpression(expr);
    } else {
      // Constant expressions with inputs could result in an exception when
      // constant folded in Velox, such as divide by zero, eg: divide(0, 0).
      // Such exceptions should be handled by Presto, so the input json is
      // returned as is and the expression is not evaluated in Velox.
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

} // namespace facebook::presto::expression
