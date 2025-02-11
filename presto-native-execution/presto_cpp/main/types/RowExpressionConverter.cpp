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
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
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
  if (type->isPrimitiveType()) {
    typeSignature = type->toString();
    boost::algorithm::to_lower(typeSignature);
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

json getWhenSpecialForm(const TypePtr& type, const json::array_t& whenArgs) {
  json when;
  when["@type"] = kSpecial;
  when["form"] = kWhen;
  when["arguments"] = whenArgs;
  when["returnType"] = getTypeSignature(type);
  return when;
}

std::vector<RowExpressionPtr> getRowExpressionArguments(
    const RowExpressionPtr& input) {
  std::vector<RowExpressionPtr> arguments;
  if (input->_type == kSpecial) {
    auto inputSpecialForm =
        dynamic_cast<protocol::SpecialFormExpression*>(input.get());
    VELOX_CHECK_NOT_NULL(inputSpecialForm);
    arguments = inputSpecialForm->arguments;
  } else if (input->_type == kCall) {
    auto inputCall = dynamic_cast<protocol::CallExpression*>(input.get());
    VELOX_CHECK_NOT_NULL(inputCall);
    arguments = inputCall->arguments;
  } else {
    VELOX_USER_FAIL(
        "Input should be a SpecialFormExpression or CallExpression");
  }
  return arguments;
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

std::string RowExpressionConverter::getValueBlock(
    const VectorPtr& vector) const {
  std::ostringstream output;
  serde_->serializeSingleColumn(vector, nullptr, pool_, &output);
  const auto serialized = output.str();
  const auto serializedSize = serialized.size();
  return encoding::Base64::encode(serialized.c_str(), serializedSize);
}

json RowExpressionConverter::getConstantRowExpression(
    const std::shared_ptr<const exec::ConstantExpr>& constantExpr) {
  protocol::ConstantExpression cexpr;
  cexpr.type = getTypeSignature(constantExpr->type());
  cexpr.valueBlock.data = getValueBlock(constantExpr->value());
  json result;
  protocol::to_json(result, cexpr);
  return result;
}

RowExpressionConverter::SwitchFormArguments
RowExpressionConverter::getSimpleSwitchFormArgs(
    const exec::SwitchExpr* switchExpr,
    const std::vector<RowExpressionPtr>& arguments) {
  SwitchFormArguments result;
  // Consider the following Presto query with simple form switch expression:
  // SELECT CASE 1 WHEN 2 THEN 31 - 1 WHEN 1 THEN 32 + 1 WHEN orderkey THEN 34
  //  ELSE 35 END FROM orders;
  // When this Presto expression is converted to velox switch expression, the
  // inputs to the velox expression contain the following `equal` expressions:
  // eq(1, 2), eq(1, 1), eq(1, orderkey). The resultant Presto simple form
  // switch expression requires the `expression` to be the first `argument`,
  // please refer to the syntax of Presto simple form switch expression here:
  // https://prestodb.io/docs/current/functions/conditional.html#case). It is
  // not possible to get the value of `expression` from velox, since these
  // equal expressions could all evaluate to either `true` or `false` during
  // constant folding. Hence, this `expression` is obtained from the input
  // Presto switch expression.
  result.arguments.emplace_back(arguments[0]);
  const auto& switchInputs = switchExpr->inputs();
  const auto numInputs = switchInputs.size();

  for (auto i = 0; i < numInputs - 1; i += 2) {
    json::array_t resultWhenArgs;
    const vector_size_t argsIdx = i / 2 + 1;
    const auto& caseValue = switchInputs[i + 1];
    auto inputWhenArgs = getRowExpressionArguments(arguments[argsIdx]);

    if (switchInputs[i]->isConstant()) {
      auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(switchInputs[i]);
      if (auto constVector =
              constantExpr->value()->as<ConstantVector<bool>>()) {
        // If this is the first switch case that evaluates to true, return the
        // expression corresponding to this case. From the aforementioned
        // example, `eq(1, 1)` evaluates to true, so the value corresponding to
        // the WHEN clause (`CASE 1 WHEN 1 THEN 32 + 1`), `33` is returned.
        if (constVector->valueAt(0) && result.arguments.size() == 1) {
          return {
              true,
              veloxToPrestoRowExpression(caseValue, inputWhenArgs[1]),
              json::array()};
        } else {
          // Skip switch cases that evaluate to false. From the aforementioned
          // example, `eq(1, 2)` evaluates to false, so the corresponding WHEN
          // clause, `CASE 1 WHEN 2 THEN 31 - 1`, is not included in the output
          // switch expression's arguments.
          continue;
        }
      } else {
        resultWhenArgs.emplace_back(getConstantRowExpression(constantExpr));
      }
    } else {
      VELOX_CHECK(!switchInputs[i]->inputs().empty());
      const auto& matchExpr = switchInputs[i]->inputs().back();
      resultWhenArgs.emplace_back(
          veloxToPrestoRowExpression(matchExpr, inputWhenArgs[0]));
    }

    resultWhenArgs.emplace_back(
        veloxToPrestoRowExpression(caseValue, inputWhenArgs[1]));
    result.arguments.emplace_back(
        getWhenSpecialForm(switchInputs[i + 1]->type(), resultWhenArgs));
  }

  // Else clause.
  if (numInputs % 2 != 0) {
    result.arguments.emplace_back(veloxToPrestoRowExpression(
        switchInputs[numInputs - 1], arguments.back()));
  }
  return result;
}

RowExpressionConverter::SwitchFormArguments
RowExpressionConverter::getSpecialSwitchFormArgs(
    const exec::SwitchExpr* switchExpr,
    const std::vector<RowExpressionPtr>& arguments) {
  // The searched form of CASE conditional expression in Presto needs to be
  // handled differently from the simple form (please refer to:
  // https://prestodb.io/docs/current/functions/conditional.html#case). The
  // searched form can be detected by the presence of a boolean value in the
  // first argument. This default boolean argument is not present in the velox
  // switch expression, so it is added to the arguments of output switch
  // expression unchanged.
  if (arguments[0]->_type == kConstant) {
    if (auto constantRowExpr =
            dynamic_cast<protocol::ConstantExpression*>(arguments[0].get())) {
      if (constantRowExpr->type == kBoolean) {
        SwitchFormArguments result;
        const auto& switchInputs = switchExpr->inputs();
        const auto numInputs = switchInputs.size();
        result.arguments = {arguments[0]};
        for (auto i = 0; i < numInputs - 1; i += 2) {
          const vector_size_t argsIdx = i / 2 + 1;
          std::vector<RowExpressionPtr> inputWhenArgs =
              getRowExpressionArguments(arguments[argsIdx]);
          json::array_t resultWhenArgs;
          resultWhenArgs.emplace_back(
              veloxToPrestoRowExpression(switchInputs[i], inputWhenArgs[0]));
          resultWhenArgs.emplace_back(veloxToPrestoRowExpression(
              switchInputs[i + 1], inputWhenArgs[1]));
          result.arguments.emplace_back(
              getWhenSpecialForm(switchInputs[i + 1]->type(), resultWhenArgs));
        }

        // Else clause.
        if (numInputs % 2 != 0) {
          result.arguments.emplace_back(veloxToPrestoRowExpression(
              switchInputs[numInputs - 1], arguments.back()));
        }
        return result;
      }
    }
  }

  return getSimpleSwitchFormArgs(switchExpr, arguments);
}

json RowExpressionConverter::getSpecialForm(
    const exec::ExprPtr& expr,
    const RowExpressionPtr& input) {
  json result;
  result["@type"] = kSpecial;
  result["returnType"] = getTypeSignature(expr->type());
  auto form = expr->name();
  // Presto requires the field form to be in upper case.
  std::transform(form.begin(), form.end(), form.begin(), ::toupper);
  result["form"] = form;
  std::vector<RowExpressionPtr> inputArguments =
      getRowExpressionArguments(input);

  // Arguments for switch expression include 'WHEN' special form expression(s)
  // so they are constructed separately. If the switch expression evaluation
  // found a case that always evaluates to `true`, the field 'isSimplified' in
  // the result is `true` and the field 'caseExpression' contains the value
  // corresponding to the simplified switch case. Otherwise, 'isSimplified' is
  // false and the field 'arguments' contains the 'when' clauses needed by the
  // Presto switch SpecialFormExpression.
  if (form == kSwitch) {
    auto switchExpr = dynamic_cast<exec::SwitchExpr*>(expr.get());
    VELOX_CHECK_NOT_NULL(switchExpr);
    auto switchResult = getSpecialSwitchFormArgs(switchExpr, inputArguments);
    if (switchResult.isSimplified) {
      return switchResult.caseExpression;
    } else {
      result["arguments"] = switchResult.arguments;
    }
  } else {
    // Presto special form expressions that are not of type `SWITCH`, such as
    // `IN`, `AND`, `OR` etc,. are handled in this clause. The list of Presto
    // special form expressions can be found in `kPrestoSpecialForms` in the
    // helper function `isPrestoSpecialForm`.
    auto exprInputs = expr->inputs();
    const auto numInputs = exprInputs.size();
    if (form == kCoalesce) {
      VELOX_CHECK_LE(numInputs, inputArguments.size());
    } else {
      VELOX_CHECK_EQ(numInputs, inputArguments.size());
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

json RowExpressionConverter::toCallRowExpression(
    const exec::ExprPtr& expr,
    const RowExpressionPtr& input) {
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
    result["arguments"].push_back(veloxToPrestoRowExpression(exprInput, input));
  }

  return result;
}

json RowExpressionConverter::veloxToPrestoRowExpression(
    const exec::ExprPtr& expr,
    const RowExpressionPtr& input) {
  if (expr->isConstant()) {
    if (expr->inputs().empty()) {
      auto constantExpr =
          std::dynamic_pointer_cast<const exec::ConstantExpr>(expr);
      VELOX_CHECK_NOT_NULL(constantExpr);
      // Constant velox expressions of ROW type map to ROW_CONSTRUCTOR special
      // form expression in Presto.
      if (expr->type()->isRow()) {
        return getRowConstructorSpecialForm(constantExpr);
      }
      return getConstantRowExpression(constantExpr);
    } else {
      // Expressions such as 'divide(0, 0)' are not constant folded during
      // compilation in velox, since they throw an exception (Divide by zero in
      // this example) during evaluation (see function `tryFoldIfConstant` in
      // `velox/expression/ExprCompiler.cpp`). The input expression is returned
      // unchanged in such cases.
      return input;
    }
  }

  if (auto field =
          std::dynamic_pointer_cast<const exec::FieldReference>(expr)) {
    return toVariableReferenceExpression(field);
  }

  // Check if special form expression or call expression.
  auto exprName = expr->name();
  boost::algorithm::to_lower(exprName);
  if (isPrestoSpecialForm(exprName)) {
    return getSpecialForm(expr, input);
  }
  return toCallRowExpression(expr, input);
}

} // namespace facebook::presto::expression
