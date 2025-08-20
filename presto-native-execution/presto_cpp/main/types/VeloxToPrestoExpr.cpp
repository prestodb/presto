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
#include "velox/vector/ConstantVector.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace facebook::presto::expression {

namespace {
const std::string kVariable = "variable";
const std::string kCall = "call";
const std::string kStatic = "$static";
const std::string kSpecial = "special";
const std::string kCoalesce = "COALESCE";
const std::string kIn = "IN";
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
getVariableReferenceExpression(const core::FieldAccessTypedExprPtr& field) {
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

std::string VeloxToPrestoExprConverter::getValueBlock(
    const VectorPtr& vector) const {
  std::ostringstream output;
  serde_->serializeSingleColumn(vector, nullptr, pool_, &output);
  const auto serialized = output.str();
  const auto serializedSize = serialized.size();
  return encoding::Base64::encode(serialized.c_str(), serializedSize);
}

ConstantExpressionPtr VeloxToPrestoExprConverter::getConstantExpression(
    const core::ConstantTypedExprPtr& constantExpr) {
  protocol::ConstantExpression cexpr;
  cexpr.type = getTypeSignature(constantExpr->type());
  cexpr.valueBlock.data = getValueBlock(constantExpr->toConstantVector(pool_));
  return std::make_shared<protocol::ConstantExpression>(cexpr);
}

std::vector<RowExpressionPtr>
VeloxToPrestoExprConverter::getSwitchSpecialFormExpressionArgs(
    const core::CallTypedExprPtr& switchExpr,
    const std::vector<RowExpressionPtr>& arguments) {
  std::vector<RowExpressionPtr> result;
  result.emplace_back(arguments[0]);
  const auto& switchInputs = switchExpr->inputs();
  const auto numInputs = switchInputs.size();

  for (auto i = 0; i < numInputs - 1; i += 2) {
    json::array_t resultWhenArgs;
    if (auto constantExpr =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                switchInputs[i])) {
      resultWhenArgs.emplace_back(getConstantExpression(constantExpr));
    } else {
      VELOX_CHECK(!switchInputs[i]->inputs().empty());
      const vector_size_t argsIdx = i / 2 + 1;
      auto inputWhenArgs = getRowExpressionArguments(arguments[argsIdx]);
      resultWhenArgs.emplace_back(
          getRowExpression(switchInputs[i], inputWhenArgs[0]));
    }

    result.emplace_back(
        getWhenSpecialForm(switchInputs[i + 1]->type(), resultWhenArgs));
  }

  // Else clause.
  if (numInputs % 2 != 0) {
    result.emplace_back(
        getRowExpression(switchInputs[numInputs - 1], arguments.back()));
  }
  return result;
}

SpecialFormExpressionPtr VeloxToPrestoExprConverter::getSpecialFormExpression(
    const core::CallTypedExprPtr& expr,
    const RowExpressionPtr& input) {
  protocol::SpecialFormExpression result;
  result._type = kSpecial;
  result.returnType = getTypeSignature(expr->type());
  auto name = expr->name();
  // Presto requires the field form to be in upper case.
  std::transform(name.begin(), name.end(), name.begin(), ::toupper);
  protocol::Form form;
  protocol::from_json(name, form);
  result.form = form;
  std::vector<RowExpressionPtr> inputArguments =
      getRowExpressionArguments(input);

  // Arguments for switch expression include 'WHEN' special form expression(s)
  // so they are constructed separately.
  if (name == kSwitch) {
    result.arguments = getSwitchSpecialFormExpressionArgs(expr, inputArguments);
  } else {
    // Presto special form expressions that are not of type `SWITCH`, such as
    // `IN`, `AND`, `OR` etc,. are handled in this clause. The list of Presto
    // special form expressions can be found in `kPrestoSpecialForms` in the
    // helper function `isPrestoSpecialForm`.
    auto exprInputs = expr->inputs();
    const auto numInputs = exprInputs.size();
    if (name == kIn) {
      // Inputs to IN are optimized by removing literals that don't match the
      // value being compared.
      VELOX_CHECK_LE(numInputs, inputArguments.size());
    } else if (name != kCoalesce) {
      // Inputs to COALESCE are optimized by deduplication and removing NULLs,
      // which could result in numInputs < inputArguments.size(). Inputs to
      // COALESCE are also unnested so numInputs can be greater than
      // inputArguments.size().
      VELOX_CHECK_EQ(numInputs, inputArguments.size());
    }
    for (auto i = 0; i < numInputs; i++) {
      result.arguments.push_back(
          getRowExpression(exprInputs[i], inputArguments[i]));
    }
  }

  return std::make_shared<protocol::SpecialFormExpression>(result);
}

SpecialFormExpressionPtr
VeloxToPrestoExprConverter::getRowConstructorExpression(
    const core::ConstantTypedExprPtr& constantExpr) {
  json result;
  result["@type"] = kSpecial;
  result["form"] = kRowConstructor;
  result["returnType"] = getTypeSignature(constantExpr->valueVector()->type());
  auto value = constantExpr->valueVector();
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

CallExpressionPtr VeloxToPrestoExprConverter::getCallExpression(
    const core::CallTypedExprPtr& expr,
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
    result["arguments"].push_back(getRowExpression(exprInput, input));
  }

  return result;
}

RowExpressionPtr VeloxToPrestoExprConverter::getRowExpression(
    const core::TypedExprPtr& expr,
    const RowExpressionPtr& input) {
  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
    // Constant velox expressions of ROW type map to ROW_CONSTRUCTOR special
    // form expression in Presto.
    if (expr->type()->isRow()) {
      return getRowConstructorExpression(constantExpr);
    } else {
      return getConstantExpression(constantExpr);
    }
  } else if (
      auto field =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
    return getVariableReferenceExpression(field);
  } else if (
      auto callTypedExpr =
          std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    // Check if special form expression or call expression.
    auto exprName = callTypedExpr->name();
    boost::algorithm::to_lower(exprName);
    if (isPrestoSpecialForm(exprName)) {
      return getSpecialFormExpression(callTypedExpr, input);
    }
    return getCallExpression(callTypedExpr, input);
  } else if (
      auto castExpr =
          std::dynamic_pointer_cast<const core::CastTypedExpr>(expr)) {
    auto call = std::make_shared<core::CallTypedExpr>(
        expr->type(), castExpr->inputs(), "cast");
    return getCallExpression(call, input);
  }

  LOG(ERROR) << "Unable to convert Velox expression: {}" << expr->toString();
  return input;
}

} // namespace facebook::presto::expression
