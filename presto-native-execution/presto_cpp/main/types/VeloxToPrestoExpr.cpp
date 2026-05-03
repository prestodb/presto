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
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/replace.hpp>
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "velox/core/ITypedExpr.h"
#include "velox/expression/ExprConstants.h"
#include "velox/vector/BaseVector.h"
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

const std::unordered_map<std::string, std::string>&
veloxToPrestoInternalFunctionMap() {
  // Maps Velox internal function names to Presto internal/native function
  // names used when constructing function signatures.
  static const std::unordered_map<std::string, std::string>
      veloxToPrestoInternalFunctionMap = {
          {"try", "presto.default.$internal$try"},
          {"try_cast", "presto.default.try_cast"},
      };
  return veloxToPrestoInternalFunctionMap;
}

// If the function name prefix starts from "presto.default", then it is a built
// in function handle. Otherwise, it is a native function handle.
std::shared_ptr<protocol::FunctionHandle> getFunctionHandle(
    const std::string& name,
    const protocol::Signature& signature) {
  static constexpr char const* kStatic = "$static";
  static constexpr char const* kNativeFunctionHandle = "native";
  static constexpr char const* builtInCatalog = "presto";
  static constexpr char const* builtInSchema = "default";

  const auto parts = util::getFunctionNameParts(name);
  if ((parts[0] == builtInCatalog) && (parts[1] == builtInSchema)) {
    auto handle = std::make_shared<protocol::BuiltInFunctionHandle>();
    handle->_type = kStatic;
    handle->signature = signature;
    return handle;
  }

  auto handle = std::make_shared<protocol::NativeFunctionHandle>();
  handle->_type = kNativeFunctionHandle;
  handle->signature = signature;
  return handle;
}

// Collects free FieldAccessTypedExpr leaf nodes from a Velox expression tree
// that are not bound by any enclosing lambda's signature. The 'boundNames'
// parameter contains names that are in scope from enclosing lambda signatures.
// Captured fields are recorded in first DFS encounter order. The exact order is
// not semantically important to Presto, but BIND arguments and the prepended
// lambda parameters must use the same order. 'visitedFreeFieldNames' ensures
// each captured variable is bound once even if referenced multiple times.
void collectFreeFieldAccesses(
    const velox::core::TypedExprPtr& expr,
    const std::unordered_set<std::string>& boundNames,
    std::vector<FieldAccessTypedExprPtr>& freeFields,
    std::unordered_set<std::string>& visitedFreeFieldNames) {
  if (auto fieldAccess =
          std::dynamic_pointer_cast<const velox::core::FieldAccessTypedExpr>(
              expr)) {
    if (fieldAccess->isInputColumn() &&
        !boundNames.count(fieldAccess->name())) {
      if (visitedFreeFieldNames.insert(fieldAccess->name()).second) {
        freeFields.push_back(fieldAccess);
      }
    }
    // Recurse into inputs for dereference-like field accesses.
    for (const auto& input : fieldAccess->inputs()) {
      collectFreeFieldAccesses(
          input, boundNames, freeFields, visitedFreeFieldNames);
    }
    return;
  }

  if (auto lambda =
          std::dynamic_pointer_cast<const velox::core::LambdaTypedExpr>(expr)) {
    // For nested lambdas, extend the bound set with the lambda's parameters.
    auto innerBound = boundNames;
    const auto& names = lambda->signature()->names();
    innerBound.insert(names.begin(), names.end());
    collectFreeFieldAccesses(
        lambda->body(), innerBound, freeFields, visitedFreeFieldNames);
    return;
  }

  // Recurse into all inputs for other expression types.
  for (const auto& input : expr->inputs()) {
    collectFreeFieldAccesses(
        input, boundNames, freeFields, visitedFreeFieldNames);
  }
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

void VeloxToPrestoExprConverter::getArgsFromConstantInList(
    const velox::core::ConstantTypedExpr* inList,
    std::vector<RowExpressionPtr>& result) const {
  const auto inListVector = inList->toConstantVector(pool_);
  auto* constantVector =
      inListVector->as<velox::ConstantVector<velox::ComplexType>>();
  VELOX_CHECK_NOT_NULL(
      constantVector, "Expected ConstantVector of Array type for IN-list.");
  const auto* arrayVector =
      constantVector->wrappedVector()->as<velox::ArrayVector>();
  VELOX_CHECK_NOT_NULL(
      arrayVector,
      "Expected constant IN-list to be of Array type, but got {}.",
      constantVector->wrappedVector()->type()->toString());

  auto wrappedIdx = constantVector->wrappedIndex(0);
  auto size = arrayVector->sizeAt(wrappedIdx);
  auto offset = arrayVector->offsetAt(wrappedIdx);
  auto elementsVector = arrayVector->elements();

  for (velox::vector_size_t i = 0; i < size; i++) {
    auto elementIndex = offset + i;
    auto elementConstant =
        velox::BaseVector::wrapInConstant(1, elementIndex, elementsVector);
    // Construct a core::ConstantTypedExpr from the constant value at this
    // index in array vector, then convert it to a protocol::RowExpression.
    const auto constantExpr =
        std::make_shared<velox::core::ConstantTypedExpr>(elementConstant);
    result.push_back(getConstantExpression(constantExpr.get()));
  }
}

// IN expression in Presto is of form `expr0 IN [expr1, expr2, ..., exprN]`.
// The Velox representation of IN expression has the same form as Presto when
// any of the expressions in the IN list is non-constant; when the IN list only
// has constant expressions, it is of form `expr0 IN constantExpr(ARRAY[
// expr1.constantValue(), expr2.constantValue(), ..., exprN.constantValue()])`.
// This function retrieves the arguments to Presto IN expression from Velox IN
// expression in both of these forms.
std::vector<RowExpressionPtr>
VeloxToPrestoExprConverter::getInSpecialFormExpressionArgs(
    const velox::core::CallTypedExpr* inExpr) const {
  std::vector<RowExpressionPtr> result;
  const auto& inputs = inExpr->inputs();
  const auto numInputs = inputs.size();
  VELOX_CHECK_GE(numInputs, 2, "IN expression should have at least 2 inputs");

  // Value being searched for with this `IN` expression is always the first
  // input, convert it to a Presto expression.
  result.push_back(getRowExpression(inputs.at(0)));
  const auto& inList = inputs.at(1);
  if (numInputs == 2 && inList->isConstantKind()) {
    // Converts inputs from constant Velox IN-list to arguments in the Presto
    // `IN` expression. Eg: For expression `col0 IN ['apple', 'foo', `bar`]`,
    // `apple`, `foo`, and `bar` from the IN-list are converted to equivalent
    // Presto constant expressions.
    const auto* constantInList =
        inList->asUnchecked<velox::core::ConstantTypedExpr>();
    getArgsFromConstantInList(constantInList, result);
  } else {
    // Converts inputs from the Velox IN-list to arguments in the Presto `IN`
    // expression when the Velox IN-list has at least one non-constant
    // expression. Eg: For expression `col0 IN ['apple', col1, 'foo']`, `apple`,
    // col1, and `foo` from the IN-list are converted to equivalent
    // Presto expressions.
    for (auto i = 1; i < numInputs; i++) {
      result.push_back(getRowExpression(inputs[i]));
    }
  }
  return result;
}

SpecialFormExpressionPtr
VeloxToPrestoExprConverter::getNestedConjunctExpression(
    const std::vector<velox::core::TypedExprPtr>& inputs,
    protocol::Form form,
    const protocol::TypeSignature& returnType) const {
  // Convert N-argument AND/OR (N > 2) to nested binary operations
  // For AND(A, B, C, D), create AND(AND(AND(A, B), C), D)
  VELOX_CHECK(
      inputs.size() > 1,
      "getNestedConjunctExpression requires at least 2 inputs, got {}",
      inputs.size());
  protocol::SpecialFormExpression currentExpr;
  currentExpr.form = form;
  currentExpr.returnType = returnType;
  currentExpr.arguments = {
      getRowExpression(inputs[0]), getRowExpression(inputs[1])};

  for (size_t i = 2; i < inputs.size(); ++i) {
    protocol::SpecialFormExpression nestedExpr;
    nestedExpr.form = form;
    nestedExpr.returnType = returnType;
    nestedExpr.arguments.push_back(
        std::make_shared<protocol::SpecialFormExpression>(currentExpr));
    nestedExpr.arguments.push_back(getRowExpression(inputs[i]));
    currentExpr = std::move(nestedExpr);
  }
  return std::make_shared<protocol::SpecialFormExpression>(currentExpr);
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
  static constexpr char const* kIn = "IN";
  static constexpr char const* kAnd = "AND";
  static constexpr char const* kOr = "OR";

  auto exprInputs = expr->inputs();
  if (name == kSwitch) {
    result.arguments = getSwitchSpecialFormExpressionArgs(expr);
  } else if (name == kIn) {
    result.arguments = getInSpecialFormExpressionArgs(expr);
  } else if ((name == kAnd || name == kOr) && exprInputs.size() > 2) {
    // Presto AND/OR are binary operations (2 arguments only).
    // If Velox has optimized to N-argument AND/OR (N > 2), we need to
    // convert back to nested binary operations for Presto compatibility.
    // Binary or unary case (N <= 2) will be handled normally.
    return getNestedConjunctExpression(exprInputs, form, result.returnType);
  } else {
    // Presto special form expressions that are not of type `SWITCH`, `IN`
    // and (N <= 2) arguments `AND`, `OR` are handled in this clause. The list
    // of Presto special form expressions can be found in `kPrestoSpecialForms`
    // in the helper function `isPrestoSpecialForm`.
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

  // Check if the ROW constant is NULL. If so, it should not be converted to
  // ROW_CONSTRUCTOR but rather kept as a NULL ConstantExpression.
  VELOX_CHECK(
      !constantExpr->isNull(),
      "getRowConstructorExpression should not be called for NULL ROW constants. "
      "Expression type: {}, Expression string: {}. ",
      constantExpr->type()->toString(),
      constantExpr->toString());

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
          velox::INTEGER(), static_cast<int32_t>(dereferenceExpr->index()))};
  for (const auto& input : dereferenceInputs) {
    const auto rowExpr = getRowExpression(input);
    protocol::to_json(j, rowExpr);
    result["arguments"].push_back(j);
  }

  return result;
}

RowExpressionPtr VeloxToPrestoExprConverter::getLambdaExpression(
    const velox::core::LambdaTypedExpr* lambdaExpr) const {
  const auto& signature = lambdaExpr->signature();

  std::unordered_set<std::string> boundNames(
      signature->names().begin(), signature->names().end());
  std::vector<FieldAccessTypedExprPtr> freeFields;
  std::unordered_set<std::string> visitedFreeFieldNames;
  collectFreeFieldAccesses(
      lambdaExpr->body(), boundNames, freeFields, visitedFreeFieldNames);

  return resolveLambdaExpression(lambdaExpr, freeFields);
}

RowExpressionPtr VeloxToPrestoExprConverter::resolveLambdaExpression(
    const velox::core::LambdaTypedExpr* lambdaExpr,
    const std::vector<FieldAccessTypedExprPtr>& freeFields) const {
  static constexpr char const* kLambda = "lambda";
  json result;
  result["@type"] = kLambda;
  const auto& signature = lambdaExpr->signature();

  std::vector<protocol::TypeSignature> argumentTypes;
  std::vector<std::string> arguments;
  argumentTypes.reserve(freeFields.size() + signature->children().size());
  arguments.reserve(freeFields.size() + signature->names().size());

  // Prepend captured variable names/types to the lambda signature.
  for (const auto& field : freeFields) {
    arguments.emplace_back(field->name());
    argumentTypes.emplace_back(getTypeSignature(field->type()));
  }
  // Then add the original lambda arguments.
  for (const auto& type : signature->children()) {
    argumentTypes.emplace_back(getTypeSignature(type));
  }
  for (const auto& name : signature->names()) {
    arguments.emplace_back(name);
  }

  result["argumentTypes"] = argumentTypes;
  result["arguments"] = arguments;
  result["body"] = getRowExpression(lambdaExpr->body());

  if (freeFields.empty()) {
    // Return a plain LambdaDefinitionExpression when there are no captured
    // variables.
    return result;
  }

  VELOX_CHECK(!freeFields.empty(), "BIND expression requires captured fields.");
  static constexpr char const* kBind = "BIND";
  LambdaDefinitionExpressionPtr lambdaDefinitionExpression = result;

  // BIND(capturedVar1, ..., expandedLambda) provides captured outer variables
  // to the lambda at evaluation time.
  json bind;
  bind["@type"] = kSpecial;
  bind["form"] = kBind;
  bind["returnType"] = getTypeSignature(lambdaExpr->type());

  bind["arguments"] = json::array();
  for (const auto& field : freeFields) {
    bind["arguments"].push_back(getVariableReferenceExpression(field.get()));
  }
  bind["arguments"].push_back(lambdaDefinitionExpression);
  return bind;
}

CallExpressionPtr VeloxToPrestoExprConverter::getCallExpression(
    const velox::core::CallTypedExpr* expr) const {
  static constexpr char const* kCall = "call";

  json result;
  result["@type"] = kCall;
  protocol::Signature signature;
  std::string veloxExprName = expr->name();
  std::string prestoExprName;

  // Map Velox expression to the right Presto expression name when constructing
  // the Presto function signature.
  const auto& opMap = veloxToPrestoOperatorMap();
  auto mapIter = opMap.find(veloxExprName);
  if (mapIter != opMap.end()) {
    prestoExprName = mapIter->second;
  } else {
    const auto& internalFunctionMap = veloxToPrestoInternalFunctionMap();
    auto internalMapIter = internalFunctionMap.find(veloxExprName);
    if (internalMapIter != internalFunctionMap.end()) {
      prestoExprName = internalMapIter->second;
    } else {
      prestoExprName = veloxExprName;
    }
  }

  signature.name = prestoExprName;
  result["displayName"] = prestoExprName;
  signature.kind = protocol::FunctionKind::SCALAR;
  signature.typeVariableConstraints = {};
  signature.longVariableConstraints = {};
  signature.variableArity = false;
  signature.returnType = getTypeSignature(expr->type());

  std::vector<protocol::TypeSignature> argumentTypes;
  auto exprInputs = expr->inputs();
  argumentTypes.reserve(exprInputs.size());
  bool isTryExpression = (veloxExprName == velox::expression::kTry);
  result["arguments"] = json::array();
  if (isTryExpression) {
    VELOX_CHECK_EQ(
        exprInputs.size(),
        1,
        "Velox TRY expression should have exactly 1 input, but got {}.",
        exprInputs.size());

    // Presto '$internal$try' expects a lambda with no arguments: () -> T.
    // Construct a "function(T)" type signature for the lambda argument.
    const auto lambdaExpr = std::make_shared<velox::core::LambdaTypedExpr>(
        velox::ROW({}), exprInputs.at(0));
    argumentTypes.emplace_back(getTypeSignature(lambdaExpr->type()));
    result["arguments"].push_back(getLambdaExpression(lambdaExpr.get()));
  } else {
    for (const auto& input : exprInputs) {
      argumentTypes.emplace_back(getTypeSignature(input->type()));
      result["arguments"].push_back(getRowExpression(input));
    }
  }
  signature.argumentTypes = argumentTypes;

  result["functionHandle"] = getFunctionHandle(prestoExprName, signature);
  result["returnType"] = getTypeSignature(expr->type());

  return result;
}

RowExpressionPtr VeloxToPrestoExprConverter::getRowExpression(
    const velox::core::TypedExprPtr& expr) const {
  switch (expr->kind()) {
    case velox::core::ExprKind::kConstant: {
      const auto* constantExpr =
          expr->asUnchecked<velox::core::ConstantTypedExpr>();
      // Non-NULL ConstantTypedExpr of ROW type maps to SpecialFormExpression of
      // type ROW_CONSTRUCTOR in Presto.
      if (expr->type()->isRow() && !constantExpr->isNull()) {
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
      // Preserve nullOnFailure (isTryCast) so that TryCast round-trips
      // correctly as "presto.default.try_cast" rather than "cast".
      const auto* castExpr = expr->asUnchecked<velox::core::CastTypedExpr>();
      auto call = std::make_shared<velox::core::CallTypedExpr>(
          expr->type(),
          castExpr->inputs(),
          castExpr->isTryCast() ? velox::expression::kTryCast
                                : velox::expression::kCast);
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
    case velox::core::ExprKind::kLambda: {
      const auto* lambdaExpr =
          expr->asUnchecked<velox::core::LambdaTypedExpr>();
      return getLambdaExpression(lambdaExpr);
    }
    // Presto does not have a RowExpression type for kConcat and kInput Velox
    // expressions. Presto to Velox expression conversion should not generate
    // Velox expressions of these types.
    case velox::core::ExprKind::kConcat:
      [[fallthrough]];
    case velox::core::ExprKind::kInput:
      [[fallthrough]];
    default:
      VELOX_FAIL(
          "Unable to convert Velox expression: {} of kind: {} to Presto RowExpression.",
          expr->toString(),
          velox::core::ExprKindName::toName(expr->kind()));
  }
}

} // namespace facebook::presto::expression
