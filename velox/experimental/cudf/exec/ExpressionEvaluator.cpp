/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
#include "velox/experimental/cudf/exec/ExpressionEvaluator.h"
#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/expression/ConstantExpr.h"
#include "velox/expression/FieldReference.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/VectorTypeUtils.h"

#include <cudf/column/column_factories.hpp>
#include <cudf/datetime.hpp>
#include <cudf/lists/count_elements.hpp>
#include <cudf/strings/attributes.hpp>
#include <cudf/strings/case.hpp>
#include <cudf/strings/contains.hpp>
#include <cudf/strings/slice.hpp>
#include <cudf/strings/split/split.hpp>
#include <cudf/table/table.hpp>
#include <cudf/transform.hpp>

#include <limits>
#include <type_traits>

namespace facebook::velox::cudf_velox {
namespace {
template <TypeKind kind>
cudf::ast::literal makeScalarAndLiteral(
    const TypePtr& type,
    const variant& var,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars) {
  using T = typename facebook::velox::KindToFlatVector<kind>::WrapperType;
  auto stream = cudf::get_default_stream();
  auto mr = cudf::get_current_device_resource_ref();

  if constexpr (cudf::is_fixed_width<T>()) {
    T value = var.value<T>();
    if (type->isShortDecimal()) {
      VELOX_FAIL("Short decimal not supported");
      /* TODO: enable after rewriting using binary ops
      using CudfDecimalType = cudf::numeric::decimal64;
      using cudfScalarType = cudf::fixed_point_scalar<CudfDecimalType>;
      auto scalar = std::make_unique<cudfScalarType>(value,
                    type->scale(),
                     true,
                     stream,
                     mr);
      scalars.emplace_back(std::move(scalar));
      return cudf::ast::literal{
          *static_cast<cudfScalarType*>(scalars.back().get())};
      */
    } else if (type->isLongDecimal()) {
      VELOX_FAIL("Long decimal not supported");
      /* TODO: enable after rewriting using binary ops
      using CudfDecimalType = cudf::numeric::decimal128;
      using cudfScalarType = cudf::fixed_point_scalar<CudfDecimalType>;
      auto scalar = std::make_unique<cudfScalarType>(value,
                    type->scale(),
                     true,
                     stream,
                     mr);
      scalars.emplace_back(std::move(scalar));
      return cudf::ast::literal{
          *static_cast<cudfScalarType*>(scalars.back().get())};
      */
    } else if (type->isIntervalYearMonth()) {
      // no support for interval year month in cudf
      VELOX_FAIL("Interval year month not supported");
    } else if (type->isIntervalDayTime()) {
      using CudfDurationType = cudf::duration_ms;
      if constexpr (std::is_same_v<T, CudfDurationType::rep>) {
        using CudfScalarType = cudf::duration_scalar<CudfDurationType>;
        auto scalar = std::make_unique<CudfScalarType>(value, true, stream, mr);
        scalars.emplace_back(std::move(scalar));
        return cudf::ast::literal{
            *static_cast<CudfScalarType*>(scalars.back().get())};
      }
    } else if (type->isDate()) {
      using CudfDateType = cudf::timestamp_D;
      if constexpr (std::is_same_v<T, CudfDateType::rep>) {
        using CudfScalarType = cudf::timestamp_scalar<CudfDateType>;
        auto scalar = std::make_unique<CudfScalarType>(value, true, stream, mr);
        scalars.emplace_back(std::move(scalar));
        return cudf::ast::literal{
            *static_cast<CudfScalarType*>(scalars.back().get())};
      }
    } else {
      // Create a numeric scalar of type T, store it in the scalars vector,
      // and use its reference in the literal expression.
      using CudfScalarType = cudf::numeric_scalar<T>;
      scalars.emplace_back(
          std::make_unique<CudfScalarType>(value, true, stream, mr));
      return cudf::ast::literal{
          *static_cast<CudfScalarType*>(scalars.back().get())};
    }
    VELOX_FAIL("Unsupported base type for literal");
  } else if (kind == TypeKind::VARCHAR) {
    auto stringValue = var.value<StringView>();
    scalars.emplace_back(
        std::make_unique<cudf::string_scalar>(stringValue, true, stream, mr));
    return cudf::ast::literal{
        *static_cast<cudf::string_scalar*>(scalars.back().get())};
  } else {
    // TODO for non-numeric types too.
    VELOX_NYI(
        "Non-numeric types not yet implemented for kind " +
        mapTypeKindToName(kind));
  }
}

template <TypeKind kind>
variant getVariant(const VectorPtr& vector, size_t atIndex = 0) {
  using T = typename facebook::velox::KindToFlatVector<kind>::WrapperType;
  if constexpr (!std::is_same_v<T, ComplexType>) {
    return vector->as<SimpleVector<T>>()->valueAt(atIndex);
  } else {
    return Variant();
  }
}

cudf::ast::literal createLiteral(
    const VectorPtr& vector,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    size_t atIndex = 0) {
  const auto kind = vector->typeKind();
  const auto& type = vector->type();
  variant value =
      VELOX_DYNAMIC_TYPE_DISPATCH(getVariant, kind, vector, atIndex);
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      makeScalarAndLiteral, kind, type, value, scalars);
}

// Helper function to extract literals from array elements based on type
void extractArrayLiterals(
    const ArrayVector* arrayVector,
    std::vector<cudf::ast::literal>& literals,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    vector_size_t offset,
    vector_size_t size) {
  auto elements = arrayVector->elements();

  for (auto i = offset; i < offset + size; ++i) {
    if (elements->isNullAt(i)) {
      // Skip null values for IN expressions
      continue;
    } else {
      literals.emplace_back(createLiteral(elements, scalars, i));
    }
  }
}

// Function to create literals from an array vector
std::vector<cudf::ast::literal> createLiteralsFromArray(
    const VectorPtr& vector,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars) {
  std::vector<cudf::ast::literal> literals;

  // Check if it's a constant vector containing an array
  if (vector->isConstantEncoding()) {
    auto constantVector = vector->asUnchecked<ConstantVector<ComplexType>>();
    if (constantVector->isNullAt(0)) {
      // Return empty vector for null array
      return literals;
    }

    auto valueVector = constantVector->valueVector();
    if (valueVector->encoding() == VectorEncoding::Simple::ARRAY) {
      auto arrayVector = valueVector->as<ArrayVector>();
      auto index = constantVector->index();
      auto size = arrayVector->sizeAt(index);
      if (size == 0) {
        // Return empty vector for empty array
        return literals;
      }

      auto offset = arrayVector->offsetAt(index);
      auto elements = arrayVector->elements();

      // Handle different element types
      if (elements->isScalar()) {
        literals.reserve(size);
        extractArrayLiterals(arrayVector, literals, scalars, offset, size);
      } else if (elements->typeKind() == TypeKind::ARRAY) {
        // Nested arrays not supported in IN expressions
        VELOX_FAIL("Nested arrays not supported in IN expressions");
      } else {
        VELOX_FAIL(
            "Unsupported element type in array: {}",
            elements->type()->toString());
      }
    } else {
      VELOX_FAIL("Expected ARRAY encoding");
    }
  } else {
    VELOX_FAIL("Expected constant vector for IN list");
  }

  return literals;
}

std::string stripPrefix(const std::string& input, const std::string& prefix) {
  if (input.size() >= prefix.size() &&
      input.compare(0, prefix.size(), prefix) == 0) {
    return input.substr(prefix.size());
  }
  return input;
}
} // namespace

using Op = cudf::ast::ast_operator;
const std::unordered_map<std::string, Op> prestoBinaryOps = {
    {"plus", Op::ADD},
    {"minus", Op::SUB},
    {"multiply", Op::MUL},
    {"divide", Op::DIV},
    {"eq", Op::EQUAL},
    {"neq", Op::NOT_EQUAL},
    {"lt", Op::LESS},
    {"gt", Op::GREATER},
    {"lte", Op::LESS_EQUAL},
    {"gte", Op::GREATER_EQUAL},
    {"and", Op::NULL_LOGICAL_AND},
    {"or", Op::NULL_LOGICAL_OR}};

const std::unordered_map<std::string, Op> sparkBinaryOps = {
    {"add", Op::ADD},
    {"subtract", Op::SUB},
    {"multiply", Op::MUL},
    {"divide", Op::DIV},
    {"equalto", Op::EQUAL},
    {"lessthan", Op::LESS},
    {"greaterthan", Op::GREATER},
    {"lessthanorequal", Op::LESS_EQUAL},
    {"greaterthanorequal", Op::GREATER_EQUAL},
    {"and", Op::NULL_LOGICAL_AND},
    {"or", Op::NULL_LOGICAL_OR},
    {"mod", Op::MOD},
};

const std::unordered_map<std::string, Op> binaryOps = [] {
  std::unordered_map<std::string, Op> merged(
      sparkBinaryOps.begin(), sparkBinaryOps.end());
  merged.insert(prestoBinaryOps.begin(), prestoBinaryOps.end());
  return merged;
}();

const std::map<std::string, Op> unaryOps = {{"not", Op::NOT}};

const std::unordered_set<std::string> supportedOps = {
    "literal",
    "between",
    "in",
    "cast",
    "switch",
    "year",
    "length",
    "substr",
    "like",
    "cardinality",
    "split",
    "lower"};

namespace detail {

bool canBeEvaluated(const std::shared_ptr<velox::exec::Expr>& expr) {
  const auto name =
      stripPrefix(expr->name(), CudfOptions::getInstance().prefix());
  if (supportedOps.count(name) || binaryOps.count(name) ||
      unaryOps.count(name)) {
    return std::all_of(
        expr->inputs().begin(), expr->inputs().end(), canBeEvaluated);
  }
  return std::dynamic_pointer_cast<velox::exec::FieldReference>(expr) !=
      nullptr;
}

} // namespace detail

struct AstContext {
  cudf::ast::tree& tree;
  std::vector<std::unique_ptr<cudf::scalar>>& scalars;
  const std::vector<RowTypePtr> inputRowSchema;
  const std::vector<std::reference_wrapper<std::vector<PrecomputeInstruction>>>
      precomputeInstructions;
  const std::shared_ptr<velox::exec::Expr>
      rootExpr; // Track the root expression

  cudf::ast::expression const& pushExprToTree(
      const std::shared_ptr<velox::exec::Expr>& expr);
  cudf::ast::expression const& addPrecomputeInstruction(
      std::string const& name,
      std::string const& instruction,
      std::string const& fieldName = {});
  cudf::ast::expression const& multipleInputsToPairWise(
      const std::shared_ptr<velox::exec::Expr>& expr);
  static bool canBeEvaluated(const std::shared_ptr<velox::exec::Expr>& expr);
};

// Create tree from Expr
// and collect precompute instructions for non-ast operations
cudf::ast::expression const& createAstTree(
    const std::shared_ptr<velox::exec::Expr>& expr,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const RowTypePtr& inputRowSchema,
    std::vector<PrecomputeInstruction>& precomputeInstructions) {
  AstContext context{
      tree, scalars, {inputRowSchema}, {precomputeInstructions}, expr};
  return context.pushExprToTree(expr);
}

cudf::ast::expression const& createAstTree(
    const std::shared_ptr<velox::exec::Expr>& expr,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const RowTypePtr& leftRowSchema,
    const RowTypePtr& rightRowSchema,
    std::vector<PrecomputeInstruction>& leftPrecomputeInstructions,
    std::vector<PrecomputeInstruction>& rightPrecomputeInstructions) {
  AstContext context{
      tree,
      scalars,
      {leftRowSchema, rightRowSchema},
      {leftPrecomputeInstructions, rightPrecomputeInstructions},
      expr};
  return context.pushExprToTree(expr);
}

// get nested column indices
std::vector<int> getNestedColumnIndices(
    const TypePtr& rowType,
    const std::string& fieldName) {
  std::vector<int> indices;
  auto rowTypePtr = asRowType(rowType);
  if (rowTypePtr->containsChild(fieldName)) {
    auto columnIndex = rowTypePtr->getChildIdx(fieldName);
    indices.push_back(columnIndex);
  }
  return indices;
}

cudf::ast::expression const& AstContext::addPrecomputeInstruction(
    std::string const& name,
    std::string const& instruction,
    std::string const& fieldName) {
  for (size_t sideIdx = 0; sideIdx < inputRowSchema.size(); ++sideIdx) {
    if (inputRowSchema[sideIdx].get()->containsChild(name)) {
      auto columnIndex = inputRowSchema[sideIdx].get()->getChildIdx(name);
      auto newColumnIndex = inputRowSchema[sideIdx].get()->size() +
          precomputeInstructions[sideIdx].get().size();
      if (fieldName.empty()) {
        // This custom op should be added to input columns.
        precomputeInstructions[sideIdx].get().emplace_back(
            columnIndex, instruction, newColumnIndex);
      } else {
        auto nestedIndices = getNestedColumnIndices(
            inputRowSchema[sideIdx].get()->childAt(columnIndex), fieldName);
        if (nestedIndices.empty())
          continue;
        precomputeInstructions[sideIdx].get().emplace_back(
            columnIndex, instruction, newColumnIndex, nestedIndices);
      }
      auto side = static_cast<cudf::ast::table_reference>(sideIdx);
      return tree.push(cudf::ast::column_reference(newColumnIndex, side));
    }
  }
  VELOX_FAIL("Field not found, " + name);
}

/// Handles logical AND/OR expressions with multiple inputs by converting them
/// into a chain of binary operations. For example, "a AND b AND c" becomes
/// "(a AND b) AND c".
///
/// @param expr The expression containing multiple inputs for AND/OR operation
/// @return A reference to the resulting AST expression
cudf::ast::expression const& AstContext::multipleInputsToPairWise(
    const std::shared_ptr<velox::exec::Expr>& expr) {
  using Operation = cudf::ast::operation;

  const auto name =
      stripPrefix(expr->name(), CudfOptions::getInstance().prefix());
  auto len = expr->inputs().size();
  // Create a simple chain of operations
  auto result = &pushExprToTree(expr->inputs()[0]);

  // Chain the rest of the inputs sequentially
  for (size_t i = 1; i < len; i++) {
    auto const& nextInput = pushExprToTree(expr->inputs()[i]);
    result = &tree.push(Operation{binaryOps.at(name), *result, nextInput});
  }
  return *result;
}

/// Pushes an expression into the AST tree and returns a reference to the
/// resulting expression.
///
/// @param expr The expression to push into the AST tree
/// @return A reference to the resulting AST expression
cudf::ast::expression const& AstContext::pushExprToTree(
    const std::shared_ptr<velox::exec::Expr>& expr) {
  using Op = cudf::ast::ast_operator;
  using Operation = cudf::ast::operation;
  using velox::exec::ConstantExpr;
  using velox::exec::FieldReference;

  const auto name =
      stripPrefix(expr->name(), CudfOptions::getInstance().prefix());
  auto len = expr->inputs().size();
  auto& type = expr->type();

  if (name == "literal") {
    auto c = dynamic_cast<ConstantExpr*>(expr.get());
    VELOX_CHECK_NOT_NULL(c, "literal expression should be ConstantExpr");
    auto value = c->value();
    VELOX_CHECK(value->isConstantEncoding());

    // Special case: VARCHAR literals cannot be handled by cudf::compute_column
    // as the final output due to variable-width output limitation.
    // However, if this is part of a larger expression tree (e.g., string
    // comparison), then cudf can handle it fine since the final output won't be
    // VARCHAR. We only need special handling when this literal will be the
    // final output.
    if (expr->type()->kind() == TypeKind::VARCHAR && expr == rootExpr) {
      // convert to cudf scalar and store it
      createLiteral(value, scalars);
      // The scalar index is scalars.size() - 1 since we just added it
      std::string fillExpr = "fill " + std::to_string(scalars.size() - 1);
      // For literals, we use the first column just to get the size, but create
      // a new column The new column will be appended after the original input
      // columns
      return addPrecomputeInstruction(inputRowSchema[0]->nameOf(0), fillExpr);
    }

    return tree.push(createLiteral(value, scalars));
  } else if (binaryOps.find(name) != binaryOps.end()) {
    if (len > 2 and (name == "and" or name == "or")) {
      return multipleInputsToPairWise(expr);
    }
    VELOX_CHECK_EQ(len, 2);
    auto const& op1 = pushExprToTree(expr->inputs()[0]);
    auto const& op2 = pushExprToTree(expr->inputs()[1]);
    return tree.push(Operation{binaryOps.at(name), op1, op2});
  } else if (unaryOps.find(name) != unaryOps.end()) {
    VELOX_CHECK_EQ(len, 1);
    auto const& op1 = pushExprToTree(expr->inputs()[0]);
    return tree.push(Operation{unaryOps.at(name), op1});
  } else if (name == "between") {
    VELOX_CHECK_EQ(len, 3);
    auto const& value = pushExprToTree(expr->inputs()[0]);
    auto const& lower = pushExprToTree(expr->inputs()[1]);
    auto const& upper = pushExprToTree(expr->inputs()[2]);
    // construct between(op2, op3) using >= and <=
    auto const& geLower = tree.push(Operation{Op::GREATER_EQUAL, value, lower});
    auto const& leUpper = tree.push(Operation{Op::LESS_EQUAL, value, upper});
    return tree.push(Operation{Op::NULL_LOGICAL_AND, geLower, leUpper});
  } else if (name == "in") {
    // number of inputs is variable. >=2
    VELOX_CHECK_EQ(len, 2);
    // actually len is 2, second input is ARRAY
    auto const& op1 = pushExprToTree(expr->inputs()[0]);
    auto c = dynamic_cast<ConstantExpr*>(expr->inputs()[1].get());
    VELOX_CHECK_NOT_NULL(c, "literal expression should be ConstantExpr");
    auto value = c->value();
    VELOX_CHECK_NOT_NULL(value, "ConstantExpr value is null");

    // Use the new createLiteralsFromArray function to get literals
    auto literals = createLiteralsFromArray(value, scalars);

    // Create equality expressions for each literal and OR them together
    std::vector<const cudf::ast::expression*> exprVec;
    for (auto& literal : literals) {
      auto const& opi = tree.push(std::move(literal));
      auto const& logicalNode = tree.push(Operation{Op::EQUAL, op1, opi});
      exprVec.push_back(&logicalNode);
    }

    // Handle empty IN list case
    if (exprVec.empty()) {
      // FAIL
      VELOX_FAIL("Empty IN list");
      // Return FALSE for empty IN list
      // auto falseValue = std::make_shared<ConstantVector<bool>>(
      //     value->pool(), 1, false, TypeKind::BOOLEAN, false);
      // return tree.push(createLiteral(falseValue, scalars));
    }

    // OR all logical nodes
    auto* result = exprVec[0];
    for (size_t i = 1; i < exprVec.size(); i++) {
      auto const& treeNode =
          tree.push(Operation{Op::NULL_LOGICAL_OR, *result, *exprVec[i]});
      result = &treeNode;
    }
    return *result;
  } else if (name == "cast" || name == "try_cast") {
    VELOX_CHECK_EQ(len, 1);
    auto const& op1 = pushExprToTree(expr->inputs()[0]);
    if (expr->type()->kind() == TypeKind::INTEGER) {
      // No int32 cast in cudf ast
      return tree.push(Operation{Op::CAST_TO_INT64, op1});
    } else if (expr->type()->kind() == TypeKind::BIGINT) {
      return tree.push(Operation{Op::CAST_TO_INT64, op1});
    } else if (expr->type()->kind() == TypeKind::DOUBLE) {
      return tree.push(Operation{Op::CAST_TO_FLOAT64, op1});
    } else {
      VELOX_FAIL("Unsupported type for cast operation");
    }
  } else if (name == "switch") {
    VELOX_CHECK_EQ(len, 3);
    // check if input[1], input[2] are literals 1 and 0.
    // then simplify as typecast bool to int
    auto c1 = dynamic_cast<ConstantExpr*>(expr->inputs()[1].get());
    auto c2 = dynamic_cast<ConstantExpr*>(expr->inputs()[2].get());
    if ((c1 and c1->toString() == "1:BIGINT" and c2 and
         c2->toString() == "0:BIGINT") ||
        (c1 and c1->toString() == "1:INTEGER" and c2 and
         c2->toString() == "0:INTEGER")) {
      auto const& op1 = pushExprToTree(expr->inputs()[0]);
      return tree.push(Operation{Op::CAST_TO_INT64, op1});
    } else if (c2 and c2->toString() == "0:DOUBLE") {
      auto const& op1 = pushExprToTree(expr->inputs()[0]);
      auto const& op1d = tree.push(Operation{Op::CAST_TO_FLOAT64, op1});
      auto const& op2 = pushExprToTree(expr->inputs()[1]);
      return tree.push(Operation{Op::MUL, op1d, op2});
    } else if (
        c1 and c1->toString() == "1:INTEGER" and c2 and
        c2->toString() == "0:INTEGER") {
      return pushExprToTree(expr->inputs()[0]);
    } else {
      VELOX_NYI("Unsupported switch complex operation " + expr->toString());
    }
  } else if (name == "year") {
    VELOX_CHECK_EQ(len, 1);

    auto fieldExpr =
        std::dynamic_pointer_cast<FieldReference>(expr->inputs()[0]);
    VELOX_CHECK_NOT_NULL(fieldExpr, "Expression is not a field");

    auto const& colRef = addPrecomputeInstruction(fieldExpr->name(), "year");
    if (type->kind() == TypeKind::BIGINT) {
      // Presto returns int64.
      return tree.push(Operation{Op::CAST_TO_INT64, colRef});
    } else {
      // Cudf returns smallint while spark returns int, cast the output column
      // in execution.
      return colRef;
    }

  } else if (name == "length") {
    VELOX_CHECK_EQ(len, 1);

    auto fieldExpr =
        std::dynamic_pointer_cast<FieldReference>(expr->inputs()[0]);
    VELOX_CHECK_NOT_NULL(fieldExpr, "Expression is not a field");

    auto const& colRef = addPrecomputeInstruction(fieldExpr->name(), "length");

    return tree.push(Operation{Op::CAST_TO_INT64, colRef});
  } else if (name == "lower") {
    VELOX_CHECK_EQ(len, 1);

    auto fieldExpr =
        std::dynamic_pointer_cast<FieldReference>(expr->inputs()[0]);
    VELOX_CHECK_NOT_NULL(fieldExpr, "Expression is not a field");
    return addPrecomputeInstruction(fieldExpr->name(), "lower");
  } else if (name == "substr") {
    // Extract the start and length parameters from the substr function call
    // and create a precomputed column with the substring operation.
    // This will be handled during AST evaluation with special column reference.
    VELOX_CHECK_GE(len, 2);
    VELOX_CHECK_LE(len, 3);
    auto fieldExpr =
        std::dynamic_pointer_cast<FieldReference>(expr->inputs()[0]);
    VELOX_CHECK_NOT_NULL(fieldExpr, "Expression is not a field");

    auto c1 = dynamic_cast<ConstantExpr*>(expr->inputs()[1].get());
    std::string substrExpr =
        "substr " + std::to_string(len - 1) + " " + c1->value()->toString(0);

    if (len > 2) {
      auto c2 = dynamic_cast<ConstantExpr*>(expr->inputs()[2].get());
      substrExpr += " " + c2->value()->toString(0);
    }
    return addPrecomputeInstruction(fieldExpr->name(), substrExpr);
  } else if (name == "like") {
    VELOX_CHECK_EQ(len, 2);

    auto fieldExpr =
        std::dynamic_pointer_cast<FieldReference>(expr->inputs()[0]);
    VELOX_CHECK_NOT_NULL(fieldExpr, "Expression is not a field");
    auto literalExpr =
        std::dynamic_pointer_cast<ConstantExpr>(expr->inputs()[1]);
    VELOX_CHECK_NOT_NULL(literalExpr, "Expression is not a literal");

    createLiteral(literalExpr->value(), scalars);

    std::string likeExpr = "like " + std::to_string(scalars.size() - 1);

    return addPrecomputeInstruction(fieldExpr->name(), likeExpr);
  } else if (name == "cardinality") {
    VELOX_CHECK_EQ(len, 1);

    auto fieldExpr =
        std::dynamic_pointer_cast<FieldReference>(expr->inputs()[0]);
    VELOX_CHECK_NOT_NULL(fieldExpr, "Expression is not a field");

    auto const& colRef =
        addPrecomputeInstruction(fieldExpr->name(), "cardinality");

    return tree.push(Operation{Op::CAST_TO_INT64, colRef});
  } else if (name == "split") {
    VELOX_CHECK_EQ(len, 3);
    auto fieldExpr =
        std::dynamic_pointer_cast<FieldReference>(expr->inputs()[0]);
    VELOX_CHECK_NOT_NULL(fieldExpr, "Expression is not a field");

    auto splitLiteralExpr =
        std::dynamic_pointer_cast<ConstantExpr>(expr->inputs()[1]);
    VELOX_CHECK_NOT_NULL(splitLiteralExpr, "Expression is not a literal");

    createLiteral(splitLiteralExpr->value(), scalars);

    auto maxsplitLiteral =
        std::dynamic_pointer_cast<ConstantExpr>(expr->inputs()[2]);
    auto splitExpr = fmt::format(
        "split {} {}",
        scalars.size() - 1,
        maxsplitLiteral->value()->toString(0));
    return addPrecomputeInstruction(fieldExpr->name(), splitExpr);
  } else if (auto fieldExpr = std::dynamic_pointer_cast<FieldReference>(expr)) {
    // Refer to the appropriate side
    const auto fieldName =
        fieldExpr->inputs().empty() ? name : fieldExpr->inputs()[0]->name();
    for (size_t sideIdx = 0; sideIdx < inputRowSchema.size(); ++sideIdx) {
      auto& schema = inputRowSchema[sideIdx];
      if (schema.get()->containsChild(fieldName)) {
        auto columnIndex = schema.get()->getChildIdx(fieldName);
        // This column may be complex data type like ROW, we need to get the
        // name from row. Push fieldName.name to the tree.
        auto side = static_cast<cudf::ast::table_reference>(sideIdx);
        if (fieldExpr->field() == fieldName) {
          return tree.push(cudf::ast::column_reference(columnIndex, side));
        } else {
          return addPrecomputeInstruction(
              fieldName, "nested_column", fieldExpr->field());
        }
      }
    }
    VELOX_FAIL("Field not found, " + name);
  } else {
    VELOX_FAIL("Unsupported expression: " + name);
  }
}

void addPrecomputedColumns(
    std::vector<std::unique_ptr<cudf::column>>& input_table_columns,
    const std::vector<PrecomputeInstruction>& precompute_instructions,
    const std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    rmm::cuda_stream_view stream) {
  for (const auto& instruction : precompute_instructions) {
    auto
        [dependent_column_index,
         ins_name,
         new_column_index,
         nested_dependent_column_indices] = instruction;
    if (ins_name == "year") {
      auto newColumn = cudf::datetime::extract_datetime_component(
          input_table_columns[dependent_column_index]->view(),
          cudf::datetime::datetime_component::YEAR,
          stream,
          cudf::get_current_device_resource_ref());
      input_table_columns.emplace_back(std::move(newColumn));
    } else if (ins_name == "length") {
      auto newColumn = cudf::strings::count_characters(
          input_table_columns[dependent_column_index]->view(),
          stream,
          cudf::get_current_device_resource_ref());
      input_table_columns.emplace_back(std::move(newColumn));
    } else if (ins_name == "lower") {
      auto newColumn = cudf::strings::to_lower(
          input_table_columns[dependent_column_index]->view(),
          stream,
          cudf::get_current_device_resource_ref());
      input_table_columns.emplace_back(std::move(newColumn));
    } else if (ins_name.rfind("substr", 0) == 0) {
      std::istringstream iss(ins_name.substr(6));
      int numberOfParameters, beginValue, length;
      iss >> numberOfParameters >> beginValue >> length;
      if (beginValue >= 1) {
        // cuDF indexing starts at 0.
        // Presto indexing starts at 1.
        // Positive indices need to substract 1.
        beginValue -= 1;
      }
      auto beginScalar = cudf::numeric_scalar<cudf::size_type>(
          beginValue, true, stream, cudf::get_current_device_resource_ref());
      // cuDF uses indices [begin, end).
      // Presto uses length as the length of the substring.
      // We compute the end as beginValue + length.
      auto endScalar = cudf::numeric_scalar<cudf::size_type>(
          beginValue + length,
          numberOfParameters != 1,
          stream,
          cudf::get_current_device_resource_ref());
      auto stepScalar = cudf::numeric_scalar<cudf::size_type>(
          1, true, stream, cudf::get_current_device_resource_ref());
      auto newColumn = cudf::strings::slice_strings(
          input_table_columns[dependent_column_index]->view(),
          beginScalar,
          endScalar,
          stepScalar,
          stream,
          cudf::get_current_device_resource_ref());
      input_table_columns.emplace_back(std::move(newColumn));
    } else if (ins_name.rfind("like", 0) == 0) {
      auto scalarIndex = std::stoi(ins_name.substr(4));
      auto newColumn = cudf::strings::like(
          input_table_columns[dependent_column_index]->view(),
          *static_cast<cudf::string_scalar*>(scalars[scalarIndex].get()),
          cudf::string_scalar(
              "", true, stream, cudf::get_current_device_resource_ref()),
          stream,
          cudf::get_current_device_resource_ref());
      input_table_columns.emplace_back(std::move(newColumn));
    } else if (ins_name.rfind("fill", 0) == 0) {
      auto scalarIndex =
          std::stoi(ins_name.substr(5)); // "fill " is 5 characters
      auto newColumn = cudf::make_column_from_scalar(
          *static_cast<cudf::string_scalar*>(scalars[scalarIndex].get()),
          input_table_columns[dependent_column_index]->size(),
          stream,
          cudf::get_current_device_resource_ref());
      input_table_columns.emplace_back(std::move(newColumn));
    } else if (ins_name == "nested_column") {
      auto newColumn = std::make_unique<cudf::column>(
          input_table_columns[dependent_column_index]->view().child(
              nested_dependent_column_indices[0]),
          stream,
          cudf::get_current_device_resource_ref());
      input_table_columns.emplace_back(std::move(newColumn));
    } else if (ins_name == "cardinality") {
      auto newColumn = cudf::lists::count_elements(
          input_table_columns[dependent_column_index]->view(),
          stream,
          cudf::get_current_device_resource_ref());
      input_table_columns.emplace_back(std::move(newColumn));
    } else if (ins_name.rfind("split", 0) == 0) {
      VELOX_CHECK_GT(ins_name.length(), 5);
      std::istringstream iss(ins_name.substr(5));
      int scalarIndex, maxSplitCount;
      iss >> scalarIndex >> maxSplitCount;
      VELOX_CHECK(!iss.fail(), "Unable to parse scalarIndex and maxSplitCount");
      // Presto specifies maxSplitCount as the maximum size of the returned
      // array while cuDF understands the parameter as how many splits can it
      // perform.
      maxSplitCount -= 1;
      auto newColumn = cudf::strings::split_record(
          input_table_columns[dependent_column_index]->view(),
          *static_cast<cudf::string_scalar*>(scalars[scalarIndex].get()),
          maxSplitCount,
          stream,
          cudf::get_current_device_resource_ref());
      input_table_columns.emplace_back(std::move(newColumn));
    } else {
      VELOX_FAIL("Unsupported precompute operation " + ins_name);
    }
  }
}

ExpressionEvaluator::ExpressionEvaluator(
    const std::vector<std::shared_ptr<velox::exec::Expr>>& exprs,
    const RowTypePtr& inputRowSchema) {
  exprAst_.reserve(exprs.size());
  for (const auto& expr : exprs) {
    cudf::ast::tree tree;
    createAstTree(
        expr, tree, scalars_, inputRowSchema, precomputeInstructions_);
    exprAst_.emplace_back(std::move(tree));
  }
}

void ExpressionEvaluator::close() {
  exprAst_.clear();
  scalars_.clear();
  precomputeInstructions_.clear();
}

std::vector<std::unique_ptr<cudf::column>> ExpressionEvaluator::compute(
    std::vector<std::unique_ptr<cudf::column>>& inputTableColumns,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  auto numColumns = inputTableColumns.size();
  addPrecomputedColumns(
      inputTableColumns, precomputeInstructions_, scalars_, stream);
  auto astInputTable =
      std::make_unique<cudf::table>(std::move(inputTableColumns));
  auto astInputTableView = astInputTable->view();
  std::vector<std::unique_ptr<cudf::column>> columns;
  for (auto& tree : exprAst_) {
    if (auto colRefPtr =
            dynamic_cast<cudf::ast::column_reference const*>(&tree.back())) {
      auto col = std::make_unique<cudf::column>(
          astInputTableView.column(colRefPtr->get_column_index()), stream, mr);
      columns.emplace_back(std::move(col));
    } else {
      auto col =
          cudf::compute_column(astInputTableView, tree.back(), stream, mr);
      columns.emplace_back(std::move(col));
    }
  }
  inputTableColumns = astInputTable->release();
  inputTableColumns.resize(numColumns);
  return columns;
}

bool ExpressionEvaluator::canBeEvaluated(
    const std::vector<std::shared_ptr<velox::exec::Expr>>& exprs) {
  return std::all_of(exprs.begin(), exprs.end(), detail::canBeEvaluated);
}

namespace {

template <
    typename RangeT,
    typename ScalarT,
    typename = std::enable_if_t<
        std::is_base_of_v<facebook::velox::common::AbstractRange, RangeT>>>
const cudf::ast::expression& createRangeExpr(
    const facebook::velox::common::Filter& filter,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const cudf::ast::expression& columnRef,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  using Op = cudf::ast::ast_operator;
  using Operation = cudf::ast::operation;

  auto* range = dynamic_cast<const RangeT*>(&filter);
  VELOX_CHECK_NOT_NULL(range, "Filter is not the expected range type");

  const bool lowerUnbounded = range->lowerUnbounded();
  const bool upperUnbounded = range->upperUnbounded();

  const cudf::ast::expression* lowerExpr = nullptr;
  const cudf::ast::expression* upperExpr = nullptr;

  auto addLiteral = [&](auto value) -> const cudf::ast::expression& {
    scalars.emplace_back(std::make_unique<ScalarT>(value, true, stream, mr));
    return tree.push(
        cudf::ast::literal{*static_cast<ScalarT*>(scalars.back().get())});
  };

  // If RangeT is BytesValues and it's a single value, return a simple equality
  // expression. This is an early return for the single-value IN-list filter on
  // bytes.
  if constexpr (std::is_same_v<RangeT, facebook::velox::common::BytesRange>) {
    if (range->isSingleValue()) {
      // Only one value in the IN-list, so just compare for equality.
      auto singleValue = range->lower();
      const auto& literal = addLiteral(singleValue);
      return tree.push(Operation{Op::EQUAL, columnRef, literal});
    }
  }

  if (!lowerUnbounded) {
    auto lowerValue = range->lower();
    const auto& lowerLiteral = addLiteral(lowerValue);

    auto lowerOp = range->lowerExclusive() ? Op::GREATER : Op::GREATER_EQUAL;
    lowerExpr = &tree.push(Operation{lowerOp, columnRef, lowerLiteral});
  }

  if (!upperUnbounded) {
    auto upperValue = range->upper();
    const auto& upperLiteral = addLiteral(upperValue);

    auto upperOp = range->upperExclusive() ? Op::LESS : Op::LESS_EQUAL;
    upperExpr = &tree.push(Operation{upperOp, columnRef, upperLiteral});
  }

  if (lowerExpr && upperExpr) {
    return tree.push(Operation{Op::NULL_LOGICAL_AND, *lowerExpr, *upperExpr});
  } else if (lowerExpr) {
    return *lowerExpr;
  } else if (upperExpr) {
    return *upperExpr;
  }

  // Both bounds unbounded => Pass-through filter (everything).
  return tree.push(Operation{Op::EQUAL, columnRef, columnRef});
}

template <TypeKind Kind>
std::reference_wrapper<const cudf::ast::expression> buildBigintRangeExpr(
    const common::Filter& filter,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const cudf::ast::expression& columnRef,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr,
    const TypePtr& columnTypePtr) {
  using NativeT = typename TypeTraits<Kind>::NativeType;

  if constexpr (std::is_integral_v<NativeT>) {
    using Op = cudf::ast::ast_operator;
    using Operation = cudf::ast::operation;

    auto* bigintRange = static_cast<const common::BigintRange*>(&filter);

    const auto lower = bigintRange->lower();
    const auto upper = bigintRange->upper();

    const bool skipLowerBound =
        lower <= static_cast<int64_t>(std::numeric_limits<NativeT>::min());
    const bool skipUpperBound =
        upper >= static_cast<int64_t>(std::numeric_limits<NativeT>::max());

    auto addLiteral = [&](int64_t value) -> const cudf::ast::expression& {
      variant veloxVariant = static_cast<NativeT>(value);
      const auto& literal =
          makeScalarAndLiteral<Kind>(columnTypePtr, veloxVariant, scalars);
      return tree.push(literal);
    };

    if (bigintRange->isSingleValue()) {
      // Equal comparison: column = value. This value is the same as the
      // lower/upper bound.
      if (skipLowerBound || skipUpperBound) {
        // If the singular value of this filter lies outside the range of the
        // column's NativeT type then we want to be always false
        return tree.push(Operation{Op::NOT_EQUAL, columnRef, columnRef});
      } else {
        auto const& literal = addLiteral(lower);
        return tree.push(Operation{Op::EQUAL, columnRef, literal});
      }
    } else {
      // Range comparison: column >= lower AND column <= upper

      const cudf::ast::expression* lowerExpr = nullptr;
      if (!skipLowerBound) {
        auto const& lowerLiteral = addLiteral(lower);
        lowerExpr =
            &tree.push(Operation{Op::GREATER_EQUAL, columnRef, lowerLiteral});
      }

      const cudf::ast::expression* upperExpr = nullptr;
      if (!skipUpperBound) {
        auto const& upperLiteral = addLiteral(upper);
        upperExpr =
            &tree.push(Operation{Op::LESS_EQUAL, columnRef, upperLiteral});
      }

      if (lowerExpr && upperExpr) {
        auto const& result =
            tree.push(Operation{Op::NULL_LOGICAL_AND, *lowerExpr, *upperExpr});
        return result;
      } else if (lowerExpr) {
        return *lowerExpr;
      } else if (upperExpr) {
        return *upperExpr;
      }

      // If neither lower nor upper bound expressions were created, it means
      // the filter covers the entire range of the type, so it's a no-op
      return tree.push(Operation{Op::EQUAL, columnRef, columnRef});
    }
  } else {
    VELOX_FAIL(
        "Unsupported type for buildBigintRangeExpr: {}",
        mapTypeKindToName(Kind));
  }
}

template <typename T>
auto createFloatingPointRangeExpr(
    const common::Filter& filter,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const cudf::ast::expression& columnRef,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) -> const cudf::ast::expression& {
  return createRangeExpr<
      facebook::velox::common::FloatingPointRange<T>,
      cudf::numeric_scalar<T>>(filter, tree, scalars, columnRef, stream, mr);
};

const cudf::ast::expression& createBytesRangeExpr(
    const common::Filter& filter,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const cudf::ast::expression& columnRef,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  return createRangeExpr<
      facebook::velox::common::BytesRange,
      cudf::string_scalar>(filter, tree, scalars, columnRef, stream, mr);
}

template <typename FilterT, typename ScalarT>
const cudf::ast::expression& buildInListExpr(
    const common::Filter& filter,
    cudf::ast::tree& tree,
    const cudf::ast::expression& columnRef,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    bool isNegated,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  using Op = cudf::ast::ast_operator;
  using Operation = cudf::ast::operation;

  auto* valuesFilter = dynamic_cast<const FilterT*>(&filter);
  VELOX_CHECK_NOT_NULL(valuesFilter, "Filter is not a List filter");
  auto const& values = valuesFilter->values();
  if (values.empty()) {
    VELOX_FAIL("Empty List filter not supported");
  }

  std::vector<const cudf::ast::expression*> exprVec;
  for (const auto& value : values) {
    scalars.emplace_back(std::make_unique<ScalarT>(value, true, stream, mr));
    auto const& literal = tree.push(
        cudf::ast::literal{*static_cast<ScalarT*>(scalars.back().get())});
    auto const& equalExpr = tree.push(
        Operation{isNegated ? Op::NOT_EQUAL : Op::EQUAL, columnRef, literal});
    exprVec.push_back(&equalExpr);
  }

  const cudf::ast::expression* result = exprVec[0];
  for (size_t i = 1; i < exprVec.size(); ++i) {
    if (isNegated) {
      result =
          &tree.push(Operation{Op::NULL_LOGICAL_AND, *result, *exprVec[i]});
    } else {
      result = &tree.push(Operation{Op::NULL_LOGICAL_OR, *result, *exprVec[i]});
    }
  }
  return *result;
}

// Build an IN-list expression for integer columns where the filter values are
// provided as int64_t but the column may be any integral type. Values outside
// the target type's range are ignored. If all values are out of range, this
// returns a constant false expression (col != col).
template <TypeKind Kind>
std::reference_wrapper<const cudf::ast::expression> buildIntegerInListExpr(
    const common::Filter& filter,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const cudf::ast::expression& columnRef,
    rmm::cuda_stream_view /*stream*/,
    rmm::device_async_resource_ref /*mr*/,
    const TypePtr& columnTypePtr) {
  using NativeT = typename TypeTraits<Kind>::NativeType;

  if constexpr (std::is_integral_v<NativeT>) {
    using Op = cudf::ast::ast_operator;
    using Operation = cudf::ast::operation;

    auto* valuesFilter =
        static_cast<const common::BigintValuesUsingBitmask*>(&filter);
    const auto& values = valuesFilter->values();

    std::vector<const cudf::ast::expression*> exprVec;
    exprVec.reserve(values.size());

    for (const int64_t value : values) {
      if (value < static_cast<int64_t>(std::numeric_limits<NativeT>::min()) ||
          value > static_cast<int64_t>(std::numeric_limits<NativeT>::max())) {
        // Skip values that cannot be represented in the column type.
        continue;
      }

      variant veloxVariant = static_cast<NativeT>(value);
      const auto& literal =
          makeScalarAndLiteral<Kind>(columnTypePtr, veloxVariant, scalars);
      auto const& cudfLiteral = tree.push(literal);
      auto const& equalExpr =
          tree.push(Operation{Op::EQUAL, columnRef, cudfLiteral});
      exprVec.push_back(&equalExpr);
    }

    if (exprVec.empty()) {
      // No representable values -> always false
      auto const& alwaysFalse =
          tree.push(Operation{Op::NOT_EQUAL, columnRef, columnRef});
      return std::ref(alwaysFalse);
    }

    const cudf::ast::expression* result = exprVec[0];
    for (size_t i = 1; i < exprVec.size(); ++i) {
      result = &tree.push(Operation{Op::NULL_LOGICAL_OR, *result, *exprVec[i]});
    }
    return std::ref(*result);
  } else {
    VELOX_FAIL(
        "Unsupported type for buildIntegerInListExpr: {}",
        mapTypeKindToName(Kind));
  }
}

} // namespace

// Convert subfield filters to cudf AST
cudf::ast::expression const& createAstFromSubfieldFilter(
    const common::Subfield& subfield,
    const common::Filter& filter,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const RowTypePtr& inputRowSchema) {
  // First, create column reference from subfield
  // For now, only support simple field references
  if (subfield.path().empty() ||
      subfield.path()[0]->kind() != common::kNestedField) {
    VELOX_FAIL(
        "Only simple field references are supported in subfield filters");
  }

  auto nestedField = static_cast<const common::Subfield::NestedField*>(
      subfield.path()[0].get());
  const std::string& fieldName = nestedField->name();

  if (!inputRowSchema->containsChild(fieldName)) {
    VELOX_FAIL("Field '{}' not found in input schema", fieldName);
  }

  auto columnIndex = inputRowSchema->getChildIdx(fieldName);
  auto const& columnRef = tree.push(cudf::ast::column_reference(columnIndex));

  using Op = cudf::ast::ast_operator;
  using Operation = cudf::ast::operation;

  auto stream = cudf::get_default_stream();
  auto mr = cudf::get_current_device_resource_ref();

  switch (filter.kind()) {
    case common::FilterKind::kBigintRange: {
      auto const& columnType = inputRowSchema->childAt(columnIndex);
      auto result = VELOX_DYNAMIC_TYPE_DISPATCH(
          buildBigintRangeExpr,
          columnType->kind(),
          filter,
          tree,
          scalars,
          columnRef,
          stream,
          mr,
          columnType);
      return result.get();
    }

    case common::FilterKind::kBigintValuesUsingBitmask: {
      auto const& columnType = inputRowSchema->childAt(columnIndex);
      // Dispatch by the column's integer kind and cast filter values to it.
      auto result = VELOX_DYNAMIC_TYPE_DISPATCH(
          buildIntegerInListExpr,
          columnType->kind(),
          filter,
          tree,
          scalars,
          columnRef,
          stream,
          mr,
          columnType);
      return result.get();
    }

    case common::FilterKind::kBytesValues: {
      return buildInListExpr<common::BytesValues, cudf::string_scalar>(
          filter, tree, columnRef, scalars, false, stream, mr);
    }

    case common::FilterKind::kNegatedBytesValues: {
      return buildInListExpr<common::NegatedBytesValues, cudf::string_scalar>(
          filter, tree, columnRef, scalars, true, stream, mr);
    }

    case common::FilterKind::kDoubleRange: {
      return createFloatingPointRangeExpr<double>(
          filter, tree, scalars, columnRef, stream, mr);
    }

    case common::FilterKind::kFloatRange: {
      return createFloatingPointRangeExpr<float>(
          filter, tree, scalars, columnRef, stream, mr);
    }

    case common::FilterKind::kBytesRange: {
      return createBytesRangeExpr(filter, tree, scalars, columnRef, stream, mr);
    }

    case common::FilterKind::kBoolValue: {
      auto* boolValue = static_cast<const common::BoolValue*>(&filter);
      auto matchesTrue = boolValue->testBool(true);
      scalars.emplace_back(std::make_unique<cudf::numeric_scalar<bool>>(
          matchesTrue, true, stream, mr));
      auto const& matchesBoolExpr = tree.push(cudf::ast::literal{
          *static_cast<cudf::numeric_scalar<bool>*>(scalars.back().get())});
      return tree.push(Operation{Op::EQUAL, columnRef, matchesBoolExpr});
    }

    case common::FilterKind::kIsNull: {
      return tree.push(Operation{Op::IS_NULL, columnRef});
    }

    case common::FilterKind::kIsNotNull: {
      // For IsNotNull, we can use NOT(IS_NULL)
      auto const& nullCheck = tree.push(Operation{Op::IS_NULL, columnRef});
      return tree.push(Operation{Op::NOT, nullCheck});
    }

    default:
      VELOX_NYI(
          "Filter type {} not yet supported for subfield filter conversion",
          static_cast<int>(filter.kind()));
  }
}

// Create a combined AST from a set of subfield filters by chaining them with
// logical ANDs. The returned expression is owned by the provided 'tree'.
cudf::ast::expression const& createAstFromSubfieldFilters(
    const common::SubfieldFilters& subfieldFilters,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const RowTypePtr& inputRowSchema) {
  using Op = cudf::ast::ast_operator;
  using Operation = cudf::ast::operation;

  std::vector<const cudf::ast::expression*> exprRefs;

  // Build individual filter expressions.
  for (const auto& [subfield, filterPtr] : subfieldFilters) {
    if (!filterPtr) {
      continue;
    }
    auto const& expr = createAstFromSubfieldFilter(
        subfield, *filterPtr, tree, scalars, inputRowSchema);
    exprRefs.push_back(&expr);
  }

  VELOX_CHECK_GT(exprRefs.size(), 0, "No subfield filters provided");

  if (exprRefs.size() == 1) {
    return *exprRefs[0];
  }

  // Combine expressions with NULL_LOGICAL_AND.
  const cudf::ast::expression* result = exprRefs[0];
  for (size_t i = 1; i < exprRefs.size(); ++i) {
    auto const& andExpr =
        tree.push(Operation{Op::NULL_LOGICAL_AND, *result, *exprRefs[i]});
    result = &andExpr;
  }

  return *result;
}
} // namespace facebook::velox::cudf_velox
