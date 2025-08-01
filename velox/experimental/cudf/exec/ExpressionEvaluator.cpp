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
#include <cudf/strings/attributes.hpp>
#include <cudf/strings/contains.hpp>
#include <cudf/strings/slice.hpp>
#include <cudf/table/table.hpp>
#include <cudf/transform.hpp>

namespace facebook::velox::cudf_velox {
namespace {
template <TypeKind kind>
cudf::ast::literal makeScalarAndLiteral(
    const VectorPtr& vector,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    size_t atIndex = 0) {
  using T = typename facebook::velox::KindToFlatVector<kind>::WrapperType;
  auto stream = cudf::get_default_stream();
  auto mr = cudf::get_current_device_resource_ref();
  const auto& type = vector->type();

  if constexpr (cudf::is_fixed_width<T>()) {
    auto constVector = vector->as<facebook::velox::SimpleVector<T>>();
    VELOX_CHECK_NOT_NULL(constVector, "ConstantVector is null");
    T value = constVector->valueAt(atIndex);
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
    auto constVector = vector->as<facebook::velox::SimpleVector<StringView>>();
    auto value = constVector->valueAt(atIndex);
    std::string_view stringValue = static_cast<std::string_view>(value);
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

cudf::ast::literal createLiteral(
    const VectorPtr& vector,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    size_t atIndex = 0) {
  const auto kind = vector->typeKind();
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      makeScalarAndLiteral, kind, std::move(vector), scalars, atIndex);
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
    {"or", Op::NULL_LOGICAL_OR}};

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
    "like"};

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
    if (c1 and c1->toString() == "1:BIGINT" and c2 and
        c2->toString() == "0:BIGINT") {
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
  } else if (name == "substr") {
    // Extract the start and length parameters from the substr function call
    // and create a precomputed column with the substring operation.
    // This will be handled during AST evaluation with special column reference.
    VELOX_CHECK_EQ(len, 3);
    auto fieldExpr =
        std::dynamic_pointer_cast<FieldReference>(expr->inputs()[0]);
    VELOX_CHECK_NOT_NULL(fieldExpr, "Expression is not a field");

    auto c1 = dynamic_cast<ConstantExpr*>(expr->inputs()[1].get());
    auto c2 = dynamic_cast<ConstantExpr*>(expr->inputs()[2].get());
    std::string substrExpr =
        "substr " + c1->value()->toString(0) + " " + c2->value()->toString(0);

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
    std::cerr << "Unsupported expression: " << expr->toString() << std::endl;
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
    } else if (ins_name.rfind("substr", 0) == 0) {
      std::istringstream iss(ins_name.substr(6));
      int beginValue, length;
      iss >> beginValue >> length;
      auto beginScalar = cudf::numeric_scalar<cudf::size_type>(
          beginValue - 1,
          true,
          stream,
          cudf::get_current_device_resource_ref());
      auto endScalar = cudf::numeric_scalar<cudf::size_type>(
          beginValue - 1 + length,
          true,
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
} // namespace facebook::velox::cudf_velox
