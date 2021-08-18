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

#include "velox/experimental/codegen/code_generator/ExprCodeGenerator.h"
#include <memory>
#include <optional>
#include <sstream>
#include "velox/experimental/codegen/ast/ASTNode.h"

namespace facebook::velox {
namespace codegen {

namespace {

// TODO add codgenType inside velox type traits for now we are using
// DeepCopiedType since use the same so far
template <TypeKind kind>
using ConstantExpressionNodeType =
    codegen::ConstantExpression<typename TypeTraits<kind>::DeepCopiedType>;

// Helper function to convert velox variant to codegen constantExpression
template <TypeKind kind>
std::shared_ptr<ConstantExpressionNodeType<kind>> toCodegenConstantExpr(
    const TypePtr& type,
    const velox::variant& value) {
  return std::make_shared<ConstantExpressionNodeType<kind>>(
      ConstantExpressionNodeType<kind>(
          type,
          value.isNull() ? std::nullopt
                         : std::optional{value.template value<kind>()}));
}

} // namespace

__attribute__((noinline)) size_t GeneratedExpressionStruct::uniqueId() {
  static std::atomic<size_t> counter = 0;
  return counter++;
};

// Might throw expression is not supported
CodegenASTNode ExprCodeGenerator::convertVeloxExpressionToCodegenAST(
    const std::shared_ptr<const core::ITypedExpr>& node,
    const RowType& inputRowType,
    std::vector<bool> inputRowNullability) {
  // inputRefExpr in velox has one child referring to the input row that do not
  // corresponds to any node in codegen AST and does not have any useful
  // information.
  if (auto inputRefExpr =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(node)) {
    auto codegenNode = std::make_shared<codegen::InputRefExpr>(
        inputRefExpr->type(),
        inputRefExpr->name(),
        inputRowType.getChildIdx(inputRefExpr->name()));
    if (inputRowNullability.size() != 0) {
      /// TODO extract this onto a separate process
      // set nullability of inputRefExpr from inputRowNullability
      codegenNode->setMaybeNull(inputRowNullability.at(
          inputRowType.getChildIdx(inputRefExpr->name())));
    }
    return codegenNode;
  }

  // convert the children subtrees to codegen AST
  std::vector<CodegenASTNode> codegenInputs;
  std::vector<velox::TypeKind> inputTypes;

  for (auto& input : node->inputs()) {
    auto inputNode = convertVeloxExpressionToCodegenAST(
        input, inputRowType, inputRowNullability);

    codegenInputs.push_back(inputNode);
    inputTypes.push_back(input->type()->kind());
  }

  // Create codegen ASTNode and connect children subtrees
  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(node)) {
    switch (constantExpr->type()->kind()) {
      case TypeKind::BOOLEAN:
        return toCodegenConstantExpr<TypeKind::BOOLEAN>(
            constantExpr->type(), constantExpr->value());
      case TypeKind::TINYINT:
        return toCodegenConstantExpr<TypeKind::TINYINT>(
            constantExpr->type(), constantExpr->value());
      case TypeKind::SMALLINT:
        return toCodegenConstantExpr<TypeKind::SMALLINT>(
            constantExpr->type(), constantExpr->value());
      case TypeKind::INTEGER:
        return toCodegenConstantExpr<TypeKind::INTEGER>(
            constantExpr->type(), constantExpr->value());
      case TypeKind::BIGINT:
        return toCodegenConstantExpr<TypeKind::BIGINT>(
            constantExpr->type(), constantExpr->value());
      case TypeKind::REAL:
        return toCodegenConstantExpr<TypeKind::REAL>(
            constantExpr->type(), constantExpr->value());
      case TypeKind::DOUBLE:
        return toCodegenConstantExpr<TypeKind::DOUBLE>(
            constantExpr->type(), constantExpr->value());
      case TypeKind::VARCHAR:
        return toCodegenConstantExpr<TypeKind::VARCHAR>(
            constantExpr->type(), constantExpr->value());
      default:
        break; // not supported otherwise
    }
  }

  // Cast expression
  if (auto castExpr =
          std::dynamic_pointer_cast<const core::CastTypedExpr>(node)) {
    // TODO: support null on failure
    if (castExpr->nullOnFailure() == false &&
        castExpr->type()->isFixedWidth()) {
      auto udfInformation =
          getCastUDF(castExpr->type()->kind(), castIntByTruncate);
      return std::make_shared<codegen::UDFCallExpr>(
          node->type(), udfInformation, codegenInputs);
    }
  }

  if (auto callExpr =
          std::dynamic_pointer_cast<const core::CallTypedExpr>(node)) {
    // Make row expression
    if (callExpr->name() == "concatRow") {
      return std::make_shared<codegen::MakeRowExpression>(
          node->type(), codegenInputs);
    }

    // If expression
    if (callExpr->name() == "if") {
      if (codegenInputs.size() == 3) {
        return std::make_shared<codegen::IfExpression>(
            node->type(), codegenInputs[0], codegenInputs[1], codegenInputs[2]);
      }

      if (codegenInputs.size() == 2) {
        return std::make_shared<codegen::IfExpression>(
            node->type(), codegenInputs[0], codegenInputs[1]);
      }
    }

    // Switch expression
    if (callExpr->name() == "switch") {
      return std::make_shared<codegen::SwitchExpression>(
          node->type(), codegenInputs);
    }

    // Symbol arithmetic expressions
    if (useBuiltInArithmetic_) {
      if (callExpr->name() == "plus") {
        return std::make_shared<codegen::AddExpr>(
            node->type(), codegenInputs.at(0), codegenInputs.at(1));
      }

      if (callExpr->name() == "minus") {
        return std::make_shared<codegen::SubtractExpr>(
            node->type(), codegenInputs.at(0), codegenInputs.at(1));
      }

      if (callExpr->name() == "multiply") {
        return std::make_shared<codegen::MultiplyExpr>(
            node->type(), codegenInputs.at(0), codegenInputs.at(1));
      }
    }

    if (useBuiltInLogical_) {
      if (callExpr->name() == "gt") {
        return std::make_shared<codegen::GreaterThan>(
            node->type(), codegenInputs.at(0), codegenInputs.at(1));
      }

      if (callExpr->name() == "gte") {
        return std::make_shared<codegen::GreaterThanEquel>(
            node->type(), codegenInputs.at(0), codegenInputs.at(1));
      }

      if (callExpr->name() == "lt") {
        return std::make_shared<codegen::LessThan>(
            node->type(), codegenInputs.at(0), codegenInputs.at(1));
      }

      if (callExpr->name() == "lte") {
        return std::make_shared<codegen::LessThanEqual>(
            node->type(), codegenInputs.at(0), codegenInputs.at(1));
      }

      if (callExpr->name() == "eq") {
        return std::make_shared<codegen::Equal>(
            node->type(), codegenInputs.at(0), codegenInputs.at(1));
      }

      if (callExpr->name() == "neq") {
        return std::make_shared<codegen::NotEqual>(
            node->type(), codegenInputs.at(0), codegenInputs.at(1));
      }

      if (callExpr->name() == "and") {
        return std::make_shared<codegen::LogicalAnd>(
            codegenInputs.at(0), codegenInputs.at(1));
      }

      if (callExpr->name() == "or") {
        return std::make_shared<codegen::LogicalOr>(
            codegenInputs.at(0), codegenInputs.at(1));
      }
    }

    if (callExpr->name() == ("coalesce")) {
      return std::make_shared<codegen::CoalesceExpr>(
          node->type(), codegenInputs);
    }

    if (callExpr->name() == ("is_null")) {
      return std::make_shared<codegen::IsNullExpr>(
          node->type(), codegenInputs.at(0));
    }

    if (callExpr->name() == ("not")) {
      return std::make_shared<codegen::NotExpr>(
          node->type(), codegenInputs.at(0));
    }

    // UDF calls
    // First check if the name is a supported argument-typed udf
    auto udfInformation =
        udfManager_.getUDFInformationTypedArgs(callExpr->name(), inputTypes);
    if (udfInformation.has_value()) {
      return std::make_shared<codegen::UDFCallExpr>(
          node->type(), *udfInformation, codegenInputs);
    }

    // Check name only udfs
    udfInformation = udfManager_.getUDFInformationUnTypedArgs(callExpr->name());
    if (udfInformation.has_value()) {
      return std::make_shared<codegen::UDFCallExpr>(
          node->type(), *udfInformation, codegenInputs);
    }
  }

  // Another form of "Make Row" expression
  if (auto concatExpr =
          std::dynamic_pointer_cast<const core::ConcatTypedExpr>(node)) {
    return std::make_shared<codegen::MakeRowExpression>(
        node->type(), codegenInputs);
  }

  // Translation not supported
  throw CodegenNotSupported(fmt::format(
      "unsupported conversion from typed expression {}\n", node->toString()));
}

// Codegen expression a codegen ASTs(rooted at MakeRowExpression)
GeneratedExpressionStruct ExprCodeGenerator::generateCodegenExpressionStruct(
    const codegen::MakeRowExpression& expressionTree,
    const RowType& inputType,
    const RowType& outputType) {
  auto codeSnippet = expressionTree.generateCode(codeGenCtx_, "outputTuple");
  return GeneratedExpressionStruct(
      inputType,
      outputType,
      codeGenCtx_.headers(),
      codeGenCtx_.declarations(),
      codeSnippet.code());
}

/// Codegen expression a codegen ASTs
GeneratedExpressionStruct ExprCodeGenerator::codegenExpression(
    const codegen::ASTNode& expressionTree,
    const RowType& inputType,
    const RowType& outputType) {
  auto codeSnippet = expressionTree.generateCode(codeGenCtx_, "outputTuple");
  return GeneratedExpressionStruct(
      inputType,
      outputType,
      codeGenCtx_.headers(),
      codeGenCtx_.declarations(),
      codeSnippet.code());
}

/// Codegen expression from a codegen ASTs
/// This Overload compute the expected input row type dynamically during
/// codegen. The user should use the generated type to pass the inputs
/// correctly.
GeneratedExpressionStruct ExprCodeGenerator::codegenExpression(
    const codegen::ASTNode& expressionTree) {
  // Mapping needs to be reset.
  codeGenCtx_.columnNameToIndexMap().clear();
  codeGenCtx_.setComputeInputTypeDuringCodegen(true);

  auto codeSnippet = expressionTree.generateCode(codeGenCtx_, "outputTuple");

  // Compute input type based on the columns encountered during the tree
  // traversal
  std::vector<std::string> usedColumnName(
      codeGenCtx_.columnNameToIndexMap().size());
  std::vector<TypePtr> usedColumnType(
      codeGenCtx_.columnNameToIndexMap().size());
  for (auto& [name, indexTypePair] : codeGenCtx_.columnNameToIndexMap()) {
    usedColumnName[indexTypePair.first] = name;
    usedColumnType[indexTypePair.first] = indexTypePair.second;
  };

  RowType inputType(std::move(usedColumnName), std::move(usedColumnType));
  RowType outputType({""}, {expressionTree.typePtr()});

  return GeneratedExpressionStruct(
      inputType,
      outputType,
      codeGenCtx_.headers(),
      codeGenCtx_.declarations(),
      codeSnippet.code());
}

/// Codegen a velox ITypeExpression tree rooted at concatTypedExpr
GeneratedExpressionStruct
ExprCodeGenerator::codegenExpressionStructFromVeloxExpr(
    const std::shared_ptr<const core::ConcatTypedExpr>& node,
    const RowType& inputType,
    std::vector<bool> inputRowNullability) {
  // Set nullability to default if not provided
  if (inputRowNullability.size() == 0 && inputType.size() != 0) {
    inputRowNullability.resize(inputType.size(), true);
  }

  auto outputType =
      std::dynamic_pointer_cast<const velox::RowType>(node->type());

  auto codegenAST =
      convertVeloxExpressionToCodegenAST(node, inputType, inputRowNullability);

  // convert velox expression tree to codegen AST
  auto codegenExprTree =
      std::dynamic_pointer_cast<codegen::MakeRowExpression>(codegenAST);

  // propagate nullability information
  codegenExprTree->propagateNullability();

  // validate the generated tree
  codegenExprTree->validate();

  // codegen from codegen AST
  return generateCodegenExpressionStruct(
      *codegenExprTree.get(), inputType, *outputType.get());
}

std::pair<std::string, std::string>
GeneratedExpressionStruct::wrapAsExternCallable(
    const CodegenCtx& exprCodegenCtx,
    const std::string& readersTupleType,
    const std::string& writersTupleType) {
  auto invokeSymbolName =
      fmt::format("invoke_{}_{}", expressionName_, uniqueId());

  std::stringstream stream;
  for (auto& header : exprCodegenCtx.headers()) {
    stream << fmt::format("#include{}\n", header);
  }

  if (exprCodegenCtx.headers().count("<tuple>") == 0) {
    stream << "#include <tuple>\n";
  }

  stream << "namespace facebook {\n namespace velox {\n";

  stream << "\n" << toStruct() << "\n\n";

  // Add the extern invoke method
  stream << fmt::format(
      "extern \"C\" void {0}(const {1}& input, {2}& output){{\n"
      "static {3} expressionObject;\n" // keep object allocated constant alive
      "expressionObject.invoke(input, output);}}\n",
      invokeSymbolName,
      readersTupleType,
      writersTupleType,
      expressionName_);

  // Close namespace
  stream << "}}\n";
  return {invokeSymbolName, stream.str()};
}

// Generate a cpp file that includes the generatedExpression
// TODO: Rename this to something more descriptive
const std::string& GeneratedExpressionStruct::toStruct(
    const std::string& name) const {
  if (structCode_.has_value()) {
    return *structCode_;
  }

  std::stringstream structCode;
  structCode << fmt::format("struct {} {{ ", name);
  structCode << fmt::format("using inputTupleType = {};\n", inputType_);

  // Add constant declarations
  for (auto& decl : declarations_) {
    structCode << decl << ";\n";
  }

  // Add state class
  structCode << fmt::format(
      "struct State {{ codegen::TempsAllocator allocator; }} state;\n\n");

  // TODO: let's templatise the invoke function;
  structCode << fmt::format(
      "template<typename IN,typename OUT>"
      "inline void invoke(IN && inputTuple, OUT&& outputTuple){{ {} }}",
      invokeFunctionBody_);
  structCode
      << "template<typename IN,typename OUT>" << std::endl
      << "inline void operator()(IN && inputTuple, OUT && outputTuple) {"
      << std::endl
      << "invoke(std::forward<IN>(inputTuple),std::forward<OUT>(outputTuple));"
      << std::endl
      << "};" << std::endl;
  structCode << "};";
  return *(structCode_ = std::optional{structCode.str()});
}

const std::string& GeneratedExpressionStruct::toStruct() const {
  return toStruct(expressionName_);
}

} // namespace codegen
} // namespace facebook::velox
