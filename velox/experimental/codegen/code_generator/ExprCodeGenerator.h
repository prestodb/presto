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
#pragma once
#include <string>

#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/experimental/codegen/ast/AST.h"
#include "velox/experimental/codegen/ast/CodegenCtx.h"

namespace facebook {
namespace velox {
namespace codegen {
using CodegenASTNode = std::shared_ptr<codegen::ASTNode>;

/// A class that represent a code generated from codegen AST tree to be called
/// from operators. (Not designed mainly for expression to expression calls)
///
/// TODO: Update this to reflected latest changes in D26383419
/// struct generatedExpression {
///  using InputTupleType = ...
///  using OutputTupleType= ...
///
///  void invoke(const InputType& InputTupleType, OutputTupleType& outputTuple)
///  {
///
///  }
/// };
class GeneratedExpressionStruct {
 public:
  GeneratedExpressionStruct(
      const RowType& inputRowTypeArg,
      const RowType& outputRowtTypeArg,
      const std::unordered_set<std::string>& headers,
      const std::vector<std::string>& declarations,
      const std::string& invokeFunctionBody,
      const std::string& nameSuffix_ = "")
      : inputRowType_(inputRowTypeArg),
        outputRowType_(outputRowtTypeArg),
        headers_(headers),
        inputType_(codegenUtils::codegenNativeType(
            inputRowTypeArg,
            codegenUtils::TypePlacement::Input)),
        outputImplType_(codegenUtils::codegenImplTypeName(outputRowtTypeArg)),
        inputImplType_(codegenUtils::codegenImplTypeName(inputRowTypeArg)),
        invokeFunctionBody_(invokeFunctionBody),
        declarations_(declarations) {
    expressionName_ =
        fmt::format("generatedExpression_{}_{}", nameSuffix_, uniqueId());
  }

  /// Copy constructor need to be explicitly declared because of the reference
  /// members
  GeneratedExpressionStruct(const GeneratedExpressionStruct& other) = default;
  GeneratedExpressionStruct(GeneratedExpressionStruct&& other) = delete;

  GeneratedExpressionStruct& operator=(GeneratedExpressionStruct&& other) =
      delete;
  GeneratedExpressionStruct& operator=(const GeneratedExpressionStruct& other) =
      delete;

  static size_t uniqueId();

  /// Create a cpp file that contains the generated expression along with an
  /// extern function to execute the expression. return the generated extern
  /// function name and the cpp file content
  std::pair<std::string, std::string> wrapAsExternCallable(
      const CodegenCtx& exprCodegenCtx,
      const std::string& readersTupleType,
      const std::string& writersTupleType);

  // Generate a cpp file content that includes the generatedExpression struct
  // definition
  const std::string& toStruct() const;

  const std::string& toStruct(const std::string& name) const;

  const std::string& expressionName() const {
    return expressionName_;
  }

  const std::string& inputTypeStr() const {
    return inputType_;
  }

  const std::string& outputImplTypeStr() const {
    return outputImplType_;
  }

  const std::string& inputImplTypeStr() const {
    return inputImplType_;
  }

  const RowType& inputRowType() const {
    return inputRowType_;
  }

  const RowType& outputRowType() const {
    return outputRowType_;
  }
  const std::unordered_set<std::string>& headers() const {
    return headers_;
  }

 private:
  RowType inputRowType_;

  RowType outputRowType_;

  /// TODO: We should really be story full library information here instead
  /// of just headers.
  /// Library headers required for compilation
  const std::unordered_set<std::string> headers_;

  /// Cache the struct definition of the expression to avoid recomputation in
  /// toStruct()
  mutable std::optional<std::string> structCode_;

  /// A unique name assigned to the generated expression (the name of the struct
  /// )
  std::string expressionName_;

  /// Code that represent the tuple input type
  const std::string inputType_;

  /// Code that represent the Implementation output type
  const std::string outputImplType_;

  /// Code that represent the Implementation input type
  const std::string inputImplType_;

  /// Body of the expression invoke member method
  const std::string invokeFunctionBody_;

  /// Members declarations to be added to the struct
  std::vector<std::string> declarations_;
};

/// This class provide utilities to convert a velox::typed expression to codegen
/// AST and to convert a codegen AST to GeneratedExpressionStruct
class ExprCodeGenerator {
 public:
  ExprCodeGenerator(
      const UDFManager& udfManager,
      bool useSymbolsForArithmetic,
      bool castIntByTruncateFlag)
      : castIntByTruncate(castIntByTruncateFlag),
        useBuiltInArithmetic_(useSymbolsForArithmetic),
        udfManager_(udfManager) {}

  // Codegen expression from velox::ConcatTypedExpr all the way to expression
  // struct.
  GeneratedExpressionStruct codegenExpressionStructFromVeloxExpr(
      const std::shared_ptr<const core::ConcatTypedExpr>& node,
      const RowType& inputType,
      std::vector<bool> inputRowNullability = std::vector<bool>());

  CodegenCtx& getContext() {
    return codeGenCtx_;
  }

  // Codegen expression from a codegen AST rooted at(MakeRowExpression)
  GeneratedExpressionStruct generateCodegenExpressionStruct(
      const codegen::MakeRowExpression& expressionTree,
      const RowType& inputType,
      const RowType& outputType);

  // Codegen expression from a codegen AST rooted at(OutputExpression)
  GeneratedExpressionStruct codegenExpression(
      const codegen::ASTNode& expressionTree,
      const RowType& inputType,
      const RowType& outputType);

  /// Codegen expression and compute inputType;
  GeneratedExpressionStruct codegenExpression(
      const codegen::ASTNode& expressionTree);

  // Convert velox::ITypedExpr to codegen AST
  CodegenASTNode convertVeloxExpressionToCodegenAST(
      const std::shared_ptr<const core::ITypedExpr>& node,
      const RowType& inputRowType,
      std::vector<bool> inputRowNullability = std::vector<bool>{});

  bool castIntByTruncate = false;

 private:
  // Whether arithmetic should be codegen using symbol expressions instead
  // of calling Velox UDFs
  bool useBuiltInArithmetic_ = false;

  bool useBuiltInLogical_ = true;

  // The appropriate UDF manager to be used during codegen AST generation
  const UDFManager& udfManager_;

  // Context used during code generation
  CodegenCtx codeGenCtx_;
};

} // namespace codegen
} // namespace velox
} // namespace facebook
