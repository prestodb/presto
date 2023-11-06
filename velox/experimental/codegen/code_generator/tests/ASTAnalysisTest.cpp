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

#include "velox/experimental/codegen/ast/ASTAnalysis.h"
#include "velox/experimental/codegen/ast/ASTNode.h"
#include "velox/experimental/codegen/code_generator/tests/CodegenExpressionsTestBase.h"
#include "velox/experimental/codegen/udf_manager/UDFManager.h"
#include "velox/type/Type.h"

namespace facebook::velox {
namespace codegen::expressions::test {
class ASTAnalysisTest : public ExpressionCodegenTestBase {
  void SetUp() override {
    ExpressionCodegenTestBase::SetUp();
    registerVeloxArithmeticUDFs(udfManager);
  }

 public:
  template <typename InputRowTypeTrait>
  ASTNodePtr generateCodegenAST(const std::string& expression) {
    auto inputRowType = makeRowType(InputRowTypeTrait::veloxDynamicTypes());
    auto untypedExpr = parse::parseExpr(expression, options_);
    auto typedExpr = core::Expressions::inferTypes(
        untypedExpr, inputRowType, getExecContext().pool());

    auto codegenExprTree =
        getExprCodeGenerator().convertVeloxExpressionToCodegenAST(
            typedExpr, *inputRowType.get());
    return codegenExprTree;
  }

  template <typename InputRowTypeTrait>
  void testDefaultNull(
      bool strict,
      const std::string& expression,
      bool expectedResult) {
    // Create velox row type
    auto codegenExprTree = generateCodegenAST<InputRowTypeTrait>(expression);
    testDefaultNull(strict, *codegenExprTree.get(), expectedResult);
  }

  void testDefaultNull(
      bool strict,
      const ASTNode& codegenExprTree,
      bool expectedResult) {
    if (strict) {
      ASSERT_EQ(
          codegen::analysis::isDefaultNullStrict(codegenExprTree),
          expectedResult);
    } else {
      ASSERT_EQ(
          codegen::analysis::isDefaultNull(codegenExprTree), expectedResult);
    }
  }

  template <typename InputRowTypeTrait>
  void testFilterDefaultNull(
      const std::string& expression,
      bool expectedResult) {
    // Create velox row type
    auto codegenExprTree = generateCodegenAST<InputRowTypeTrait>(expression);
    testFilterDefaultNull(*codegenExprTree.get(), expectedResult);
  }

  void testFilterDefaultNull(
      const ASTNode& codegenExprTree,
      bool expectedResult) {
    ASSERT_EQ(
        codegen::analysis::isFilterDefaultNull(codegenExprTree),
        expectedResult);
  }

  parse::ParseOptions options_;
};

TEST_F(ASTAnalysisTest, DefaultNullStrict) {
  testDefaultNull<RowTypeTrait<>>(true, "12.9", true);
  testDefaultNull<RowTypeTrait<TypeKind::REAL>>(true, "C0", true);
  testDefaultNull<RowTypeTrait<TypeKind::REAL, TypeKind::REAL>>(
      true, "C0+C1", true);
  testDefaultNull<RowTypeTrait<TypeKind::REAL, TypeKind::REAL>>(
      true, "C0+C1*12.1", true);

  testDefaultNull<RowTypeTrait<TypeKind::REAL, TypeKind::REAL>>(
      true, "if(C0>C1, C1, C0)", false);
  testDefaultNull<RowTypeTrait<TypeKind::REAL, TypeKind::REAL>>(
      true, "C0+if(C0>C1, C0, C1)", false);
}

TEST_F(ASTAnalysisTest, DefaultNull) {
  // All default null strict are default nulls
  testDefaultNull<RowTypeTrait<>>(false, "12.9", true);
  testDefaultNull<RowTypeTrait<TypeKind::REAL>>(false, "C0", true);
  testDefaultNull<RowTypeTrait<TypeKind::REAL, TypeKind::REAL>>(
      false, "C0+C1", true);
  testDefaultNull<RowTypeTrait<TypeKind::REAL, TypeKind::REAL>>(
      false, "C0+C1*12.1", true);

  testDefaultNull<RowTypeTrait<TypeKind::REAL, TypeKind::REAL>>(
      false, "if(C0>C1, C1, C0)", false);
  testDefaultNull<RowTypeTrait<TypeKind::REAL, TypeKind::REAL>>(
      false, "C0+if(C0>C1, C0, C1)", false);

  // default nulls but not defaul nulls strict
  auto udf = UDFInformation()
                 .withNullMode(ExpressionNullMode::NullInNullOut)
                 .withIsOptionalArguments(false)
                 .withIsOptionalOutput(true) // nullable
                 .withOutputForm(OutputForm::Return)
                 .withCodegenNullInNullOutChecks(true)
                 .withCalledFunctionName("test");

  auto ast = UDFCallExpr(
      INTEGER(),
      udf,
      {generateCodegenAST<RowTypeTrait<TypeKind::BIGINT>>("C0"),
       generateCodegenAST<RowTypeTrait<TypeKind::BIGINT>>("1")});

  // not strict default null
  testDefaultNull(true, ast, false);
  // is default null
  testDefaultNull(false, ast, true);

  // TODO: once we support udfs with such behavior add more test
}

TEST_F(ASTAnalysisTest, FilterDefaultNull) {
  // arithmetic + comparison
  testFilterDefaultNull<RowTypeTrait<>>("1 < 2", true);
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT>>("C0 < 1", true);
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT>>(
      "C0 < C1", true);
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT>>(
      "C0+1 < 2*C1/C0", true);

  // logical
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT>>(
      "C0 < 1 AND C1 < 2", true);
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT>>(
      "NOT (C0 < 1 OR C1 < 2)", true);
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT>>("NOT C0 IS NULL", true);

  // non-default null
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT>>(
      "IF(C0>1,C0,C1)", false);
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT>>(
      "CASE WHEN C0 > 1 THEN C0 ELSE C1 END", false);
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT>>(
      "C0 IS NULL", false);
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT>>(
      "COALESCE(C0,C1)", false);

  // example where the current analysis is failing us, all of the exprs below
  // are supposed to be true but can't be detected by the algorithm yet
  // FIXME: remove these when algorithm improved
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT>>(
      "C0 > 1 OR C0 > 1", false);
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT>>(
      "C0 > 1 AND 1 < 2", false);
  testFilterDefaultNull<RowTypeTrait<TypeKind::BIGINT>>(
      "C0 > 1 AND 1 is NULL", false);
}
} // namespace codegen::expressions::test
}; // namespace facebook::velox
