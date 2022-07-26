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

#include "velox/experimental/codegen/CodegenASTAnalysis.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include "velox/core/PlanNode.h"
#include "velox/dwio_move/common/tests/utils/BatchMaker.h"
#include "velox/experimental/codegen/CodegenCompiledExpressionTransform.h"
#include "velox/experimental/codegen/CompiledExpressionAnalysis.h"
#include "velox/experimental/codegen/tests/CodegenTestBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::codegen;
using namespace facebook::velox::core;
using namespace facebook::velox::test;

namespace facebook::velox::codegen {

auto parseTypedExprs(
    const std::vector<std::string>& stringExprs,
    std::shared_ptr<const RowType>& rowType) {
  std::vector<std::shared_ptr<const core::ITypedExpr>> typedExprs;
  for (const auto& stringExpr : stringExprs) {
    auto untyped = parse::parseExpr(stringExpr);
    auto typed = core::Expressions::inferTypes(untyped, rowType);
    typedExprs.push_back(std::move(typed));
  };
  return typedExprs;
}

class GenerateAstTest : public CodegenTestBase {
 protected:
  void SetUp() override {
    functions::prestosql::registerAllFunctions();

    registerVeloxArithmeticUDFs(udfManager_);
    useBuiltInForArithmetic_ = false;

    pool_ = memory::getDefaultScopedMemoryPool();

    rowType_ = ROW({"c0", "c1", "c2"}, {DOUBLE(), DOUBLE(), DOUBLE()});

    vector_ = std::vector<RowVectorPtr>{std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 10, *pool_))};

    auto valueNode = std::make_shared<ValuesNode>(
        "valueNode", std::vector<RowVectorPtr>{vector_});

    std::vector<std::string> exprString{"c0 + c1", "c1 - c0"};

    auto projections = parseTypedExprs(exprString, rowType_);

    project_ = std::make_shared<ProjectNode>(
        "projection",
        std::vector<std::string>{"p", "m"},
        projections,
        valueNode);
  };

  std::unique_ptr<facebook::velox::memory::MemoryPool> pool_;

  std::shared_ptr<ProjectNode> project_;
  std::shared_ptr<const RowType> rowType_;
  std::vector<RowVectorPtr> vector_;

  bool useBuiltInForArithmetic_;
  UDFManager udfManager_;
};

TEST_F(GenerateAstTest, CreateAST) {
  CodegenASTAnalysis astAnalysis(udfManager_, useBuiltInForArithmetic_);
  astAnalysis.run(*project_.get());

  EXPECT_EQ(astAnalysis.nodes().size(), 2);

  for (const auto& projection : project_->projections()) {
    EXPECT_EQ(astAnalysis.nodes().count(projection), 1);
  }
};

TEST_F(GenerateAstTest, CreateASTSourceCode) {
  CompiledExpressionAnalysis compiled(udfManager_, useBuiltInForArithmetic_);
  compiled.run(*project_.get());

  EXPECT_EQ(compiled.code().size(), 2);

  for (const auto& projection : project_->projections()) {
    EXPECT_EQ(compiled.code().count(projection), 1);
  }
};

TEST_F(GenerateAstTest, CompiledExpr) {
  CompiledExpressionAnalysis expressionAnalysis(
      udfManager_, useBuiltInForArithmetic_);
  expressionAnalysis.run(*project_.get());
  facebook::velox::codegen::detail::Visitor visitor(
      expressionAnalysis.code(), this->defaultCompilerOptions());
  auto newProject =
      std::dynamic_pointer_cast<facebook::velox::core::ProjectNode>(
          visitor.visit(*project_.get(), project_->sources()));
  for (const auto projectExpr : newProject->projections()) {
    // Check we generated ICompiledCode nodes.
    auto callExpression =
        std::dynamic_pointer_cast<const codegen::ICompiledCall>(
            projectExpr->inputs()[0]);
    ASSERT_NE(callExpression, nullptr);
  };
};
} // namespace facebook::velox::codegen
