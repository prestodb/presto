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

#include "velox/experimental/exec/OffProcessExpressionEval.h"
#include <folly/Random.h>
#include <folly/init/Init.h>
#include "velox/connectors/fuzzer/tests/FuzzerConnectorTestBase.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/parse/Expressions.h"

namespace facebook::velox::exec::test {

class OffProcessExpressionEvalTest
    : public connector::fuzzer::test::FuzzerConnectorTestBase {
 protected:
  OffProcessExpressionEvalTest() {
    exec::Operator::registerOperator(
        std::make_unique<OffProcessExpressionEvalTranslator>());
  }

  std::vector<core::TypedExprPtr> parseExpr(
      const std::string& exprString,
      const RowVectorPtr& rowVector) {
    auto rowType = asRowType(rowVector->type());
    std::vector<core::TypedExprPtr> typedExprs;
    typedExprs.push_back(OperatorTestBase::parseExpr(
        exprString, rowType, parse::ParseOptions{}));
    return typedExprs;
  }

 private:
  VectorFuzzer::Options fuzzerOptions_;
};

TEST_F(OffProcessExpressionEvalTest, singleBatch) {
  auto rowVector = vectorMaker_.rowVector({
      vectorMaker_.flatVector({0, 1, 2, 3, 4}),
      vectorMaker_.flatVector({"a", "b", "c", "d", "e"}),
  });

  auto lambda = [&](std::string id, core::PlanNodePtr input) {
    return std::make_shared<OffProcessExpressionEvalNode>(
        id, parseExpr("1 + 2", rowVector), input);
  };

  auto plan =
      exec::test::PlanBuilder().values({rowVector}).addNode(lambda).planNode();
  exec::test::AssertQueryBuilder(plan).assertResults(rowVector);
}

TEST_F(OffProcessExpressionEvalTest, multipleBatches) {
  std::vector<RowVectorPtr> inputVectors;

  for (int32_t i = 0; i < 100; i++) {
    inputVectors.push_back(vectorMaker_.rowVector({
        vectorMaker_.flatVector({folly::Random::rand32()}),
        vectorMaker_.flatVector({std::to_string(i)}),
    }));
  }

  auto plan = exec::test::PlanBuilder()
                  .values(inputVectors)
                  .addNode([](std::string id, core::PlanNodePtr input) {
                    return std::make_shared<OffProcessExpressionEvalNode>(
                        id, std::vector<core::TypedExprPtr>{}, input);
                  })
                  .planNode();

  exec::test::AssertQueryBuilder(plan).assertResults(inputVectors);
}

TEST_F(OffProcessExpressionEvalTest, fuzzer) {
  for (size_t i = 0; i < 10; i++) {
    auto randType = VectorFuzzer({}, pool()).randRowType();

    auto plan = exec::test::PlanBuilder()
                    .tableScan(randType, makeFuzzerTableHandle(), {})
                    .addNode([](std::string id, core::PlanNodePtr input) {
                      return std::make_shared<OffProcessExpressionEvalNode>(
                          id, std::vector<core::TypedExprPtr>{}, input);
                    })
                    .planNode();

    exec::test::AssertQueryBuilder(plan)
        .splits(makeFuzzerSplits(50, 100))
        .assertTypeAndNumRows(randType, 50 * 100);
  }
}

} // namespace facebook::velox::exec::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
