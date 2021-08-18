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
#include "velox/exec/tests/OperatorTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
class PlanNodeToStringTest : public OperatorTestBase {
 public:
  PlanNodeToStringTest() {
    auto rowType = ROW({"c0", "c1"}, {INTEGER(), INTEGER()});

    auto vector = makeRowVector(
        {makeFlatVector<int32_t>(100, [](auto row) { return row; }),
         makeFlatVector<int32_t>(100, [](auto row) { return row + 1; })});
    vectors_.push_back(vector);

    plan_ = exec::test::PlanBuilder()
                .values(vectors_)
                .filter("c0 % 10 < 9")
                .project(
                    {"c0", "c0 % 100 + c1 % 50"},
                    {
                        "out1",
                        "out2",
                    })
                .filter("out1 % 10 < 8")
                .project({"out1 + 10"}, {"out3"})
                .planNode();
  }

  std::shared_ptr<core::PlanNode> plan_;

 private:
  std::vector<RowVectorPtr> vectors_;
};

TEST_F(PlanNodeToStringTest, basic) {
  auto output = plan_->toString();
  ASSERT_EQ("->project\n", output);
}

TEST_F(PlanNodeToStringTest, recursive) {
  auto output = plan_->toString(false, true);
  auto* expectedOutput =
      "->project\n"
      "  ->filter\n"
      "    ->project\n"
      "      ->filter\n"
      "        ->values\n"
      "";
  ASSERT_EQ(std::string(expectedOutput), output);
}

TEST_F(PlanNodeToStringTest, detailed) {
  auto output = plan_->toString(true, false);
  ASSERT_EQ(
      "->project[expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10)), ]\n",
      output);
}

TEST_F(PlanNodeToStringTest, recursiveAndDetailed) {
  auto output = plan_->toString(true, true);
  ASSERT_EQ(
      "->project[expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10)), ]\n"
      "  ->filter[expression: lt(modulus(cast ROW[\"out1\"] as BIGINT,10),8)]\n"
      "    ->project[expressions: (out1:INTEGER, ROW[\"c0\"]), (out2:BIGINT, plus(modulus(cast ROW[\"c0\"] as BIGINT,100),modulus(cast ROW[\"c1\"] as BIGINT,50))), ]\n"
      "      ->filter[expression: lt(modulus(cast ROW[\"c0\"] as BIGINT,10),9)]\n"
      "        ->values[]\n",
      output);
}
