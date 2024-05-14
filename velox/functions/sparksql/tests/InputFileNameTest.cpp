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

#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class InputFileNameTest : public SparkFunctionBaseTest {
 protected:
  exec::DriverCtx driverCtxForTesting() {
    auto plan = PlanBuilder().tableScan(ROW({{"d", BIGINT()}})).planFragment();
    auto task = exec::Task::create(
        "t",
        std::move(plan),
        0,
        std::make_shared<core::QueryCtx>(executor_.get()),
        exec::Task::ExecutionMode::kParallel);
    exec::DriverCtx driverCtx(task, 0, 0, 0, 0);
    return driverCtx;
  }

  void testInputFileName(
      const std::string& fileName,
      const VectorPtr& value,
      const VectorPtr& expected) {
    auto selected = SelectivityVector(value->size());
    std::vector<core::TypedExprPtr> args;
    auto expr = exec::ExprSet(
        {std::make_shared<core::CallTypedExpr>(
            VARCHAR(), args, "input_file_name")},
        &execCtx_);
    auto data = makeRowVector({value});
    auto driverCtx = driverCtxForTesting();
    driverCtx.inputFileName = fileName;
    exec::EvalCtx evalCtx(&execCtx_, &expr, data.get(), &driverCtx);
    std::vector<VectorPtr> results(1);
    expr.eval(selected, evalCtx, results);
    velox::test::assertEqualVectors(expected, results[0]);
  }
};

TEST_F(InputFileNameTest, basic) {
  constexpr int32_t kSize = 10;
  auto value =
      makeFlatVector<int64_t>(kSize, [](vector_size_t row) { return row; });
  auto expected = makeConstant("file:///tmp/text.txt", kSize);
  testInputFileName("file:///tmp/text.txt", value, expected);

  auto values = makeNullableFlatVector<int64_t>(
      {1, 2, 3, 4, 5, std::nullopt, 6, 8, 9, 10});
  testInputFileName("file:///tmp/text.txt", values, expected);

  auto expecedWithSpecicalChars =
      makeConstant("file:///tmp/test%20dir/a=b/main%23text.txt", kSize);
  testInputFileName(
      "file:///tmp/test dir/a=b/main#text.txt",
      value,
      expecedWithSpecicalChars);
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
