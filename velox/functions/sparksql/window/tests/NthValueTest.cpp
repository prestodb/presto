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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/lib/window/tests/WindowTestBase.h"
#include "velox/functions/sparksql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

class SparkNthValueTest : public WindowTestBase {
 protected:
  void SetUp() override {
    WindowTestBase::SetUp();
    WindowTestBase::options_.parseIntegerAsBigint = false;
    velox::functions::window::sparksql::registerWindowFunctions("");
  }
};

TEST_F(SparkNthValueTest, basic) {
  vector_size_t size = 20;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 30; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 5 + 1; }),
  });

  std::string overClause = "partition by c0 order by c1";
  std::string frameClausee =
      "range between unbounded preceding and current row";
  WindowTestBase::testWindowFunction(
      {vectors}, "nth_value(c0, 1)", {overClause}, {frameClausee});
}

TEST_F(SparkNthValueTest, singlePartition) {
  vector_size_t size = 20;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 30; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 5 + 1; }),
  });

  std::string overClause = "partition by c0 order by c1";
  std::string frameClausee =
      "range between unbounded preceding and current row";
  WindowTestBase::testWindowFunction(
      {vectors}, "nth_value(c0, 1)", {overClause}, {frameClausee});
}

TEST_F(SparkNthValueTest, nonLiteralOffset) {
  vector_size_t size = 20;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 30; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
  });

  std::string overClause = "partition by c0 order by c1";
  std::string offsetError = "Offset must be literal for spark";
  assertWindowFunctionError(
      {vectors}, "nth_value(c0, c2)", overClause, offsetError);
}

TEST_F(SparkNthValueTest, invalidOffsets) {
  vector_size_t size = 20;

  auto vectors = makeRowVector(
      {makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
       makeFlatVector<int32_t>(size, [](auto row) { return row % 50; })});

  std::string overClause = "partition by c0 order by c1";
  std::string offsetError = "Offset must be at least 1";
  assertWindowFunctionError(
      {vectors}, "nth_value(c0, 0)", overClause, offsetError);
  assertWindowFunctionError(
      {vectors}, "nth_value(c0, -1)", overClause, offsetError);
}

}; // namespace
}; // namespace facebook::velox::window::test
