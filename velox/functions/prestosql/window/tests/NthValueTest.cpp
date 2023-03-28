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
#include "velox/functions/prestosql/window/tests/WindowTestBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

// This test class is for different variations of nth_value function
// parameterized by the over clause. Each function invocation tests the function
// over all possible frame clauses.
class NthValueTest : public WindowTestBase {
 protected:
  NthValueTest() : overClause_("") {}

  explicit NthValueTest(const std::string& overClause)
      : overClause_(overClause) {}

  // This test has all important variations of the nth_value function
  // invocation to be tested per (dataset, partition, frame) clause combination.
  void testNthValue(const std::vector<RowVectorPtr>& input) {
    // This is a basic test case to give the value of the first frame row.
    testWindowFunction(input, "nth_value(c0, 1)");

    // This function invocation gets the column value of the 10th
    // frame row. Many tests have < 10 rows per partition. so the function
    // is expected to return null for such offsets.
    testWindowFunction(input, "nth_value(c0, 10)");

    // This test gets the nth_value offset from a column c2. The offsets could
    // be outside the partition also. The error cases for -ve offset values
    // are tested separately.
    testWindowFunction(input, "nth_value(c3, c2)");
  }

  // This is for testing different output column types in the nth_value
  // column parameter.
  void testPrimitiveType(const TypePtr& type) {
    vector_size_t size = 25;
    auto vectors = makeRowVector({
        makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
        // Note : The Fuzz vector used in nth_value can have null values.
        makeRandomInputVector(type, size, 0.3),
    });

    // Add c4 column in sort order in overClauses to impose a deterministic
    // output row order in the tests.
    auto newOverClause = overClause_ + ", c4";

    // The below tests cover nth_value invocations with constant and column
    // arguments. The offsets could also give rows beyond the partition
    // returning null in those cases.
    WindowTestBase::testWindowFunction(
        {vectors}, "nth_value(c4, 1)", {newOverClause}, kFrameClauses);
    WindowTestBase::testWindowFunction(
        {vectors}, "nth_value(c4, 7)", {newOverClause}, kFrameClauses);
    WindowTestBase::testWindowFunction(
        {vectors}, "nth_value(c4, c2)", {newOverClause}, kFrameClauses);
  }

 private:
  void testWindowFunction(
      const std::vector<RowVectorPtr>& input,
      const std::string& function) {
    WindowTestBase::testWindowFunction(
        input, function, {overClause_}, kFrameClauses);
  }

  const std::string overClause_;
};

class MultiNthValueTest : public NthValueTest,
                          public testing::WithParamInterface<std::string> {
 public:
  MultiNthValueTest() : NthValueTest(GetParam()) {}
};

// Tests nth_value with data of a uniform distribution.
TEST_P(MultiNthValueTest, basic) {
  testNthValue({makeSimpleVector(50)});
}

// Tests nth_value with a dataset with a single partition.
TEST_P(MultiNthValueTest, singlePartition) {
  testNthValue({makeSinglePartitionVector(50)});
}

// Tests nth_value with a dataset with a single partition, but spread across
// 2 input vectors.
TEST_P(MultiNthValueTest, multiInput) {
  testNthValue({makeSinglePartitionVector(50), makeSinglePartitionVector(75)});
}

// Tests nth_value with a dataset where all partitions have a single row.
TEST_P(MultiNthValueTest, singleRowPartitions) {
  testNthValue({makeSingleRowPartitionsVector((50))});
}

// Tests nth_value with a randomly generated dataset.
TEST_P(MultiNthValueTest, randomInput) {
  testNthValue({makeRandomInputVector((50))});
}

// Tests nth_value projecting result columns of different types.
TEST_P(MultiNthValueTest, integerValues) {
  testPrimitiveType(INTEGER());
}

TEST_P(MultiNthValueTest, tinyintValues) {
  testPrimitiveType(TINYINT());
}

TEST_P(MultiNthValueTest, smallintValues) {
  testPrimitiveType(SMALLINT());
}

TEST_P(MultiNthValueTest, bigintValues) {
  testPrimitiveType(BIGINT());
}

TEST_P(MultiNthValueTest, realValues) {
  testPrimitiveType(REAL());
}

TEST_P(MultiNthValueTest, doubleValues) {
  testPrimitiveType(DOUBLE());
}

TEST_P(MultiNthValueTest, varcharValues) {
  testPrimitiveType(VARCHAR());
}

TEST_P(MultiNthValueTest, varbinaryValues) {
  testPrimitiveType(VARBINARY());
}

TEST_P(MultiNthValueTest, timestampValues) {
  testPrimitiveType(TIMESTAMP());
}

TEST_P(MultiNthValueTest, dateValues) {
  testPrimitiveType(DATE());
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    NthValueTest,
    MultiNthValueTest,
    testing::ValuesIn(std::vector<std::string>(kOverClauses)));

TEST_F(NthValueTest, nullOffsets) {
  // Test that nth_value with null offset returns rows with null value.
  vector_size_t size = 100;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
      makeFlatVector<int64_t>(
          size, [](auto row) { return row % 3 + 1; }, nullEvery(5)),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
  });

  WindowTestBase::testWindowFunction(
      {vectors}, "nth_value(c0, c2)", kOverClauses);
}

TEST_F(NthValueTest, invalidOffsets) {
  vector_size_t size = 20;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5; }),
  });

  std::string overClause = "partition by c0 order by c1";
  std::string offsetError = "Offset must be at least 1";
  assertWindowFunctionError(
      {vectors}, "nth_value(c0, 0)", overClause, offsetError);
  assertWindowFunctionError(
      {vectors}, "nth_value(c0, -1)", overClause, offsetError);
  assertWindowFunctionError(
      {vectors}, "nth_value(c0, c2)", overClause, offsetError);
}

TEST_F(NthValueTest, invalidFrames) {
  vector_size_t size = 20;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5; }),
  });

  std::string overClause = "partition by c0 order by c1";
  assertWindowFunctionError(
      {vectors},
      "nth_value(c0, 5)",
      overClause,
      "rows between 0 preceding and current row",
      "k in frame bounds must be at least 1");
  assertWindowFunctionError(
      {vectors},
      "nth_value(c0, 5)",
      overClause,
      "rows between c2 preceding and current row",
      "k in frame bounds must be at least 1");
}

}; // namespace
}; // namespace facebook::velox::window::test
