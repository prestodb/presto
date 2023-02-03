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

class NthValueTest : public WindowTestBase {
 protected:
  NthValueTest() : frameClause_("range unbounded preceding") {}

  NthValueTest(const std::string& frameClause) : frameClause_(frameClause) {}

  void testWindowFunction(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::vector<std::string>& overClauses) {
    WindowTestBase::testWindowFunction(
        input, function, overClauses, frameClause_);
  }

  void testPrimitiveType(const TypePtr& type) {
    vector_size_t size = 25;
    auto vectors = makeRowVector({
        makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
        makeFlatFuzzVector(type, size),
    });

    // Add c3 column in sort order in overClauses to impose a deterministic
    // output row order in the tests.
    const auto column = ", c3";
    const std::vector<std::string> basicOverClauses =
        addSuffixToClauses(column, kBasicOverClauses);
    const std::vector<std::string> sortOrderBasedOverClauses =
        addSuffixToClauses(column, kSortOrderBasedOverClauses);

    testWindowFunction({vectors}, "nth_value(c3, c2)", basicOverClauses);
    testWindowFunction(
        {vectors}, "nth_value(c3, c2)", sortOrderBasedOverClauses);
    testWindowFunction({vectors}, "nth_value(c3, 1)", basicOverClauses);
    testWindowFunction(
        {vectors}, "nth_value(c3, 5)", sortOrderBasedOverClauses);
  }

  RowVectorPtr makeBasicVectors(vector_size_t size) {
    return makeRowVector({
        makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
        // These columns are used for k PRECEDING/FOLLOWING frame bounds. So
        // they should have values >= 1.
        makeFlatVector<int64_t>(size, [](auto row) { return row % 7 + 1; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
    });
  }

  RowVectorPtr makeSinglePartitionVectors(vector_size_t size) {
    return makeRowVector({
        makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
        // These columns are used for k PRECEDING/FOLLOWING frame bounds. So
        // they should have values >= 1.
        makeFlatVector<int64_t>(size, [](auto row) { return row % 50 + 1; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 5 + 1; }),
    });
  }

  const std::string frameClause_;
};

class MultiNthValueTest : public NthValueTest,
                          public testing::WithParamInterface<std::string> {
 public:
  MultiNthValueTest() : NthValueTest(GetParam()) {}
};

TEST_P(MultiNthValueTest, basic) {
  auto vectors = makeBasicVectors(50);

  testWindowFunction({vectors}, "nth_value(c0, c2)", kBasicOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 1)", kBasicOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 5)", kBasicOverClauses);
}

TEST_P(MultiNthValueTest, basicWithSortOrder) {
  auto vectors = makeBasicVectors(50);

  testWindowFunction(
      {vectors}, "nth_value(c0, c2)", kSortOrderBasedOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 1)", kSortOrderBasedOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 5)", kSortOrderBasedOverClauses);
}

TEST_P(MultiNthValueTest, singlePartition) {
  auto vectors = makeSinglePartitionVectors(500);

  testWindowFunction({vectors}, "nth_value(c0, c2)", kBasicOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 1)", kBasicOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 25)", kBasicOverClauses);
}

TEST_P(MultiNthValueTest, singlePartitionWithSortOrder) {
  auto vectors = makeSinglePartitionVectors(500);

  testWindowFunction(
      {vectors}, "nth_value(c0, c2)", kSortOrderBasedOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 1)", kSortOrderBasedOverClauses);
  testWindowFunction(
      {vectors}, "nth_value(c0, 25)", kSortOrderBasedOverClauses);
}

TEST_P(MultiNthValueTest, multiInput) {
  auto vectors = makeSinglePartitionVectors(250);
  auto doubleVectors = {vectors, vectors};

  testWindowFunction(doubleVectors, "nth_value(c0, c2)", kBasicOverClauses);
  testWindowFunction(doubleVectors, "nth_value(c0, 1)", kBasicOverClauses);
  testWindowFunction(doubleVectors, "nth_value(c0, 25)", kBasicOverClauses);
}

TEST_P(MultiNthValueTest, multiInputWithSortOrder) {
  auto vectors = makeSinglePartitionVectors(250);
  auto doubleVectors = {vectors, vectors};

  testWindowFunction(
      doubleVectors, "nth_value(c0, c2)", kSortOrderBasedOverClauses);
  testWindowFunction(
      doubleVectors, "nth_value(c0, 1)", kSortOrderBasedOverClauses);
  testWindowFunction(
      doubleVectors, "nth_value(c0, 25)", kSortOrderBasedOverClauses);
}

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

TEST_F(NthValueTest, nullOffsets) {
  // Test that nth_value with null offset returns rows with null value.
  vector_size_t size = 100;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
      makeFlatVector<int64_t>(
          size, [](auto row) { return row % 3 + 1; }, nullEvery(5)),
  });

  testWindowFunction({vectors}, "nth_value(c0, c2)", kBasicOverClauses);
  testWindowFunction(
      {vectors}, "nth_value(c0, c2)", kSortOrderBasedOverClauses);
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

VELOX_INSTANTIATE_TEST_SUITE_P(
    NthValueTest,
    MultiNthValueTest,
    testing::ValuesIn(std::vector<std::string>(kFrameClauses)));

}; // namespace
}; // namespace facebook::velox::window::test
