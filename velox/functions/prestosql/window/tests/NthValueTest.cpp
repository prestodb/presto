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
#include <boost/random/uniform_int_distribution.hpp>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/lib/window/tests/WindowTestBase.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

// This test class is for different variations of (nth|first|last)_value
// functions parameterized by the over clause. Each function invocation tests
// the function over all possible frame clauses.
class NthValueTest : public WindowTestBase {
 protected:
  NthValueTest() : overClause_("") {}

  explicit NthValueTest(const std::string& overClause)
      : overClause_(overClause) {}

  void SetUp() override {
    WindowTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
    rng_.seed(std::time(nullptr));
  }

  // These tests have all important variations of the (nth|first|last)_value
  // function invocations to be tested per (dataset, partition, frame) clause
  // combination. The following types of datasets are tested with this utility
  // function in the unit tests:
  // i) Data of a uniform distribution.
  // ii) Dataset with a single partition.
  // iii) Dataset with a single partition spread across 2 input vectors.
  // iv) Dataset where all partitions have a single row.
  // v) Dataset that is randomly generated.
  void testValueFunctions(const std::vector<RowVectorPtr>& input) {
    // This is a basic test case to give the value of the first frame row.
    testWindowFunction(input, "nth_value(c0, 1)");

    // This function invocation gets the column value of the 10th
    // frame row. Many tests have < 10 rows per partition. so the function
    // is expected to return null for such offsets.
    testWindowFunction(input, "nth_value(c2, 10)", false);

    // This test gets the nth_value offset from a column c2. The offsets could
    // be outside the partition also. The error cases for -ve offset values
    // are tested separately.
    testWindowFunction(input, "nth_value(c3, c2)", false);

    // The first_value, last_value functions are tested for columns c1 which
    // contains null values.
    testWindowFunction(input, "first_value(c1)", false);
    testWindowFunction(input, "last_value(c1)", false);
  }

  // This is for testing different output column types in the
  // (nth|first|last)_value functions' column parameter.
  void testPrimitiveTypes() {
    vector_size_t size = 25;
    auto vectors = makeRowVector({
        makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3; }),
        // Note : The Fuzz vector used in nth_value can have null values.
        makeRandomInputVector(INTEGER(), size, 0.5),
        makeRandomInputVector(TINYINT(), size, 0.1),
        makeRandomInputVector(SMALLINT(), size, 0.2),
        makeRandomInputVector(BIGINT(), size, 0.3),
        makeRandomInputVector(REAL(), size, 0.4),
        makeRandomInputVector(DOUBLE(), size, 0.5),
        makeRandomInputVector(VARCHAR(), size, 0.6),
        makeRandomInputVector(VARBINARY(), size, 0.7),
        makeRandomInputVector(TIMESTAMP(), size, 0.8),
        makeRandomInputVector(DATE(), size, 0.9),
        makeRandomInputVector(DECIMAL(10, 2), size, 0.1),
        makeRandomInputVector(DECIMAL(20, 5), size, 0.2),
    });

    const std::string overClause =
        "partition by c0 order by c1 desc nulls first, c2 asc nulls first, c3 desc";

    bool createTable = true;
    for (auto i = 4; i < vectors->childrenSize(); i++) {
      auto col = fmt::format("c{}", i);
      // Add the type specific column in sort order in overClauses to impose a
      // deterministic output row order in the tests.
      auto newOverClause = overClause + ", " + col;

      // The below tests cover nth_value invocations with constant and column
      // arguments. The offsets could also give rows beyond the partition
      // returning null in those cases.
      WindowTestBase::testWindowFunction(
          {vectors},
          fmt::format("nth_value({}, 1)", col),
          {newOverClause},
          kFrameClauses,
          createTable);
      createTable = false;
      WindowTestBase::testWindowFunction(
          {vectors},
          fmt::format("nth_value({}, 7)", col),
          {newOverClause},
          kFrameClauses,
          createTable);
      WindowTestBase::testWindowFunction(
          {vectors},
          fmt::format("nth_value({}, c2)", col),
          {newOverClause},
          kFrameClauses,
          createTable);

      WindowTestBase::testWindowFunction(
          {vectors},
          fmt::format("first_value({})", col),
          {newOverClause},
          kFrameClauses,
          createTable);
      WindowTestBase::testWindowFunction(
          {vectors},
          fmt::format("last_value({})", col),
          {newOverClause},
          kFrameClauses,
          createTable);
    }
  }

  FuzzerGenerator rng_;

 private:
  void testWindowFunction(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      bool createDuckDBTable = true) {
    WindowTestBase::testWindowFunction(
        input, function, {overClause_}, kFrameClauses, createDuckDBTable);
  }

  const std::string overClause_;
};

class MultiNthValueTest : public NthValueTest,
                          public testing::WithParamInterface<std::string> {
 public:
  MultiNthValueTest() : NthValueTest(GetParam()) {}
};

TEST_P(MultiNthValueTest, basic) {
  testValueFunctions({makeSimpleVector(40)});
}

TEST_P(MultiNthValueTest, singlePartition) {
  testValueFunctions(
      {makeSinglePartitionVector(40), makeSinglePartitionVector(50)});
}

TEST_P(MultiNthValueTest, singleRowPartitions) {
  testValueFunctions({makeSingleRowPartitionsVector((30))});
}

TEST_P(MultiNthValueTest, randomInput) {
  testValueFunctions({makeRandomInputVector((25))});
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    NthValueTest,
    MultiNthValueTest,
    testing::ValuesIn(std::vector<std::string>(kOverClauses)));

TEST_F(NthValueTest, allTypes) {
  testPrimitiveTypes();
}

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

  auto offset =
      boost::random::uniform_int_distribution<int32_t>(INT32_MIN, -1)(rng_);
  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
      makeFlatVector<int64_t>(
          size, [offset](auto /* row */) { return offset; }),
  });

  std::string overClause = "partition by c0 order by c1";

  assertWindowFunctionError(
      {vectors},
      "nth_value(c0, 5)",
      overClause,
      fmt::format("rows between {} preceding and current row", offset),
      fmt::format("Window frame {} offset must not be negative", offset));
  assertWindowFunctionError(
      {vectors},
      "nth_value(c0, 5)",
      overClause,
      "rows between c2 preceding and current row",
      fmt::format("Window frame {} offset must not be negative", offset));
}

TEST_F(NthValueTest, int32FrameOffset) {
  vector_size_t size = 100;
  WindowTestBase::options_.parseIntegerAsBigint = false;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
      makeFlatVector<int64_t>(
          size, [](auto row) { return row % 3 + 1; }, nullEvery(5)),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
  });

  const std::vector<std::string> kPartitionClauses = {
      "partition by c0 order by c1 nulls first, c2, c3",
      "partition by c0, c2 order by c1 nulls first, c3",
      "partition by c0 order by c1 desc, c2, c3",
      "partition by c0, c2 order by c1 desc nulls first, c3",
  };
  WindowTestBase::testWindowFunction(
      {vectors},
      "nth_value(c0, c2)",
      kPartitionClauses,
      {"rows between 5 preceding and current row"});

  WindowTestBase::options_.parseIntegerAsBigint = true;
}

TEST_F(NthValueTest, emptyFrames) {
  auto vectors = makeSinglePartitionVector(100);
  static const std::vector<std::string> kFunctionsList = {
      "first_value(c2)",
      "first_value(c2)",
      "nth_value(c2, 5)",
      "nth_value(c1, c2)",
  };
  bool createTable = true;
  for (const auto& fn : kFunctionsList) {
    WindowTestBase::testWindowFunction(
        {vectors}, fn, kOverClauses, kEmptyFrameClauses, createTable);
    createTable = false;
  }
}

// DuckDB has errors in IGNORE NULLS logic when the start or end
// bound is CURRENT ROW and also when both frame ends are
// preceding or following. So the testing is limited to this
// subset of frame clauses.
// The limitations are fixed in the latest version of DuckDB, so
// these tests will be updated then.
inline const std::vector<std::string> kIgnoreNullsFrames = {
    "range between unbounded preceding and unbounded following",
    "rows between unbounded preceding and unbounded following",

    "rows between 5 preceding and unbounded following",
    "rows between unbounded preceding and 5 following",

    "rows between 1 preceding and 5 following",
    "rows between c2 preceding and unbounded following",
    "rows between unbounded preceding and c2 following",
    "rows between c2 preceding and c2 following",

    // Frame clauses with invalid frames.
    "rows between 1 preceding and 4 preceding",
    "rows between 4 following and 1 following",
};

inline const std::vector<std::string> kIgnoreNullsPartitionClauses = {
    "partition by c0 order by c1 desc, c2, c3",
    "partition by c0 order by c1 desc nulls first, c2, c3",
    "partition by c0 order by c1 asc, c2, c3",
    "partition by c0 order by c1 asc nulls first, c2, c3",
};

TEST_F(NthValueTest, ignoreNulls) {
  auto input = makeSimpleVector(40);
  const std::vector<std::string> kFunctionsList = {
      "first_value(c1 IGNORE NULLS)",
      "last_value(c1 IGNORE NULLS)",
      "nth_value(c1, 3 IGNORE NULLS)",
      "nth_value(c3, c2 IGNORE NULLS)",
  };

  bool createTable = true;
  for (auto fn : kFunctionsList) {
    WindowTestBase::testWindowFunction(
        {input},
        fn,
        kIgnoreNullsPartitionClauses,
        kIgnoreNullsFrames,
        createTable);
    createTable = false;
  }
}

TEST_F(NthValueTest, frameStartsFromFollowing) {
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({1, std::nullopt, 2}),
      makeFlatVector<bool>({false, false, false}),
      makeFlatVector<int64_t>({1, 2, 3}),
  });
  auto expected = makeRowVector(
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 2}),
       makeFlatVector<bool>({false, false, false}),
       makeFlatVector<int64_t>({1, 2, 3}),
       makeNullableFlatVector<int64_t>({2, 2, std::nullopt})});

  WindowTestBase::testWindowFunction(
      {input},
      "first_value(c0 IGNORE NULLS)",
      "partition by c1 order by c2",
      "rows between 1 following and unbounded following",
      expected);
  WindowTestBase::testWindowFunction(
      {input},
      "last_value(c0 IGNORE NULLS)",
      "partition by c1 order by c2",
      "rows between 1 following and unbounded following",
      expected);
}

// These tests are added since DuckDB has issues with
// CURRENT ROW frames. These tests will be replaced by DuckDB based
// tests after it is upgraded to v0.8.
TEST_F(NthValueTest, ignoreNullsCurrentRow) {
  auto size = 15;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5; }),
      makeFlatVector<int64_t>(
          size, [](auto row) { return row % 5; }, nullEvery(3)),
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
  });
  createDuckDbTable({input});

  std::vector<std::optional<int64_t>> expectedWindow;
  auto assertResults = [&](const std::string& functionSql,
                           const std::string& frame) {
    auto queryInfo = buildWindowQuery(
        {input}, functionSql, "partition by c0 order by c2", frame);
    auto expected = makeRowVector({
        input->childAt(0),
        input->childAt(1),
        input->childAt(2),
        makeNullableFlatVector<int64_t>(expectedWindow),
    });
    assertQuery(queryInfo.planNode, expected);
  };

  expectedWindow = {
      std::nullopt,
      1,
      2,
      std::nullopt,
      4,
      0,
      std::nullopt,
      2,
      3,
      std::nullopt,
      0,
      1,
      std::nullopt,
      3,
      4};
  assertResults("first_value(c1 IGNORE NULLS)", "rows current row");
  assertResults("nth_value(c1, 1 IGNORE NULLS)", "rows current row");
  assertResults("last_value(c1 IGNORE NULLS)", "rows current row");

  expectedWindow = {
      std::nullopt, 1, 2, std::nullopt, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4};
  assertResults(
      "first_value(c1 IGNORE NULLS)",
      "rows between unbounded preceding and current row");
  assertResults(
      "nth_value(c1, 1 IGNORE NULLS)",
      "rows between unbounded preceding and current row");
  assertResults(
      "last_value(c1 IGNORE NULLS)",
      "rows between unbounded preceding and current row");

  expectedWindow = {
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      1,
      2,
      std::nullopt,
      4,
      0,
      1,
      2,
      3,
      4};
  assertResults(
      "first_value(c1 IGNORE NULLS)",
      "rows between unbounded preceding and 1 preceding");
  assertResults(
      "nth_value(c1, 1 IGNORE NULLS)",
      "rows between unbounded preceding and 1 preceding");
  assertResults(
      "last_value(c1 IGNORE NULLS)",
      "rows between unbounded preceding and 1 preceding");

  expectedWindow = {0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, std::nullopt, 3, 4};
  assertResults(
      "first_value(c1 IGNORE NULLS)",
      "rows between current row and 1 following");
  assertResults(
      "nth_value(c1, 1 IGNORE NULLS)",
      "rows between current row and 1 following");
  assertResults(
      "last_value(c1 IGNORE NULLS)",
      "rows between current row and 1 following");

  expectedWindow = {0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, std::nullopt, 3, 4};
  assertResults(
      "first_value(c1 IGNORE NULLS)",
      "rows between current row and unbounded following");
  assertResults(
      "nth_value(c1, 1 IGNORE NULLS)",
      "rows between current row and unbounded following");
  assertResults(
      "last_value(c1 IGNORE NULLS)",
      "rows between current row and unbounded following");

  expectedWindow = {
      std::nullopt, 1, 2, std::nullopt, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4};
  assertResults(
      "first_value(c1 IGNORE NULLS)",
      "range between unbounded preceding and current row");
  assertResults(
      "nth_value(c1, 1 IGNORE NULLS)",
      "range between unbounded preceding and current row");
  assertResults(
      "last_value(c1 IGNORE NULLS)",
      "range between unbounded preceding and current row");

  expectedWindow = {0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, std::nullopt, 3, 4};
  assertResults(
      "first_value(c1 IGNORE NULLS)",
      "range between current row and unbounded following");
  assertResults(
      "nth_value(c1, 1 IGNORE NULLS)",
      "range between current row and unbounded following");
  assertResults(
      "last_value(c1 IGNORE NULLS)",
      "range between current row and unbounded following");
}

} // namespace
} // namespace facebook::velox::window::test
