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
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/functions/lib/window/tests/WindowTestBase.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {
namespace {

// Test parameter is function name: lead or lag.
class LeadLagTest : public WindowTestBase,
                    public testing::WithParamInterface<std::string> {
 protected:
  void SetUp() override {
    WindowTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
  }

  std::string fn(const std::string& params) {
    return fmt::format("{}({})", GetParam(), params);
  }

  bool isLag() {
    return GetParam() == "lag";
  }

  RowVectorPtr appendColumn(
      const RowVectorPtr& rowVector,
      const VectorPtr& newColumn) {
    std::vector<VectorPtr> columns = rowVector->children();
    columns.push_back(newColumn);
    return makeRowVector(columns);
  }
};

TEST_P(LeadLagTest, offset) {
  // largeOffset is larger than std::numeric_limits<int32_t>::max()
  // and is a negative number when cast to int32.
  int64_t largeOffset = (int64_t)std::numeric_limits<int32_t>::max() * 2;
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      // Offsets.
      makeFlatVector<int64_t>({1, 2, 3, 1, 2}),
      // Offsets with nulls.
      makeNullableFlatVector<int64_t>({1, 2, 3, std::nullopt, 2}),
      // Large offsets.
      makeFlatVector<int64_t>(
          {largeOffset, largeOffset, largeOffset, largeOffset, largeOffset}),
      // Default values.
      makeNullableFlatVector<int64_t>({std::nullopt, 99, 99, 99, std::nullopt}),
  });

  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");

    SCOPED_TRACE(queryInfo.functionSql);
    assertQuery(queryInfo.planNode, queryInfo.querySql);
  };

  // Default offset.
  assertResults(fn("c0"));

  // Constant offset.
  assertResults(fn("c0, 2"));

  // Large offset.
  assertResults(fn("c0, c3"));

  // Large && CONSTANT offset.
  assertResults(fn(fmt::format("c0, {}", largeOffset)));

  // Constant null offset. DuckDB returns incorrect results for this case. It
  // treats null offset as 0.
  auto queryInfo =
      buildWindowQuery({data}, fn("c0, null::bigint"), "order by c0", "");

  auto expected =
      appendColumn(data, makeAllNullFlatVector<int64_t>(data->size()));
  assertQuery(queryInfo.planNode, expected);

  // Variable offsets.
  assertResults(fn("c0, c1"));

  // Variable offsets with nulls.
  queryInfo = buildWindowQuery({data}, fn("c0, c2"), "order by c0", "");

  // This query hits UBSAN failure in DuckDB (probably due to null offset).
  std::vector<std::optional<int64_t>> expectedWindow;
  if (isLag()) {
    expectedWindow = {
        std::nullopt, std::nullopt, std::nullopt, std::nullopt, 3};
  } else {
    expectedWindow = {2, 4, std::nullopt, std::nullopt, std::nullopt};
  }
  expected =
      appendColumn(data, makeNullableFlatVector<int64_t>(expectedWindow));

  assertQuery(queryInfo.planNode, expected);

  // Out of range offsets return default value(99 here, constant case), whereas
  // null offsets return null.
  queryInfo = buildWindowQuery({data}, fn("c0, c2, 99"), "order by c0", "");
  if (isLag()) {
    expectedWindow = {99, 99, 99, std::nullopt, 3};
  } else {
    expectedWindow = {2, 4, 99, std::nullopt, 99};
  }
  expected =
      appendColumn(data, makeNullableFlatVector<int64_t>(expectedWindow));
  assertQuery(queryInfo.planNode, expected);

  // Out of range offsets return default value(null here, constant null case),
  // whereas null offsets return null.
  queryInfo =
      buildWindowQuery({data}, fn("c0, c2, null::bigint"), "order by c0", "");
  if (isLag()) {
    expectedWindow = {
        std::nullopt, std::nullopt, std::nullopt, std::nullopt, 3};
  } else {
    expectedWindow = {2, 4, std::nullopt, std::nullopt, std::nullopt};
  }
  expected =
      appendColumn(data, makeNullableFlatVector<int64_t>(expectedWindow));
  assertQuery(queryInfo.planNode, expected);

  // Out of range offsets return default value(c4 here, nullable offset
  // variable case), whereas null offsets return null.
  queryInfo = buildWindowQuery({data}, fn("c0, c2, c4"), "order by c0", "");
  if (isLag()) {
    expectedWindow = {std::nullopt, 99, 99, std::nullopt, 3};
  } else {
    expectedWindow = {2, 4, 99, std::nullopt, std::nullopt};
  }
  expected =
      appendColumn(data, makeNullableFlatVector<int64_t>(expectedWindow));
  assertQuery(queryInfo.planNode, expected);
}

TEST_P(LeadLagTest, ignoreNullsInt64Offset) {
  // The offset is bigger than int32:max() and it is also a positive number
  // if cast to int32. With only such a special number we can trigger
  // some tricky bug.
  int64_t largeOffset = (int64_t)std::numeric_limits<uint32_t>::max() + 2;
  auto data = makeRowVector(
      {// Values.
       makeNullableFlatVector<int64_t>({1, std::nullopt, 3, 4, 5}),
       // Offsets.
       makeFlatVector<int64_t>(
           {largeOffset, largeOffset, largeOffset, largeOffset, largeOffset})});

  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");

    SCOPED_TRACE(queryInfo.functionSql);
    assertQuery(queryInfo.planNode, queryInfo.querySql);
  };

  // Test the large offset which is a column reference.
  assertResults(fn("c0, c1 IGNORE NULLS"));

  // Test the large offset which is a CONSTANT.
  assertResults(fn(fmt::format("c0, {} IGNORE NULLS", largeOffset)));
}

TEST_P(LeadLagTest, zeroOffset) {
  auto data = makeRowVector({
      // Values with null.
      makeNullableFlatVector<int32_t>(
          {1, std::nullopt, 2, std::nullopt, std::nullopt}),
      // Values without null.
      makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
      // Offsets.
      makeFlatVector<int64_t>({0, 0, 0, 0, 0}),
  });
  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");
    SCOPED_TRACE(queryInfo.functionSql);
    assertQuery(queryInfo.planNode, queryInfo.querySql);
  };

  assertResults(fn("c0, 0"));
  assertResults(fn("c0, c2"));
  assertResults(fn("c0, 0 IGNORE NULLS"));
  assertResults(fn("c0, c2 IGNORE NULLS"));

  assertResults(fn("c1, 0"));
  assertResults(fn("c1, c2"));
  assertResults(fn("c1, 0 IGNORE NULLS"));
  assertResults(fn("c1, c2 IGNORE NULLS"));
}

TEST_P(LeadLagTest, defaultValue) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      // Default values.
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
      // Default values with nulls.
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30, std::nullopt, 50}),
  });

  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");

    SCOPED_TRACE(queryInfo.functionSql);
    assertQuery(queryInfo.planNode, queryInfo.querySql);
  };

  // Constant non-null default value.
  assertResults(fn("c0, 2, 100"));
  assertResults(fn("c0, 22, 100"));

  // Constant null default value.
  assertResults(fn("c0, 2, null::bigint"));

  // Variable default values.
  assertResults(fn("c0, 2, c1"));
  assertResults(fn("c0, 22, c1"));

  // Variable default values with nulls.
  assertResults(fn("c0, 2, c2"));
  assertResults(fn("c0, 22, c2"));
}

// Make sure resultOffset passed to LagFunction::apply is handled correctly.
TEST_P(LeadLagTest, smallPartitions) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row; }),
      // Small partitions. 5 rows each.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row / 5; }),
      // Default values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row * 10; }),
  });

  createDuckDbTable({data});

  // Single-row partitions.
  auto queryInfo = buildWindowQuery({data}, fn("c0"), "partition by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery({data}, fn("c0, 1, 100"), "partition by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery({data}, fn("c0, 2, c2"), "partition by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  // Small partitions.
  queryInfo =
      buildWindowQuery({data}, fn("c0"), "partition by c1 order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery(
      {data}, fn("c0, 1, 100"), "partition by c1 order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery(
      {data}, fn("c0, 2, c2"), "partition by c1 order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);
}

// Make sure partitionOffset logic in LagFunction::apply works correctly.
TEST_P(LeadLagTest, largePartitions) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row; }),
      // Offsets with nulls.
      makeFlatVector<int64_t>(
          10'000, [](auto row) { return 1 + row % 5; }, nullEvery(7)),
      // Default values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row * 10; }),
  });

  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");
    SCOPED_TRACE(queryInfo.functionSql);
    AssertQueryBuilder(queryInfo.planNode, duckDbQueryRunner_)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(queryInfo.querySql);
  };

  assertResults(fn("c0"));
  assertResults(fn("c0, 5"));
  assertResults(fn("c0, 5, 100"));
  assertResults(fn("c0, 50000, 100"));

  // This query hits UBSAN failure in DuckDB (probably due to null offset).
  auto queryInfo = buildWindowQuery({data}, fn("c0, c1"), "order by c0", "");

  VectorPtr expectedWindow;
  if (isLag()) {
    expectedWindow = makeFlatVector<int64_t>(
        data->size(),
        [](auto row) { return row - (1 + row % 5); },
        [](auto row) { return row < 5 || row % 7 == 0; });
  } else {
    expectedWindow = makeFlatVector<int64_t>(
        data->size(),
        [](auto row) { return row + (1 + row % 5); },
        [](auto row) { return row >= 9'997 || row % 7 == 0; });
  }

  {
    SCOPED_TRACE(queryInfo.functionSql);
    AssertQueryBuilder(queryInfo.planNode)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(appendColumn(data, expectedWindow));
  }

  assertResults(fn("c0, 50000, c2"));

  // This query hits UBSAN failure in DuckDB (probably due to null offset).
  queryInfo = buildWindowQuery({data}, fn("c0, c1, c2"), "order by c0", "");

  if (isLag()) {
    expectedWindow = makeFlatVector<int64_t>(
        data->size(),
        // Default values.
        [](auto row) {
          auto defaultValue = row < 5;
          return defaultValue ? row * 10 : row - (1 + row % 5);
        },
        nullEvery(7));
  } else {
    expectedWindow = makeFlatVector<int64_t>(
        data->size(),
        // Default values.
        [](auto row) {
          auto defaultValue = row >= 9'997;
          return defaultValue ? row * 10 : row + (1 + row % 5);
        },
        nullEvery(7));
  }

  {
    SCOPED_TRACE(queryInfo.functionSql);
    AssertQueryBuilder(queryInfo.planNode)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(appendColumn(data, expectedWindow));
  }
}

TEST_P(LeadLagTest, invalidOffset) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      // Offsets.
      makeFlatVector<int64_t>({1, 0, -2, 2, 4}),
  });

  auto copyResults = [&](const std::string& sql) {
    auto queryInfo = buildWindowQuery({data}, sql, "", "");
    AssertQueryBuilder(queryInfo.planNode).copyResults(pool());
  };

  VELOX_ASSERT_THROW(
      copyResults(fn("c0, -1")), "(-1 vs. 0) Offset must be at least 0");
  VELOX_ASSERT_THROW(
      copyResults(fn("c0, c1")), "(-2 vs. 0) Offset must be at least 0");
}

// Verify that lag function doesn't take frames into account. It operates on the
// whole partition instead.
TEST_P(LeadLagTest, emptyFrames) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  createDuckDbTable({data});

  static const std::string kEmptyFrame =
      "rows between 100 preceding AND 90 preceding";

  // DuckDB results are incorrect. It returns NULL for empty frames.
  std::vector<std::optional<int64_t>> expectedWindow;

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "", kEmptyFrame);
    auto expected = makeRowVector({
        data->childAt(0),
        makeNullableFlatVector<int64_t>(expectedWindow),
    });
    assertQuery(queryInfo.planNode, expected);
  };

  if (isLag()) {
    expectedWindow = {std::nullopt, 1, 2, 3, 4};
    assertResults(fn("c0"));

    expectedWindow = {std::nullopt, std::nullopt, 1, 2, 3};
    assertResults(fn("c0, 2"));

    expectedWindow = {100, 100, 1, 2, 3};
    assertResults(fn("c0, 2, 100"));
  } else {
    expectedWindow = {2, 3, 4, 5, std::nullopt};
    assertResults(fn("c0"));

    expectedWindow = {3, 4, 5, std::nullopt, std::nullopt};
    assertResults(fn("c0, 2"));

    expectedWindow = {3, 4, 5, 100, 100};
    assertResults(fn("c0, 2, 100"));
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(LagTest, LeadLagTest, ::testing::Values("lag"));

VELOX_INSTANTIATE_TEST_SUITE_P(
    LeadTest,
    LeadLagTest,
    ::testing::Values("lead"));

// DuckDB has errors in IGNORE NULLS logic for empty
// frames (tested above). So using non-empty frames.
inline const std::vector<std::string> kIgnoreNullsFrames = {
    "range current row",
    "range between unbounded preceding and current row",

    "range between unbounded preceding and unbounded following",

    "rows between 5 preceding and unbounded following",
    "rows between unbounded preceding and 5 following",

    "rows between 1 preceding and 5 following",

    "rows between c2 preceding and unbounded following",
    "rows between unbounded preceding and c2 following",
    "rows between c2 preceding and c2 following",
};

inline const std::vector<std::string> kIgnoreNullsPartitionClauses = {
    "partition by c0 order by c1 desc, c2",
    "partition by c0 order by c1 desc nulls first, c2",
    "partition by c0 order by c1 asc, c2",
    "partition by c0 order by c1 asc nulls first, c2",
};

TEST_F(LeadLagTest, ignoreNulls) {
  auto size = 40;
  auto input = makeRowVector(
      {makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
       makeFlatVector<int64_t>(
           size, [](auto row) { return row % 7; }, nullEvery(8)),
       makeFlatVector<int64_t>(size, [](auto row) { return row % 6 + 1; }),
       // All null values.
       makeAllNullFlatVector<int64_t>(size)});
  // c1 has null values, so used for the values argument.
  const std::vector<std::string> kFunctionsList = {
      "lead(c1, 2 IGNORE NULLS)",
      "lag(c1, 2 IGNORE NULLS)",
      "lead(c1, c2 IGNORE NULLS)",
      "lag(c1, c2 IGNORE NULLS)",
      "lead(c1, 2, 5 IGNORE NULLS)",
      "lag(c1, 2, 5 IGNORE NULLS)",
      "lead(c1, 2, c2 IGNORE NULLS)",
      "lag(c1, 2, c2 IGNORE NULLS)",
      // All null values with IGNORE NULLS specified return default
      // value.
      "lead(c3, 2, 99 IGNORE NULLS)",
      "lag(c3, 2, 99 IGNORE NULLS)",
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

} // namespace
} // namespace facebook::velox::window::test
