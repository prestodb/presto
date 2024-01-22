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

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {

class LastAggregateTest : public aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    aggregate::test::AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
    // Disable incremental aggregation tests because the boolean field in
    // intermediate result of spark_last is unset and has undefined value.
    AggregationTestBase::disableTestIncremental();
  }

  template <typename T>
  void testAggregate() {
    {
      auto vectors = {makeRowVector({
          makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
          makeFlatVector<T>(
              98, // size
              [](auto row) { return row; }, // valueAt
              [](auto row) { return row % 3 == 0; }), // nullAt
      })};

      createDuckDbTable(vectors);

      // We do not test with TableScan because having two input splits makes the
      // result non-deterministic.
      {
        SCOPED_TRACE("ignore null + group by");
        testAggregations(
            vectors,
            {"c0"},
            {"spark_last_ignore_null(c1)"},
            "SELECT c0, last(c1 ORDER BY c1 NULLS FIRST) FROM tmp GROUP BY c0",
            /*config*/ {},
            /*testWithTableScan*/ false);
      }
      {
        // Expected result should have first 7 rows including nulls.
        SCOPED_TRACE("not ignore null + group by");
        auto expected = {makeRowVector({
            makeFlatVector<int32_t>(7, [](auto row) { return row; }),
            makeFlatVector<T>(
                7, // size
                [](auto row) { return 91 + row; }, // valueAt
                [](auto row) { return (91 + row) % 3 == 0; }), // nullAt
        })};
        testAggregations(
            vectors,
            {"c0"},
            {"spark_last(c1)"},
            expected,
            /*config*/ {},
            /*testWithTableScan*/ false);
      }
    }

    {
      auto vectors = {makeRowVector({
          makeNullableFlatVector<T>({std::nullopt, 1, 2, std::nullopt}),
      })};

      {
        SCOPED_TRACE("ignore null + global");
        auto expectedTrue = {makeRowVector({makeNullableFlatVector<T>({2})})};
        testAggregations(
            vectors,
            {},
            {"spark_last_ignore_null(c0)"},
            expectedTrue,
            /*config*/ {},
            /*testWithTableScan*/ false);
      }
      {
        SCOPED_TRACE("not ignore null + global");
        auto expectedFalse = {
            makeRowVector({makeNullableFlatVector<T>({std::nullopt})})};
        testAggregations(
            vectors,
            {},
            {"spark_last(c0)"},
            expectedFalse,
            /*config*/ {},
            /*testWithTableScan*/ false);
      }
    }
  }

  void testGroupBy(
      std::vector<RowVectorPtr> data,
      std::vector<RowVectorPtr> ignoreNullData,
      std::vector<RowVectorPtr> hasNullData) {
    {
      SCOPED_TRACE("ignore null + group by");
      testAggregations(
          data,
          {"c0"},
          {"spark_last_ignore_null(c1)"},
          ignoreNullData,
          /*config*/ {},
          /*testWithTableScan*/ false);
    }

    {
      SCOPED_TRACE("not ignore null + group by");
      testAggregations(
          data,
          {"c0"},
          {"spark_last(c1)"},
          hasNullData,
          /*config*/ {},
          /*testWithTableScan*/ false);
    }
  }

  void testGlobalAggregate(
      std::vector<RowVectorPtr> data,
      std::vector<RowVectorPtr> ignoreNullData,
      std::vector<RowVectorPtr> hasNullData) {
    {
      SCOPED_TRACE("ignore null + global");
      testAggregations(
          data,
          {},
          {"spark_last_ignore_null(c0)"},
          ignoreNullData,
          /*config*/ {},
          /*testWithTableScan*/ false);
    }

    {
      SCOPED_TRACE("not ignore null + global");
      testAggregations(
          data,
          {},
          {"spark_last(c0)"},
          hasNullData,
          /*config*/ {},
          /*testWithTableScan*/ false);
    }
  }
};

TEST_F(LastAggregateTest, boolean) {
  testAggregate<bool>();
}

TEST_F(LastAggregateTest, tinyInt) {
  testAggregate<int8_t>();
}

TEST_F(LastAggregateTest, smallInt) {
  testAggregate<int16_t>();
}

TEST_F(LastAggregateTest, integer) {
  testAggregate<int32_t>();
}

TEST_F(LastAggregateTest, bigint) {
  testAggregate<int64_t>();
}

TEST_F(LastAggregateTest, real) {
  testAggregate<float>();
}

TEST_F(LastAggregateTest, double) {
  testAggregate<double>();
}

TEST_F(LastAggregateTest, timestampGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeFlatVector<Timestamp>(
          98, // size
          [](auto row) { return Timestamp(row, row); }, // valueAt
          [](auto row) { return row % 3 == 0; }), // nullAt
  })};

  auto ignoreNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<Timestamp>(
          7, // size
          [](auto row) {
            return (row + 91) % 3 == 0 ? Timestamp(row + 91 - 7, row + 91 - 7)
                                       : Timestamp(row + 91, row + 91);
          } // valueAt
          ),
  })};

  // Expected result should have first 7 rows including nulls.
  auto hasNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<Timestamp>(
          7, // size
          [](auto row) { return Timestamp(91 + row, 91 + row); }, // valueAt
          [](auto row) { return (row + 91) % 3 == 0; }), // nullAt
  })};

  testGroupBy(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, timestampGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<Timestamp>(
          {std::nullopt, Timestamp(1, 1), Timestamp(2, 2), std::nullopt}),
  })};

  auto ignoreNullData = {
      makeRowVector({makeNullableFlatVector<Timestamp>({Timestamp(2, 2)})})};

  auto hasNullData = {
      makeRowVector({makeNullableFlatVector<Timestamp>({std::nullopt})})};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, dateGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeFlatVector<int32_t>(
          98, // size
          [](auto row) { return row; }, // valueAt
          [](auto row) { return row % 3 == 0; }, // nullAt
          DATE()),
  })};

  auto ignoreNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<int32_t>(
          7, // size
          [](auto row) {
            return (row + 91) % 3 == 0 ? row + 91 - 7 : row + 91;
          }, // valueAt,
          nullptr, // nullAt
          DATE()),
  })};

  // Expected result should have first 7 rows including nulls.
  auto hasNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<int32_t>(
          7, // size
          [](auto row) { return row + 91; }, // valueAt
          [](auto row) { return (row + 91) % 3 == 0; }, // nullAt
          DATE()),
  })};

  testGroupBy(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, dateGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<int32_t>(
          {std::nullopt, 1, 2, std::nullopt}, DATE()),
  })};

  auto ignoreNullData = {
      makeRowVector({makeNullableFlatVector<int32_t>({2}, DATE())})};

  auto hasNullData = {
      makeRowVector({makeNullableFlatVector<int32_t>({std::nullopt}, DATE())})};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, shortDecimalGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(4, [](auto row) { return row % 2; }),
      makeNullableFlatVector<int64_t>(
          {1, std::nullopt, std::nullopt, 2}, DECIMAL(8, 2)),
  })};

  auto ignoreNullData = {makeRowVector({
      makeFlatVector<int32_t>(2, [](auto row) { return row; }),
      makeNullableFlatVector<int64_t>({1, 2}, DECIMAL(8, 2)),
  })};

  auto hasNullData = {makeRowVector({
      makeFlatVector<int32_t>(2, [](auto row) { return row; }),
      makeNullableFlatVector<int64_t>({std::nullopt, 2}, DECIMAL(8, 2)),
  })};

  testGroupBy(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, shortDecimalGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<int64_t>({1, std::nullopt}, DECIMAL(8, 2)),
  })};

  auto ignoreNullData = {
      makeRowVector({makeNullableFlatVector<int64_t>({1}, DECIMAL(8, 2))})};

  auto hasNullData = {makeRowVector(
      {makeNullableFlatVector<int64_t>({std::nullopt}, DECIMAL(8, 2))})};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, longDecimalGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(4, [](auto row) { return row % 2; }),
      makeNullableFlatVector<int128_t>(
          {1, std::nullopt, std::nullopt, 2}, DECIMAL(28, 2)),
  })};

  auto ignoreNullData = {makeRowVector({
      makeFlatVector<int32_t>(2, [](auto row) { return row; }),
      makeNullableFlatVector<int128_t>({1, 2}, DECIMAL(28, 2)),
  })};

  auto hasNullData = {makeRowVector({
      makeFlatVector<int32_t>(2, [](auto row) { return row; }),
      makeNullableFlatVector<int128_t>({std::nullopt, 2}, DECIMAL(28, 2)),
  })};

  testGroupBy(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, longDecimalGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<int128_t>({1, std::nullopt}, DECIMAL(28, 2)),
  })};

  auto ignoreNullData = {
      makeRowVector({makeNullableFlatVector<int128_t>({1}, DECIMAL(28, 2))})};

  auto hasNullData = {makeRowVector(
      {makeNullableFlatVector<int128_t>({std::nullopt}, DECIMAL(28, 2))})};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, intervalGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeFlatVector<int64_t>(
          98, // size
          [](auto row) { return row; }, // valueAt
          [](auto row) { return row % 3 == 0; }, // nullAt
          INTERVAL_DAY_TIME()),
  })};

  auto ignoreNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<int64_t>(
          7, // size
          [](auto row) {
            return (row + 91) % 3 == 0 ? row + 91 - 7 : row + 91;
          }, // valueAt
          nullptr, // nullAt
          INTERVAL_DAY_TIME()),
  })};

  // Expected result should have first 7 rows including nulls.
  auto hasNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<int64_t>(
          7, // size
          [](auto row) { return row + 91; }, // valueAt
          [](auto row) { return (row + 91) % 3 == 0; }, // nullAt
          INTERVAL_DAY_TIME()),
  })};

  testGroupBy(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, intervalGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<int64_t>(
          {std::nullopt, 1, 2, std::nullopt}, INTERVAL_DAY_TIME()),
  })};

  auto ignoreNullData = {makeRowVector(
      {makeNullableFlatVector<int64_t>({2}, INTERVAL_DAY_TIME())})};

  auto hasNullData = {makeRowVector(
      {makeNullableFlatVector<int64_t>({std::nullopt}, INTERVAL_DAY_TIME())})};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, varcharGroupBy) {
  std::vector<std::string> data(98);
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeFlatVector<StringView>(
          98, // size
          [&data](auto row) {
            data[row] = std::to_string(row);
            return StringView(data[row]);
          }, // valueAt
          [](auto row) { return row % 3 == 0; }), // nullAt
  })};

  createDuckDbTable(vectors);

  // Verify when ignoreNull is true.
  testAggregations(
      vectors,
      {"c0"},
      {"spark_last_ignore_null(c1)"},
      "SELECT c0, last(c1) FROM tmp WHERE c1 IS NOT NULL GROUP BY c0",
      /*config*/ {},
      /*testWithTableScan*/ false);

  // Verify when ignoreNull is false.
  // Expected result should have last 7 rows [91..98) including nulls.
  auto expected = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<StringView>(
          7, // size
          [&data](auto row) { return StringView(data[91 + row]); }, // valueAt
          [](auto row) { return (91 + row) % 3 == 0; }), // nullAt
  })};
  testAggregations(
      vectors,
      {"c0"},
      {"spark_last(c1)"},
      expected,
      /*config*/ {},
      /*testWithTableScan*/ false);
}

TEST_F(LastAggregateTest, varcharGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<StringView>(
          {std::nullopt, "a", "b", std::nullopt}),
  })};

  auto ignoreNullData = {
      makeRowVector({makeNullableFlatVector<StringView>({"b"})})};

  auto hasNullData = {
      makeRowVector({makeNullableFlatVector<StringView>({std::nullopt})})};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, arrayGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeArrayVector<int64_t>(
          98, // size
          [](auto row) { return row % 3; }, // sizeAt
          [](auto row, auto idx) { return row * 100 + idx; }, // valueAt
          // Even rows are null.
          [](auto row) { return row % 2 == 0; }), // nullAt
  })};

  auto hasNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeArrayVector<int64_t>(
          7,
          [](auto row) {
            // Even rows are null, for these return values for (row - 7)
            return ((91 + row) % 2) ? (91 + row) % 3 : (91 + row - 7) % 3;
          },
          [](auto row, auto idx) {
            // Even rows are null, for these return values for (row - 7)
            return ((91 + row) % 2) ? (91 + row) * 100 + idx
                                    : (91 + row - 7) * 100 + idx;
          }),
  })};

  auto ignoreNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeArrayVector<int64_t>(
          7, // size
          [](auto row) { return (91 + row) % 3; }, // sizeAt
          [](auto row, auto idx) { return (91 + row) * 100 + idx; }, // valueAt
          [](auto row) { return (91 + row) % 2 == 0; }), // nullAt
  })};

  testGroupBy(vectors, hasNullData, ignoreNullData);
}

TEST_F(LastAggregateTest, arrayGlobal) {
  auto vectors = {makeRowVector({
      makeNullableArrayVector<int64_t>(
          {std::nullopt, {{1, 2}}, {{3, 4}}, std::nullopt}),
  })};

  auto ignoreNullData = {makeRowVector({
      makeArrayVector<int64_t>({{3, 4}}),
  })};

  auto hasNullData = {makeRowVector({
      makeNullableArrayVector<int64_t>({std::nullopt}),
  })};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(LastAggregateTest, mapGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeMapVector<int64_t, float>(
          98, // size
          [](auto row) { return row % 2 ? 0 : 2; }, // sizeAt
          [](auto idx) { return idx; }, // keyAt
          [](auto idx) { return idx * 0.1; }), // valueAt
  })};

  // Expected result should have last 7 rows of input |vectors|
  auto expected = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeMapVector<int64_t, float>(
          7, // size
          [](auto row) { return row % 2 ? 2 : 0; }, // sizeAt
          [](auto idx) { return 92 + idx; }, // keyAt
          [](auto idx) { return (92 + idx) * 0.1; }), // valueAt
  })};

  testAggregations(
      vectors,
      {"c0"},
      {"spark_last(c1)"},
      expected,
      /*config*/ {},
      /*testWithTableScan*/ false);
}

TEST_F(LastAggregateTest, mapGlobal) {
  auto O = [](const std::vector<std::pair<int64_t, std::optional<float>>>& m) {
    return std::make_optional(m);
  };
  auto vectors = {makeRowVector({
      makeNullableMapVector<int64_t, float>(
          {std::nullopt, O({{1, 2.0}}), O({{2, 4.0}}), std::nullopt}),
  })};

  auto ignoreNullData = {makeRowVector({
      makeNullableMapVector<int64_t, float>({O({{2, 4.0}})}),
  })};

  auto hasNullData = {makeRowVector({
      makeNullableMapVector<int64_t, float>({std::nullopt}),
  })};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
