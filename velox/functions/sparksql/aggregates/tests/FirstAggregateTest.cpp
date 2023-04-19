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
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::aggregate::test;

namespace facebook::velox::functions::sparksql::aggregates::test {

namespace {

class FirstAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    aggregates::registerAggregateFunctions("spark_");
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

      {
        SCOPED_TRACE("ignore null + group by");
        testAggregations(
            vectors,
            {"c0"},
            {"spark_first_ignore_null(c1)"},
            "SELECT c0, first(c1 ORDER BY c1 NULLS LAST) FROM tmp GROUP BY c0");
      }
      {
        // Expected result should have first 7 rows including nulls.
        SCOPED_TRACE("not ignore null + group by");
        auto expected = {makeRowVector({
            makeFlatVector<int32_t>(7, [](auto row) { return row; }),
            makeFlatVector<T>(
                7, // size
                [](auto row) { return row; }, // valueAt
                [](auto row) { return row % 3 == 0; }), // nullAt
        })};
        testAggregations(vectors, {"c0"}, {"spark_first(c1)"}, expected);
      }
    }

    {
      auto vectors = {makeRowVector({
          makeNullableFlatVector<T>({std::nullopt, 1, 2, std::nullopt}),
      })};

      {
        SCOPED_TRACE("ignore null + global");
        auto expectedTrue = {makeRowVector({makeNullableFlatVector<T>({1})})};
        testAggregations(
            vectors, {}, {"spark_first_ignore_null(c0)"}, expectedTrue);
      }
      {
        SCOPED_TRACE("not ignore null + global");
        auto expectedFalse = {
            makeRowVector({makeNullableFlatVector<T>({std::nullopt})})};
        testAggregations(vectors, {}, {"spark_first(c0)"}, expectedFalse);
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
          data, {"c0"}, {"spark_first_ignore_null(c1)"}, ignoreNullData);
    }

    {
      SCOPED_TRACE("not ignore null + group by");
      testAggregations(data, {"c0"}, {"spark_first(c1)"}, hasNullData);
    }
  }

  void testGlobalAggregate(
      std::vector<RowVectorPtr> data,
      std::vector<RowVectorPtr> ignoreNullData,
      std::vector<RowVectorPtr> hasNullData) {
    {
      SCOPED_TRACE("ignore null + global");
      testAggregations(
          data, {}, {"spark_first_ignore_null(c0)"}, ignoreNullData);
    }

    {
      SCOPED_TRACE("not ignore null + global");
      testAggregations(data, {}, {"spark_first(c0)"}, hasNullData);
    }
  }
};

TEST_F(FirstAggregateTest, boolean) {
  testAggregate<bool>();
}

TEST_F(FirstAggregateTest, tinyInt) {
  testAggregate<int8_t>();
}

TEST_F(FirstAggregateTest, smallInt) {
  testAggregate<int16_t>();
}

TEST_F(FirstAggregateTest, integer) {
  testAggregate<int32_t>();
}

TEST_F(FirstAggregateTest, bigint) {
  testAggregate<int64_t>();
}

TEST_F(FirstAggregateTest, real) {
  testAggregate<float>();
}

TEST_F(FirstAggregateTest, double) {
  testAggregate<double>();
}

TEST_F(FirstAggregateTest, timestampGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeFlatVector<Timestamp>(
          98, // size
          [](auto row) { return Timestamp(row, row); }, // valueAt
          nullEvery(3)),
  })};

  auto ignoreNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<Timestamp>(
          7, // size
          [](auto row) {
            return row % 3 == 0 ? Timestamp(row + 7, row + 7)
                                : Timestamp(row, row);
          } // valueAt
          ),
  })};

  // Expected result should have first 7 rows including nulls.
  auto hasNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<Timestamp>(
          7, // size
          [](auto row) { return Timestamp(row, row); }, // valueAt
          [](auto row) { return row % 3 == 0; }), // nullAt
  })};

  testGroupBy(vectors, ignoreNullData, hasNullData);
}

TEST_F(FirstAggregateTest, timestampGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<Timestamp>(
          {std::nullopt, Timestamp(1, 1), Timestamp(2, 2), std::nullopt}),
  })};

  auto ignoreNullData = {
      makeRowVector({makeNullableFlatVector<Timestamp>({Timestamp(1, 1)})})};

  auto hasNullData = {
      makeRowVector({makeNullableFlatVector<Timestamp>({std::nullopt})})};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(FirstAggregateTest, dateGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeFlatVector<Date>(
          98, // size
          [](auto row) { return Date(row); }, // valueAt
          nullEvery(3)),
  })};

  auto ignoreNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<Date>(
          7, // size
          [](auto row) {
            return row % 3 == 0 ? Date(row + 7) : Date(row);
          } // valueAt
          ),
  })};

  // Expected result should have first 7 rows including nulls.
  auto hasNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<Date>(
          7, // size
          [](auto row) { return Date(row); }, // valueAt
          [](auto row) { return row % 3 == 0; }), // nullAt
  })};

  testGroupBy(vectors, ignoreNullData, hasNullData);
}

TEST_F(FirstAggregateTest, dateGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<Date>(
          {std::nullopt, Date(1), Date(2), std::nullopt}),
  })};

  auto ignoreNullData = {makeRowVector({makeFlatVector<Date>({Date(1)})})};

  auto hasNullData = {
      makeRowVector({makeNullableFlatVector<Date>({std::nullopt})})};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(FirstAggregateTest, intervalGroupBy) {
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
          [](auto row) { return row % 3 == 0 ? (row + 7) : (row); }, // valueAt
          nullptr, // nullAt
          INTERVAL_DAY_TIME()),
  })};

  // Expected result should have first 7 rows including nulls.
  auto hasNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<int64_t>(
          7, // size
          [](auto row) { return row; }, // valueAt
          nullEvery(3),
          INTERVAL_DAY_TIME()),
  })};

  testGroupBy(vectors, ignoreNullData, hasNullData);
}

TEST_F(FirstAggregateTest, intervalGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<int64_t>(
          {std::nullopt, 1, 2, std::nullopt}, INTERVAL_DAY_TIME()),
  })};

  auto ignoreNullData = {makeRowVector(
      {makeNullableFlatVector<int64_t>({1}, INTERVAL_DAY_TIME())})};

  auto hasNullData = {makeRowVector(
      {makeNullableFlatVector<int64_t>({std::nullopt}, INTERVAL_DAY_TIME())})};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(FirstAggregateTest, varcharGroupBy) {
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

  {
    SCOPED_TRACE("ignore null + group by");
    testAggregations(
        vectors,
        {"c0"},
        {"spark_first_ignore_null(c1)"},
        "SELECT c0, first(c1) FROM tmp WHERE c1 IS NOT NULL GROUP BY c0");
  }

  {
    SCOPED_TRACE("not ignore null + group by");
    auto expected = {makeRowVector({
        makeFlatVector<int32_t>(7, [](auto row) { return row; }),
        makeFlatVector<StringView>(
            7, // size
            [&data](auto row) { return StringView(data[row]); }, // valueAt
            nullEvery(3)),
    })};
    testAggregations(vectors, {"c0"}, {"spark_first(c1)"}, expected);
  }
}

TEST_F(FirstAggregateTest, varcharGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<StringView>(
          {std::nullopt, "a", "b", std::nullopt}),
  })};

  auto ignoreNullData = {makeRowVector({makeFlatVector<StringView>({"a"})})};

  auto hasNullData = {
      makeRowVector({makeNullableFlatVector<std::string>({std::nullopt})})};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(FirstAggregateTest, arrayGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeArrayVector<int64_t>(
          98, // size
          [](auto row) { return row % 3; }, // sizeAt
          [](auto row, auto idx) { return row * 100 + idx; }, // valueAt
          // Even rows are null.
          nullEvery(2)),
  })};

  auto hasNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeArrayVector<int64_t>(
          7,
          [](auto row) {
            // Even rows are null, for these return values for (row + 7)
            return (row % 2) ? row % 3 : (row + 7) % 3;
          },
          [](auto row, auto idx) {
            // Even rows are null, for these return values for (row + 7)
            return (row % 2) ? row * 100 + idx : (row + 7) * 100 + idx;
          }),
  })};

  auto ignoreNullData = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeArrayVector<int64_t>(
          7, // size
          [](auto row) { return row % 3; }, // sizeAt
          [](auto row, auto idx) { return row * 100 + idx; }, // valueAt
          nullEvery(2)),
  })};

  testGroupBy(vectors, hasNullData, ignoreNullData);
}

TEST_F(FirstAggregateTest, arrayGlobal) {
  auto vectors = {makeRowVector({
      makeNullableArrayVector<int64_t>(
          {std::nullopt, {{1, 2}}, {{3, 4}}, std::nullopt}),
  })};

  auto ignoreNullData = {makeRowVector({
      makeArrayVector<int64_t>({{1, 2}}),
  })};

  auto hasNullData = {makeRowVector({
      makeNullableArrayVector<int64_t>({std::nullopt}),
  })};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

TEST_F(FirstAggregateTest, mapGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeMapVector<int64_t, float>(
          98, // size
          [](auto row) { return row % 2 ? 0 : 2; }, // sizeAt
          [](auto idx) { return idx; }, // keyAt
          [](auto idx) { return idx * 0.1; }), // valueAt
  })};

  auto expected = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeMapVector<int64_t, float>(
          7, // size
          [](auto row) { return row % 2 ? 0 : 2; }, // sizeAt
          [](auto idx) { return idx; }, // keyAt
          [](auto idx) { return idx * 0.1; }), // valueAt
  })};

  testAggregations(vectors, {"c0"}, {"spark_first(c1)"}, expected);
}

TEST_F(FirstAggregateTest, mapGlobal) {
  auto O = [](const std::vector<std::pair<int64_t, std::optional<float>>>& m) {
    return std::make_optional(m);
  };
  auto vectors = {makeRowVector({
      makeNullableMapVector<int64_t, float>(
          {std::nullopt, O({{1, 2.0}}), O({{2, 4.0}}), std::nullopt}),
  })};

  auto ignoreNullData = {makeRowVector({
      makeNullableMapVector<int64_t, float>({O({{1, 2.0}})}),
  })};

  auto hasNullData = {makeRowVector({
      makeNullableMapVector<int64_t, float>({std::nullopt}),
  })};

  testGlobalAggregate(vectors, ignoreNullData, hasNullData);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::aggregates::test
