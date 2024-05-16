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
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

class MapAggTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }
};

TEST_F(MapAggTest, groupBy) {
  vector_size_t num = 10;

  auto data = makeRowVector({
      makeFlatVector<int32_t>({0, 0, 0, 1, 1, 1, 2, 2, 2, 3}),
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      makeFlatVector<double>(
          {0.05, 1.05, 2.05, 3.05, 4.05, 5.05, 6.05, 7.05, 8.05, 9.05}),
      makeFlatVector<bool>(
          {true, false, true, false, true, false, true, false, true, false}),
  });

  auto vectors = {data, data, data};

  auto expectedResult = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3}),
      makeMapVector<int32_t, double>({
          {{0, 0.05}, {1, 1.05}, {2, 2.05}},
          {{3, 3.05}, {4, 4.05}, {5, 5.05}},
          {{6, 6.05}, {7, 7.05}, {8, 8.05}},
          {{9, 9.05}},
      }),
  });

  testAggregations(vectors, {"c0"}, {"map_agg(c1, c2)"}, {expectedResult});

  expectedResult = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3}),
      makeMapVectorFromJson<int32_t, double>({
          "{0: 0.05, 2: 2.05}",
          "{4: 4.05}",
          "{6: 6.05, 8: 8.05}",
          "null",
      }),
  });

  testAggregations(
      vectors, {"c0"}, {"map_agg(c1, c2) filter (where c3)"}, {expectedResult});
}

// Verify that null keys are skipped.
TEST_F(MapAggTest, groupByNullKeys) {
  vector_size_t num = 10;

  auto data = makeRowVector({
      makeFlatVector<int32_t>({0, 0, 0, 1, 1, 1, 2, 2, 2, 3}),
      makeNullableFlatVector<int32_t>(
          {std::nullopt, 1, 2, 3, std::nullopt, 5, 6, 7, 8, std::nullopt}),
      makeFlatVector<double>(
          {-1, 1.05, 2.05, 3.05, -1, 5.05, 6.05, 7.05, 8.05, -1}),
  });

  auto vectors = {data, data, data};

  auto expectedResult = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3}),
      makeMapVectorFromJson<int32_t, double>({
          "{1: 1.05, 2: 2.05}",
          "{3: 3.05, 5: 5.05}",
          "{6: 6.05, 7: 7.05, 8: 8.05}",
          "null",
      }),
  });

  testAggregations(vectors, {"c0"}, {"map_agg(c1, c2)"}, {expectedResult});
}

TEST_F(MapAggTest, groupByWithNullValues) {
  vector_size_t size = 90;

  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](vector_size_t row) { return row / 3; }),
      makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; }),
      makeFlatVector<double>(
          size, [](vector_size_t row) { return row + 0.05; }, nullEvery(7)),
  });

  auto vectors = {data, data, data};

  auto expectedResult = {makeRowVector({
      makeFlatVector<int32_t>(30, [](vector_size_t row) { return row; }),
      makeMapVector<int32_t, double>(
          30,
          [](vector_size_t /*row*/) { return 3; },
          [](vector_size_t row) { return row; },
          [](vector_size_t row) { return row + 0.05; },
          nullptr,
          nullEvery(7)),
  })};

  testAggregations(vectors, {"c0"}, {"map_agg(c1, c2)"}, expectedResult);
}

TEST_F(MapAggTest, groupByWithDuplicates) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({0, 0, 1, 1, 2, 2, 3, 3, 4, 4}),
      makeFlatVector<int32_t>({0, 0, 1, 1, 2, 2, 3, 3, 4, 4}),
      makeFlatVector<double>(
          {0.05, 1.05, 2.05, 3.05, 4.05, 5.05, 6.05, 7.05, 8.05, 9.05}),
  });

  auto vectors = {data, data, data};

  auto expectedResult = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
      makeMapVector<int32_t, double>({
          {{0, 0.05}},
          {{1, 2.05}},
          {{2, 4.05}},
          {{3, 6.05}},
          {{4, 8.05}},
      }),
  });

  // We don't test with TableScan because when there are duplicate keys in
  // different splits, the result is non-deterministic.
  testAggregations(vectors, {"c0"}, {"map_agg(c1, c2)"}, {expectedResult});
}

TEST_F(MapAggTest, groupByNoData) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>({}),
       makeFlatVector<int32_t>({}),
       makeFlatVector<int32_t>({})});

  auto vectors = {data, data, data};

  auto expectedResult = {makeRowVector(
      {makeFlatVector<int32_t>({}), makeMapVector<int32_t, double>({})})};

  testAggregations(vectors, {"c0"}, {"map_agg(c1, c2)"}, expectedResult);
}

TEST_F(MapAggTest, global) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      makeFlatVector<double>(
          {0.05, 1.05, 2.05, 3.05, 4.05, 5.05, 6.05, 7.05, 8.05, 9.05}),
      makeFlatVector<bool>(
          {true, false, true, false, true, false, true, false, true, false}),
  });

  auto vectors = {data, data, data};

  auto expectedResult = makeRowVector({
      makeMapVector<int32_t, double>({
          {
              {0, 0.05},
              {1, 1.05},
              {2, 2.05},
              {3, 3.05},
              {4, 4.05},
              {5, 5.05},
              {6, 6.05},
              {7, 7.05},
              {8, 8.05},
              {9, 9.05},
          },
      }),
  });

  testAggregations(vectors, {}, {"map_agg(c0, c1)"}, {expectedResult});

  expectedResult = makeRowVector({
      makeMapVector<int32_t, double>({
          {
              {0, 0.05},
              {2, 2.05},
              {4, 4.05},
              {6, 6.05},
              {8, 8.05},
          },
      }),
  });

  testAggregations(
      vectors, {}, {"map_agg(c0, c1) FILTER (WHERE c2)"}, {expectedResult});
}

// Verify that null keys are skipped.
TEST_F(MapAggTest, globalWithNullKeys) {
  auto data = makeRowVector({
      makeNullableFlatVector<int32_t>({
          0,
          1,
          std::nullopt,
          3,
          std::nullopt,
          5,
          6,
          7,
          std::nullopt,
          std::nullopt,
      }),
      makeFlatVector<double>(
          {0.05, 1.05, -1, 3.05, -1, 5.05, 6.05, 7.05, -1, -1}),
  });

  auto vectors = {data, data, data};

  auto expectedResult = makeRowVector({
      makeMapVector<int32_t, double>({
          {{0, 0.05}, {1, 1.05}, {3, 3.05}, {5, 5.05}, {6, 6.05}, {7, 7.05}},
      }),
  });

  testAggregations(vectors, {}, {"map_agg(c0, c1)"}, {expectedResult});
}

TEST_F(MapAggTest, globalWithNullValues) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      makeNullableFlatVector<double>(
          {std::nullopt,
           1.05,
           2.05,
           3.05,
           4.05,
           5.05,
           6.05,
           std::nullopt,
           8.05,
           9.05}),
  });

  auto vectors = {data, data, data};

  auto expectedResult = makeRowVector({
      makeMapVectorFromJson<int32_t, double>({
          "{0: null, 1: 1.05, 2: 2.05, 3: 3.05, 4: 4.05, 5: 5.05, 6: 6.05, 7: null, 8: 8.05, 9: 9.05}",
      }),
  });

  testAggregations(vectors, {}, {"map_agg(c0, c1)"}, {expectedResult});
}

TEST_F(MapAggTest, globalNoData) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int32_t>({}), makeFlatVector<int32_t>({})})};

  testAggregations(vectors, {}, {"map_agg(c0, c1)"}, "SELECT null");
}

TEST_F(MapAggTest, globalDuplicateKeys) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({0, 0, 1, 1, 2, 2, 3, 3, 4, 4}),
      makeNullableFlatVector<double>({
          std::nullopt,
          1.05,
          2.05,
          3.05,
          4.05,
          5.05,
          6.05,
          std::nullopt,
          8.05,
          9.05,
      }),
  });

  auto vectors = {data, data, data};

  auto expectedResult = makeRowVector({
      makeMapVectorFromJson<int32_t, double>({
          "{0: null, 1: 2.05, 2: 4.05, 3: 6.05, 4: 8.05}",
      }),
  });

  // We don't test with TableScan because when there are duplicate keys in
  // different splits, the result is non-deterministic.
  testAggregations(vectors, {}, {"map_agg(c0, c1)"}, {expectedResult});
}

/// Reproduces the bug reported in
/// https://github.com/facebookincubator/velox/issues/3143
TEST_F(MapAggTest, selectiveMaskWithDuplicates) {
  auto data = makeRowVector({
      // Grouping key with mostly unique values.
      makeFlatVector<int64_t>(
          100, [](auto row) { return row == 91 ? 90 : row; }),
      // Keys. All the same. Grouping key '90' gets a duplicate key.
      makeFlatVector<int64_t>(100, [](auto row) { return 27; }),
      // Values. All unique.
      makeFlatVector<int64_t>(100, [](auto row) { return row; }),
      // Mask.
      makeFlatVector<bool>(100, [](auto row) { return row > 85 && row < 95; }),
  });

  auto nonNullResults = makeMapVector<int64_t, int64_t>({
      {{27, 86}},
      {{27, 87}},
      {{27, 88}},
      {{27, 89}},
      {{27, 90}},
      {{27, 92}},
      {{27, 93}},
      {{27, 94}},
  });

  auto expectedResult = makeRowVector({
      makeFlatVector<int64_t>(
          99, [](auto row) { return row >= 91 ? row + 1 : row; }),
      BaseVector::create(nonNullResults->type(), 99, pool()),
  });
  for (auto row = 0; row < 99; ++row) {
    if (row > 85 && row < 94) {
      expectedResult->childAt(1)->copy(nonNullResults.get(), row, row - 86, 1);
    } else {
      expectedResult->childAt(1)->setNull(row, true);
    }
  }

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({"c0"}, {"map_agg(c1, c2)"}, {"c3"})
                  .planNode();
  assertQuery(plan, {expectedResult});
}

TEST_F(MapAggTest, unknownKey) {
  auto data = makeRowVector({
      makeFlatVector<int8_t>({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}),
      makeAllNullFlatVector<UnknownValue>(10),
      makeConstant<int32_t>(123, 10),
  });

  testAggregations(
      {data}, {"c0"}, {"map_agg(c1, c2)"}, "VALUES (1, NULL), (2, NULL)");

  testAggregations({data}, {}, {"map_agg(c1, c2)"}, "VALUES (NULL)");
}

TEST_F(MapAggTest, stringLifeCycle) {
  vector_size_t num = 10;
  std::vector<std::string> s(num);
  for (int i = 0; i < num; ++i) {
    s[i] = std::string(StringView::kInlineSize + 1, 'a' + i);
  }

  auto data = makeRowVector({
      makeFlatVector<StringView>(num, [&](auto i) { return StringView(s[i]); }),
      makeFlatVector<double>(num, [](auto i) { return i + 0.05; }),
  });

  auto vectors = {data, data, data};

  auto expectedResult = makeRowVector({makeMapVector<StringView, double>(
      1,
      [&](vector_size_t /*row*/) { return num; },
      [&](vector_size_t i) { return StringView(s[i]); },
      [&](vector_size_t i) { return i + 0.05; })});

  testReadFromFiles(vectors, {}, {"map_agg(c0, c1)"}, {expectedResult});
}

TEST_F(MapAggTest, arrayCheckNulls) {
  auto batch = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2]",
          "[6, 7]",
          "[2, 3]",
      }),
      makeFlatVector<int32_t>({
          1,
          2,
          3,
      }),
      makeFlatVector<int32_t>({
          1,
          2,
          3,
      }),
  });

  auto batchWithNull = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2]",
          "[6, 7]",
          "[3, null]",
      }),
      makeFlatVector<int32_t>({
          1,
          2,
          3,
      }),
      makeFlatVector<int32_t>({
          1,
          2,
          3,
      }),
  });

  testFailingAggregations(
      {batch, batchWithNull},
      {},
      {"map_agg(c0, c1)"},
      "ARRAY comparison not supported for values that contain nulls");
  testFailingAggregations(
      {batch, batchWithNull},
      {"c2"},
      {"map_agg(c0, c1)"},
      "ARRAY comparison not supported for values that contain nulls");
}

TEST_F(MapAggTest, rowCheckNull) {
  auto batch = makeRowVector({
      makeRowVector({
          makeFlatVector<StringView>({
              "a"_sv,
              "b"_sv,
              "c"_sv,
          }),
          makeNullableFlatVector<StringView>({
              "aa"_sv,
              "bb"_sv,
              "cc"_sv,
          }),
      }),
      makeFlatVector<int8_t>({1, 2, 3}),
      makeFlatVector<int8_t>({1, 2, 3}),
  });

  auto batchWithNull = makeRowVector({
      makeRowVector({
          makeNullableFlatVector<StringView>({
              "a"_sv,
              std::nullopt,
              "c"_sv,
          }),
          makeFlatVector<StringView>({
              "aa"_sv,
              "bb"_sv,
              "cc"_sv,
          }),
      }),
      makeFlatVector<int8_t>({1, 2, 3}),
      makeFlatVector<int8_t>({1, 2, 3}),
  });

  testFailingAggregations(
      {batch, batchWithNull},
      {},
      {"map_agg(c0, c1)"},
      "ROW comparison not supported for values that contain nulls");
  testFailingAggregations(
      {batch, batchWithNull},
      {"c2"},
      {"map_agg(c0, c1)"},
      "ROW comparison not supported for values that contain nulls");
}

} // namespace
} // namespace facebook::velox::aggregate::test
