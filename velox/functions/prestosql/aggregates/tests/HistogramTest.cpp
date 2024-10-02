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

#include "velox/exec/Aggregate.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

class HistogramTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }

  void testHistogramWithDuck(
      const VectorPtr& vector1,
      const VectorPtr& vector2) {
    ASSERT_EQ(vector1->size(), vector2->size());

    auto num = vector1->size();
    auto reverseIndices = makeIndicesInReverse(num);

    auto vectors = makeRowVector(
        {vector1,
         vector2,
         wrapInDictionary(reverseIndices, num, vector1),
         wrapInDictionary(reverseIndices, num, vector2)});

    createDuckDbTable({vectors});
    testAggregations(
        {vectors},
        {"c0"},
        {"histogram(c1)"},
        "SELECT c0, histogram(c1) FROM tmp GROUP BY c0");

    testAggregations(
        {vectors},
        {"c0"},
        {"histogram(c3)"},
        "SELECT c0, histogram(c3) FROM tmp GROUP BY c0");

    testAggregations(
        {vectors},
        {"c2"},
        {"histogram(c1)"},
        "SELECT c2, histogram(c1) FROM tmp GROUP BY c2");

    testAggregations(
        {vectors},
        {"c2"},
        {"histogram(c3)"},
        "SELECT c2, histogram(c3) FROM tmp GROUP BY c2");
  }

  void testGlobalHistogramWithDuck(const VectorPtr& vector) {
    auto num = vector->size();
    auto reverseIndices = makeIndicesInReverse(num);

    auto vectors =
        makeRowVector({vector, wrapInDictionary(reverseIndices, num, vector)});

    createDuckDbTable({vectors});
    testAggregations(
        {vectors}, {}, {"histogram(c0)"}, "SELECT histogram(c0) FROM tmp");

    testAggregations(
        {vectors}, {}, {"histogram(c1)"}, "SELECT histogram(c1) FROM tmp");
  }

  void testHistogram(
      const std::string& expression,
      const std::vector<std::string>& groupKeys,
      const VectorPtr& vector1,
      const VectorPtr& vector2,
      const RowVectorPtr& expected) {
    auto vectors = makeRowVector({vector1, vector2});
    testAggregations({vectors}, groupKeys, {expression}, {expected});
  }
};

TEST_F(HistogramTest, groupByInteger) {
  vector_size_t num = 37;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 2; }, nullEvery(5));

  testHistogramWithDuck(vector1, vector2);

  // Test when some group-by keys have only null values.
  auto vector3 =
      makeNullableFlatVector<int64_t>({1, 1, 2, 2, 2, 3, 3, std::nullopt});
  auto vector4 = makeNullableFlatVector<int64_t>(
      {10, 11, 20, std::nullopt, 20, std::nullopt, std::nullopt, 40});

  testHistogramWithDuck(vector3, vector4);
}

TEST_F(HistogramTest, groupByDouble) {
  vector_size_t num = 37;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<double>(
      num, [](vector_size_t row) { return row % 2 + 0.05; }, nullEvery(5));

  testHistogramWithDuck(vector1, vector2);
}

TEST_F(HistogramTest, groupByBoolean) {
  vector_size_t num = 37;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<bool>(
      num, [](vector_size_t row) { return row % 5 == 3; }, nullEvery(5));

  testHistogramWithDuck(vector1, vector2);
}

TEST_F(HistogramTest, groupByTimestamp) {
  vector_size_t num = 10;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<Timestamp>(
      num,
      [](vector_size_t row) { return Timestamp{row % 2, 17'123'456}; },
      nullEvery(5));

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 0, 1, 2}),
       makeMapVector<Timestamp, int64_t>(
           {{{Timestamp{0, 17'123'456}, 2}},
            {{Timestamp{0, 17'123'456}, 1}, {Timestamp{1, 17'123'456}, 2}},
            {{Timestamp{1, 17'123'456}, 2}},
            {{Timestamp{0, 17'123'456}, 1}}})});

  testHistogram("histogram(c1)", {"c0"}, vector1, vector2, expected);
}

TEST_F(HistogramTest, groupByDate) {
  vector_size_t num = 10;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 2; }, nullEvery(5), DATE());

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 0, 1, 2}),
       makeMapVector<int32_t, int64_t>(
           {{{{0}, 2}}, {{{0}, 1}, {{1}, 2}}, {{{1}, 2}}, {{{0}, 1}}},
           MAP(DATE(), BIGINT()))});

  testHistogram("histogram(c1)", {"c0"}, vector1, vector2, expected);
}

TEST_F(HistogramTest, groupByInterval) {
  vector_size_t num = 37;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<int64_t>(
      num, [](auto row) { return row; }, nullEvery(5), INTERVAL_DAY_TIME());

  testHistogramWithDuck(vector1, vector2);
}

TEST_F(HistogramTest, groupByString) {
  std::vector<std::string> strings = {
      "grapes",
      "oranges",
      "sweet fruits: apple",
      "sweet fruits: banana",
      "sweet fruits: papaya",
  };

  auto keys = makeFlatVector<int16_t>(
      1'000, [](auto row) { return row % 17; }, nullEvery(19));
  auto data = makeFlatVector<StringView>(
      1'000,
      [&](auto row) { return StringView(strings[row % strings.size()]); },
      nullEvery(11));
  testGlobalHistogramWithDuck(data);
}

TEST_F(HistogramTest, globalInteger) {
  vector_size_t num = 29;
  auto vector = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 5; }, nullEvery(7));

  testGlobalHistogramWithDuck(vector);
}

TEST_F(HistogramTest, globalDouble) {
  vector_size_t num = 29;
  auto vector = makeFlatVector<double>(
      num, [](vector_size_t row) { return row % 5 + 0.05; }, nullEvery(7));

  testGlobalHistogramWithDuck(vector);
}

TEST_F(HistogramTest, globalBoolean) {
  auto vector = makeFlatVector<bool>(
      1'000, [](vector_size_t row) { return row % 5 == 2; }, nullEvery(7));

  testGlobalHistogramWithDuck(vector);
}

TEST_F(HistogramTest, globalTimestamp) {
  vector_size_t num = 10;
  auto vector = makeFlatVector<Timestamp>(
      num,
      [](vector_size_t row) { return Timestamp{row % 4, 100}; },
      nullEvery(7));

  auto expected = makeRowVector({makeMapVector<Timestamp, int64_t>(
      {{{Timestamp{0, 100}, 2},
        {Timestamp{1, 100}, 3},
        {Timestamp{2, 100}, 2},
        {Timestamp{3, 100}, 1}}})});

  testHistogram("histogram(c1)", {}, vector, vector, expected);
}

TEST_F(HistogramTest, globalDate) {
  vector_size_t num = 10;
  auto vector = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 4; }, nullEvery(7), DATE());

  auto expected = makeRowVector({makeMapVector<int32_t, int64_t>(
      {{{{0}, 2}, {{1}, 3}, {{2}, 2}, {{3}, 1}}}, MAP(DATE(), BIGINT()))});

  testHistogram("histogram(c1)", {}, vector, vector, expected);
}

TEST_F(HistogramTest, globalInterval) {
  auto vector = makeFlatVector<int64_t>(
      1'000, [](auto row) { return row; }, nullEvery(7), INTERVAL_DAY_TIME());

  testGlobalHistogramWithDuck(vector);
}

TEST_F(HistogramTest, globalEmpty) {
  auto vector = makeFlatVector<int32_t>({});
  testGlobalHistogramWithDuck(vector);
}

TEST_F(HistogramTest, globalString) {
  std::vector<std::string> strings = {
      "grapes",
      "oranges",
      "sweet fruits: apple",
      "sweet fruits: banana",
      "sweet fruits: papaya",
  };

  auto data = makeFlatVector<StringView>(1'000, [&](auto row) {
    return StringView(strings[row % strings.size()]);
  });
  testGlobalHistogramWithDuck(data);

  // Some nulls.
  data = makeFlatVector<StringView>(
      1'000,
      [&](auto row) { return StringView(strings[row % strings.size()]); },
      nullEvery(7));
  testGlobalHistogramWithDuck(data);

  // All nulls.
  testGlobalHistogramWithDuck(makeAllNullFlatVector<StringView>(1'000));

  // Lots of unique strings.
  std::string scratch;
  data = makeFlatVector<StringView>(
      1'000,
      [&](auto row) {
        scratch = std::string(50 + row, 'A' + (row % 11));
        return StringView(scratch);
      },
      nullEvery(7));
  testGlobalHistogramWithDuck(data);
}

TEST_F(HistogramTest, globalNaNs) {
  // Verify that NaNs with different binary representations are considered equal
  // and deduplicated.
  static const auto kNaN = std::numeric_limits<double>::quiet_NaN();
  static const auto kSNaN = std::numeric_limits<double>::signaling_NaN();
  auto vector = makeFlatVector<double>({1, kNaN, kSNaN, 2, 3, kNaN, kSNaN, 3});

  auto expected = makeRowVector({makeMapVectorFromJson<double, int64_t>({
      "{1: 1, 2: 1, 3: 2, NaN: 4}",
  })});

  testHistogram("histogram(c1)", {}, vector, vector, expected);
}

TEST_F(HistogramTest, arrays) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1}),
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3]",
          "[1, 2]",
          "[]",
          "[1, 2]",
          "[]",
          "[1, null, 2, null]",
          "[1, null, 2, null]",
          "[]",
          "[1, null, 2, null]",
          "null",
          "[1, null, 2, null]",
          "null",
      }),
  });

  auto expected = makeRowVector({
      makeMapVector(
          std::vector<vector_size_t>{0},
          makeArrayVectorFromJson<int32_t>({
              "[1, 2, 3]",
              "[1, 2]",
              "[]",
              "[1, null, 2, null]",
          }),
          makeFlatVector<int64_t>({1, 2, 3, 4})),
  });

  testAggregations({input}, {}, {"histogram(c1)"}, {expected});

  expected = makeRowVector({
      makeMapVector(
          std::vector<vector_size_t>{0},
          makeArrayVectorFromJson<int32_t>({
              "[1, 2, 3]",
              "[1, 2]",
              "[]",
              "[1, null, 2, null]",
          }),
          makeFlatVector<int64_t>({3, 6, 9, 12})),
  });
  testAggregations({input, input, input}, {}, {"histogram(c1)"}, {expected});

  // Group by.
  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1}),
      makeMapVector(
          std::vector<vector_size_t>{0, 3},
          makeArrayVectorFromJson<int32_t>({
              // 1st map.
              "[1, 2, 3]",
              "[]",
              "[1, null, 2, null]",
              // 2nd map.
              "[1, 2]",
              "[]",
              "[1, null, 2, null]",
          }),
          makeFlatVector<int64_t>({1, 2, 3, 2, 1, 1})),
  });
  testAggregations({input}, {"c0"}, {"histogram(c1)"}, {expected});

  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1}),
      makeMapVector(
          std::vector<vector_size_t>{0, 3},
          makeArrayVectorFromJson<int32_t>({
              // 1st map.
              "[1, 2, 3]",
              "[]",
              "[1, null, 2, null]",
              // 2nd map.
              "[1, 2]",
              "[]",
              "[1, null, 2, null]",
          }),
          makeFlatVector<int64_t>({3, 6, 9, 6, 3, 3})),
  });
  testAggregations(
      {input, input, input}, {"c0"}, {"histogram(c1)"}, {expected});
}

TEST_F(HistogramTest, unknown) {
  auto input = makeRowVector({
      makeFlatVector<int32_t>(100, [](auto row) { return row % 2; }),
      makeAllNullFlatVector<UnknownValue>(100),
  });

  auto expected = makeRowVector({
      BaseVector::createNullConstant(MAP(UNKNOWN(), BIGINT()), 1, pool()),
  });

  testAggregations({input}, {}, {"histogram(c1)"}, {expected});

  expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1}),
      BaseVector::createNullConstant(MAP(UNKNOWN(), BIGINT()), 2, pool()),
  });

  testAggregations({input}, {"c0"}, {"histogram(c1)"}, {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::test
