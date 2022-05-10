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

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class HistogramTest : public AggregationTestBase {
 protected:
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

TEST_F(HistogramTest, groupByTimestamp) {
  vector_size_t num = 10;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<Timestamp>(
      num,
      [](vector_size_t row) {
        return Timestamp{row % 2, 100};
      },
      nullEvery(5));

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 0, 1, 2}),
       makeMapVector<Timestamp, int64_t>(
           {{{Timestamp{0, 100}, 2}},
            {{Timestamp{0, 100}, 1}, {Timestamp{1, 100}, 2}},
            {{Timestamp{1, 100}, 2}},
            {{Timestamp{0, 100}, 1}}})});

  testHistogram("histogram(c1)", {"c0"}, vector1, vector2, expected);
}

TEST_F(HistogramTest, groupByDate) {
  vector_size_t num = 10;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<Date>(
      num, [](vector_size_t row) { return Date{row % 2}; }, nullEvery(5));

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 0, 1, 2}),
       makeMapVector<Date, int64_t>(
           {{{Date{0}, 2}},
            {{Date{0}, 1}, {Date{1}, 2}},
            {{Date{1}, 2}},
            {{Date{0}, 1}}})});

  testHistogram("histogram(c1)", {"c0"}, vector1, vector2, expected);
}

TEST_F(HistogramTest, globalGroupByInteger) {
  vector_size_t num = 29;
  auto vector = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 5; }, nullEvery(7));

  testGlobalHistogramWithDuck(vector);
}

TEST_F(HistogramTest, globalGroupByDouble) {
  vector_size_t num = 29;
  auto vector = makeFlatVector<double>(
      num, [](vector_size_t row) { return row % 5 + 0.05; }, nullEvery(7));

  testGlobalHistogramWithDuck(vector);
}

TEST_F(HistogramTest, globalGroupByTimestamp) {
  vector_size_t num = 10;
  auto vector = makeFlatVector<Timestamp>(
      num,
      [](vector_size_t row) {
        return Timestamp{row % 4, 100};
      },
      nullEvery(7));

  auto expected = makeRowVector({makeMapVector<Timestamp, int64_t>(
      {{{Timestamp{0, 100}, 2},
        {Timestamp{1, 100}, 3},
        {Timestamp{2, 100}, 2},
        {Timestamp{3, 100}, 1}}})});

  testHistogram("histogram(c1)", {}, vector, vector, expected);
}

TEST_F(HistogramTest, globalGroupByDate) {
  vector_size_t num = 10;
  auto vector = makeFlatVector<Date>(
      num, [](vector_size_t row) { return Date{row % 4}; }, nullEvery(7));

  auto expected = makeRowVector({makeMapVector<Date, int64_t>(
      {{{Date{0}, 2}, {Date{1}, 3}, {Date{2}, 2}, {Date{3}, 1}}})});

  testHistogram("histogram(c1)", {}, vector, vector, expected);
}

TEST_F(HistogramTest, globalGroupByEmpty) {
  auto vector = makeFlatVector<int32_t>({});
  testGlobalHistogramWithDuck(vector);
}

} // namespace
} // namespace facebook::velox::aggregate::test
