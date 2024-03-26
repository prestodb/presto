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

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

class SumDataSizeForStatsTest : public AggregationTestBase {
 public:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }
};

TEST_F(SumDataSizeForStatsTest, nullValues) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<int8_t>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<int16_t>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<int32_t>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<float>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<double>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<bool>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<Timestamp>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}),
  })};

  testAggregations(
      vectors,
      {},
      {"sum_data_size_for_stats(c0)",
       "sum_data_size_for_stats(c1)",
       "sum_data_size_for_stats(c2)",
       "sum_data_size_for_stats(c3)",
       "sum_data_size_for_stats(c4)",
       "sum_data_size_for_stats(c5)",
       "sum_data_size_for_stats(c6)",
       "sum_data_size_for_stats(c7)",
       "sum_data_size_for_stats(c8)"},
      "SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL");
}

TEST_F(SumDataSizeForStatsTest, nullAndNonNullValues) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<int8_t>({std::nullopt, 0}),
      makeNullableFlatVector<int16_t>({std::nullopt, 0}),
      makeNullableFlatVector<int32_t>({std::nullopt, 0}),
      makeNullableFlatVector<int64_t>({std::nullopt, 0}),
      makeNullableFlatVector<float>({std::nullopt, 0}),
      makeNullableFlatVector<double>({std::nullopt, 0}),
      makeNullableFlatVector<bool>({std::nullopt, 0}),
      makeNullableFlatVector<Timestamp>({std::nullopt, Timestamp(0, 0)}),
      makeNullableFlatVector<StringView>({std::nullopt, "std::nullopt"}),
  })};

  testAggregations(
      vectors,
      {},
      {"sum_data_size_for_stats(c0)",
       "sum_data_size_for_stats(c1)",
       "sum_data_size_for_stats(c2)",
       "sum_data_size_for_stats(c3)",
       "sum_data_size_for_stats(c4)",
       "sum_data_size_for_stats(c5)",
       "sum_data_size_for_stats(c6)",
       "sum_data_size_for_stats(c7)",
       "sum_data_size_for_stats(c8)"},
      "SELECT 1, 2, 4, 8, 4, 8, 1, 16, 16");
}

template <class T>
T generator(vector_size_t i) {
  return T(i);
}
template <>
Timestamp generator<Timestamp>(vector_size_t i) {
  return Timestamp(i, i);
}
TEST_F(SumDataSizeForStatsTest, allScalarTypes) {
  // Make input size at least 8 to ensure drivers get 2 input batches for
  // spilling when tested with data read from files.
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 1, 2, 1, 2, 1, 2}),
       makeFlatVector<int8_t>(8, generator<int8_t>),
       makeFlatVector<int16_t>(8, generator<int16_t>),
       makeFlatVector<int32_t>(8, generator<int32_t>),
       makeFlatVector<int64_t>(8, generator<int64_t>),
       makeFlatVector<float>(8, generator<float>),
       makeFlatVector<double>(8, generator<double>),
       makeFlatVector<bool>(8, generator<bool>),
       makeFlatVector<Timestamp>(8, generator<Timestamp>)})};

  // With grouping keys.
  testAggregations(
      vectors,
      {"c0"},
      {"sum_data_size_for_stats(c1)",
       "sum_data_size_for_stats(c2)",
       "sum_data_size_for_stats(c3)",
       "sum_data_size_for_stats(c4)",
       "sum_data_size_for_stats(c5)",
       "sum_data_size_for_stats(c6)",
       "sum_data_size_for_stats(c7)",
       "sum_data_size_for_stats(c8)"},
      // result for group 1 and 2
      "VALUES (1,4,8,16,32,16,32,4,64),(2,4,8,16,32,16,32,4,64)");

  // Without grouping keys.
  testAggregations(
      vectors,
      {},
      {"sum_data_size_for_stats(c1)",
       "sum_data_size_for_stats(c2)",
       "sum_data_size_for_stats(c3)",
       "sum_data_size_for_stats(c4)",
       "sum_data_size_for_stats(c5)",
       "sum_data_size_for_stats(c6)",
       "sum_data_size_for_stats(c7)",
       "sum_data_size_for_stats(c8)"},
      "VALUES (8,16,32,64,32,64,8,128)");
}

TEST_F(SumDataSizeForStatsTest, arrayGlobalAggregate) {
  auto vectors = {makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3, 4, 5},
          {},
          {1, 2, 3},
      }),
  })};
  testAggregations(vectors, {}, {"sum_data_size_for_stats(c0)"}, "SELECT 76");
}

TEST_F(SumDataSizeForStatsTest, mapGlobalAggregate) {
  auto vectors = {makeRowVector({
      makeMapVector<int8_t, int32_t>({
          {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}},
          {},
          {{1, 1}, {1, 1}, {1, 1}},
      }),
  })};
  testAggregations(vectors, {}, {"sum_data_size_for_stats(c0)"}, "SELECT 52");
}

TEST_F(SumDataSizeForStatsTest, rowGlobalAggregate) {
  auto vectors = {makeRowVector({
      makeRowVector({
          makeArrayVector<int64_t>({
              {1, 2, 3, 4, 5},
              {},
              {1, 2, 3},
          }),
          makeMapVector<int8_t, int32_t>({
              {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}},
              {},
              {{1, 1}, {1, 1}, {1, 1}},
          }),
      }),
  })};
  testAggregations(vectors, {}, {"sum_data_size_for_stats(c0)"}, "SELECT 140");
}

TEST_F(SumDataSizeForStatsTest, varbinaryGlobalAggregate) {
  VectorPtr varbinaryVector = BaseVector::create(VARBINARY(), 3, pool());
  auto flatVector = varbinaryVector->asFlatVector<StringView>();
  flatVector->set(0, "buf");
  flatVector->set(1, "");
  flatVector->set(2, "hello, world !");

  auto vectors = {makeRowVector({varbinaryVector})};
  testAggregations(vectors, {}, {"sum_data_size_for_stats(c0)"}, "SELECT 29");
}

TEST_F(SumDataSizeForStatsTest, varcharGlobalAggregate) {
  auto vectors = {makeRowVector({
      makeFlatVector<StringView>({
          "{1, 2, 3, 4, 5}",
          "{}",
          "{1, 2, 3}",
      }),
  })};
  testAggregations(vectors, {}, {"sum_data_size_for_stats(c0)"}, "SELECT 38");
}

TEST_F(SumDataSizeForStatsTest, complexRecursiveGlobalAggregate) {
  auto vectors = {makeRowVector({
      makeRowVector({
          makeFlatVector<StringView>({
              "{1, 2, 3, 4, 5}",
              "{}",
              "{1, 2, 3}",
          }),
          createMapOfArraysVector<int8_t, int64_t>({
              {{1, std::nullopt}},
              {{2, {{4, 5, std::nullopt}}}},
              {{std::nullopt, {{7, 8, 9}}}},
          }),
      }),
  })};

  testAggregations(vectors, {}, {"sum_data_size_for_stats(c0)"}, "SELECT 115");
}

TEST_F(SumDataSizeForStatsTest, constantEncodingTest) {
  auto columnOne = makeFlatVector<int64_t>({1, 2, 1});
  auto columnTwo = makeRowVector({
      makeFlatVector<StringView>({
          "{1, 2, 3, 4, 5}",
          "{}",
          "{1, 2, 3}",
      }),
      createMapOfArraysVector<int8_t, int64_t>({
          {{1, std::nullopt}},
          {{2, {{4, 5, std::nullopt}}}},
          {{std::nullopt, {{7, 8, 9}}}},
      }),
  });
  auto columnTwoConstantEncoded = BaseVector::wrapInConstant(3, 1, columnTwo);

  auto vectors = {makeRowVector({columnOne, columnTwoConstantEncoded})};

  testAggregations(
      vectors, {}, {"sum_data_size_for_stats(c1)"}, "VALUES (108)");

  testAggregations(
      vectors, {"c0"}, {"sum_data_size_for_stats(c1)"}, "VALUES (1,72),(2,36)");
}

TEST_F(SumDataSizeForStatsTest, dictionaryEncodingTest) {
  auto columnOne = makeFlatVector<int64_t>({1, 2, 1});
  auto columnTwo = makeRowVector({
      makeFlatVector<StringView>({
          "{1, 2, 3, 4, 5}",
          "{}",
          "{1, 2, 3}",
      }),
      createMapOfArraysVector<int8_t, int64_t>({
          {{1, std::nullopt}},
          {{2, {{4, 5, std::nullopt}}}},
          {{std::nullopt, {{7, 8, 9}}}},
      }),
  });
  vector_size_t size = 3;
  auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; ++i) {
    rawIndices[i] = i;
  }
  auto columnTwoDictionaryEncoded =
      BaseVector::wrapInDictionary(nullptr, indices, size, columnTwo);
  auto vectors = {makeRowVector({columnOne, columnTwoDictionaryEncoded})};

  testAggregations(vectors, {}, {"sum_data_size_for_stats(c1)"}, "SELECT 115");

  testAggregations(
      vectors, {"c0"}, {"sum_data_size_for_stats(c1)"}, "VALUES (1,79),(2,36)");
}

TEST_F(SumDataSizeForStatsTest, mask) {
  auto data = makeRowVector(
      {"m", "v"},
      {
          makeFlatVector<bool>({false, false, true, true}),
          makeNullableArrayVector<int64_t>({
              std::nullopt,
              std::nullopt,
              {{1, 2, 3, 4}},
              {{1, 2, 3, 4, 5}},
          }),
      });

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"sum_data_size_for_stats(v)"}, {"m"})
                  .planNode();
  assertQuery(plan, "SELECT 80");
}

} // namespace
} // namespace facebook::velox::aggregate::test
