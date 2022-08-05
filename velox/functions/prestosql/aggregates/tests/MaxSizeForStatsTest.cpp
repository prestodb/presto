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

using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class MaxSizeForStatsTest : public AggregationTestBase {
 public:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }
};

TEST_F(MaxSizeForStatsTest, nullValues) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<int8_t>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<int16_t>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<int32_t>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<float>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<double>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<bool>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<Date>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<Timestamp>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}),
  })};

  testAggregations(
      vectors,
      {},
      {"\"max_data_size_for_stats\"(c0)",
       "\"max_data_size_for_stats\"(c1)",
       "\"max_data_size_for_stats\"(c2)",
       "\"max_data_size_for_stats\"(c3)",
       "\"max_data_size_for_stats\"(c4)",
       "\"max_data_size_for_stats\"(c5)",
       "\"max_data_size_for_stats\"(c6)",
       "\"max_data_size_for_stats\"(c7)",
       "\"max_data_size_for_stats\"(c8)",
       "\"max_data_size_for_stats\"(c9)"},
      "SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL");
}

TEST_F(MaxSizeForStatsTest, nullAndNonNullValues) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<int8_t>({std::nullopt, 0}),
      makeNullableFlatVector<int16_t>({std::nullopt, 0}),
      makeNullableFlatVector<int32_t>({std::nullopt, 0}),
      makeNullableFlatVector<int64_t>({std::nullopt, 0}),
      makeNullableFlatVector<float>({std::nullopt, 0}),
      makeNullableFlatVector<double>({std::nullopt, 0}),
      makeNullableFlatVector<bool>({std::nullopt, 0}),
      makeNullableFlatVector<Date>({std::nullopt, 0}),
      makeNullableFlatVector<Timestamp>({std::nullopt, Timestamp(0, 0)}),
      makeNullableFlatVector<StringView>({std::nullopt, "std::nullopt"}),
  })};

  testAggregations(
      vectors,
      {},
      {"\"max_data_size_for_stats\"(c0)",
       "\"max_data_size_for_stats\"(c1)",
       "\"max_data_size_for_stats\"(c2)",
       "\"max_data_size_for_stats\"(c3)",
       "\"max_data_size_for_stats\"(c4)",
       "\"max_data_size_for_stats\"(c5)",
       "\"max_data_size_for_stats\"(c6)",
       "\"max_data_size_for_stats\"(c7)",
       "\"max_data_size_for_stats\"(c8)",
       "\"max_data_size_for_stats\"(c9)"},
      "SELECT 1, 2, 4, 8, 4, 8, 1, 4, 16, 16");
}

template <class T>
T generator(vector_size_t i) {
  return T(i);
}
template <>
Timestamp generator<Timestamp>(vector_size_t i) {
  return Timestamp(i, i);
}
TEST_F(MaxSizeForStatsTest, allScalarTypes) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 1, 2}),
       makeFlatVector<int8_t>(4, generator<int8_t>),
       makeFlatVector<int16_t>(4, generator<int16_t>),
       makeFlatVector<int32_t>(4, generator<int32_t>),
       makeFlatVector<int64_t>(4, generator<int64_t>),
       makeFlatVector<float>(4, generator<float>),
       makeFlatVector<double>(4, generator<double>),
       makeFlatVector<bool>(4, generator<bool>),
       makeFlatVector<Date>(4, generator<Date>),
       makeFlatVector<Timestamp>(4, generator<Timestamp>)})};

  // With grouping keys.
  testAggregations(
      vectors,
      {"c0"},
      {"\"max_data_size_for_stats\"(c1)",
       "\"max_data_size_for_stats\"(c2)",
       "\"max_data_size_for_stats\"(c3)",
       "\"max_data_size_for_stats\"(c4)",
       "\"max_data_size_for_stats\"(c5)",
       "\"max_data_size_for_stats\"(c6)",
       "\"max_data_size_for_stats\"(c7)",
       "\"max_data_size_for_stats\"(c8)",
       "\"max_data_size_for_stats\"(c9)"},
      "VALUES (1,1,2,4,8,4,8,1,4,16),(2,1,2,4,8,4,8,1,4,16)");

  // Without grouping keys.
  testAggregations(
      vectors,
      {},
      {"\"max_data_size_for_stats\"(c1)",
       "\"max_data_size_for_stats\"(c2)",
       "\"max_data_size_for_stats\"(c3)",
       "\"max_data_size_for_stats\"(c4)",
       "\"max_data_size_for_stats\"(c5)",
       "\"max_data_size_for_stats\"(c6)",
       "\"max_data_size_for_stats\"(c7)",
       "\"max_data_size_for_stats\"(c8)",
       "\"max_data_size_for_stats\"(c9)"},
      "VALUES (1,2,4,8,4,8,1,4,16)");
}

TEST_F(MaxSizeForStatsTest, arrayGlobalAggregate) {
  auto vectors = {makeRowVector({makeArrayVector<int64_t>({
      {1, 2, 3, 4, 5},
      {},
      {1, 2, 3},
  })})};
  testAggregations(
      vectors, {}, {"\"max_data_size_for_stats\"(c0)"}, "SELECT 44");
}

TEST_F(MaxSizeForStatsTest, mapGlobalAggregate) {
  auto vectors = {makeRowVector({makeMapVector<int8_t, int32_t>(
      {{{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}},
       {},
       {{1, 1}, {1, 1}, {1, 1}}})})};
  testAggregations(
      vectors, {}, {"\"max_data_size_for_stats\"(c0)"}, "SELECT 29");
}

TEST_F(MaxSizeForStatsTest, rowGlobalAggregate) {
  auto vectors = {makeRowVector({makeRowVector(
      {makeArrayVector<int64_t>({
           {1, 2, 3, 4, 5},
           {},
           {1, 2, 3},
       }),
       makeMapVector<int8_t, int32_t>(
           {{{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}},
            {},
            {{1, 1}, {1, 1}, {1, 1}}})})})};
  testAggregations(
      vectors, {}, {"\"max_data_size_for_stats\"(c0)"}, "SELECT 77");
}

TEST_F(MaxSizeForStatsTest, varbinaryGlobalAggregate) {
  VectorPtr varbinaryVector = BaseVector::create(VARBINARY(), 3, pool());
  auto flatVector = varbinaryVector->asFlatVector<StringView>();
  StringView buf_sv{"buf"};
  StringView _sv{""};
  StringView hello_sv{"hello, world !"};
  flatVector->set(0, buf_sv);
  flatVector->set(1, _sv);
  flatVector->set(2, hello_sv);

  auto vectors = {makeRowVector({varbinaryVector})};
  testAggregations(
      vectors, {}, {"\"max_data_size_for_stats\"(c0)"}, "SELECT 18");
}

TEST_F(MaxSizeForStatsTest, varcharGlobalAggregate) {
  auto vectors = {makeRowVector({makeFlatVector<StringView>({
      "{1, 2, 3, 4, 5}",
      "{}",
      "{1, 2, 3}",
  })})};
  testAggregations(
      vectors, {}, {"\"max_data_size_for_stats\"(c0)"}, "SELECT 19");
}

TEST_F(MaxSizeForStatsTest, complexRecursiveGlobalAggregate) {
  auto vectors = {makeRowVector({makeRowVector(
      {makeFlatVector<StringView>({
           "{1, 2, 3, 4, 5}",
           "{}",
           "{1, 2, 3}",
       }),
       createMapOfArraysVector<int8_t, int64_t>(
           {{{1, std::nullopt}},
            {{2, {{4, 5, std::nullopt}}}},
            {{std::nullopt, {{7, 8, 9}}}}})})})};

  testAggregations(
      vectors, {}, {"\"max_data_size_for_stats\"(c0)"}, "SELECT 50");
}

TEST_F(MaxSizeForStatsTest, constantEncodingTest) {
  auto columnOne = makeFlatVector<int64_t>({1, 2, 1});
  auto columnTwo = makeRowVector(
      {makeFlatVector<StringView>({
           "{1, 2, 3, 4, 5}",
           "{}",
           "{1, 2, 3}",
       }),
       createMapOfArraysVector<int8_t, int64_t>(
           {{{1, std::nullopt}},
            {{2, {{4, 5, std::nullopt}}}},
            {{std::nullopt, {{7, 8, 9}}}}})});
  auto columnTwoConstantEncoded = BaseVector::wrapInConstant(3, 1, columnTwo);

  auto vectors = {makeRowVector({columnOne, columnTwoConstantEncoded})};

  testAggregations(
      vectors, {}, {"\"max_data_size_for_stats\"(c1)"}, "SELECT 36");

  testAggregations(
      vectors,
      {"c0"},
      {"\"max_data_size_for_stats\"(c1)"},
      "VALUES (1,36),(2,36)");
}

TEST_F(MaxSizeForStatsTest, dictionaryEncodingTest) {
  auto columnOne = makeFlatVector<int64_t>({1, 2, 1});
  auto columnTwo = makeRowVector(
      {makeFlatVector<StringView>({
           "{1, 2, 3, 4, 5}",
           "{}",
           "{1, 2, 3}",
       }),
       createMapOfArraysVector<int8_t, int64_t>(
           {{{1, std::nullopt}},
            {{2, {{4, 5, std::nullopt}}}},
            {{std::nullopt, {{7, 8, 9}}}}})});
  vector_size_t size = 3;
  auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; ++i) {
    rawIndices[i] = i;
  }
  auto columnTwoDictionaryEncoded =
      BaseVector::wrapInDictionary(nullptr, indices, size, columnTwo);
  auto vectors = {makeRowVector({columnOne, columnTwoDictionaryEncoded})};

  testAggregations(
      vectors, {}, {"\"max_data_size_for_stats\"(c1)"}, "SELECT 50");

  testAggregations(
      vectors,
      {"c0"},
      {"\"max_data_size_for_stats\"(c1)"},
      "VALUES (1,50),(2,36)");
}

} // namespace
} // namespace facebook::velox::aggregate::test
