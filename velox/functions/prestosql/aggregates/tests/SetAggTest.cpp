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
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::functions::aggregate::test;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

constexpr int64_t kLongMax = std::numeric_limits<int64_t>::max();
constexpr int64_t kLongMin = std::numeric_limits<int64_t>::min();
constexpr int128_t kHugeMax = std::numeric_limits<int128_t>::max();
constexpr int128_t kHugeMin = std::numeric_limits<int128_t>::min();

class SetAggTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }
};

TEST_F(SetAggTest, global) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 3, 4, 2, 6, 7}),
  });

  auto expected = makeRowVector({
      makeArrayVector<int32_t>({
          {1, 2, 3, 4, 5, 6, 7},
      }),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Null inputs.
  data = makeRowVector({
      makeNullableFlatVector<int32_t>(
          {1, 2, std::nullopt, 4, 5, std::nullopt, 4, 2, 6, 7}),
  });

  expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[1, 2, 4, 5, 6, 7, null]"}),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // All inputs are null.
  data = makeRowVector({
      makeAllNullFlatVector<int32_t>(1'000),
  });

  expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[null]"}),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});
}

TEST_F(SetAggTest, groupBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 3, 4, 2, 6, 7}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<int32_t>({
          {1, 2, 3, 7},
          {3, 4, 5, 6},
      }),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"set_agg(c1)"},
      {"c0", "array_sort(a0)"},
      {expected});

  // Null inputs.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeNullableFlatVector<int32_t>(
          {1,
           std::nullopt,
           3,
           std::nullopt,
           5,
           std::nullopt,
           3,
           std::nullopt,
           6,
           1}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVectorFromJson<int32_t>({
          "[1, null]",
          "[3, 5, 6, null]",
      }),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"set_agg(c1)"},
      {"c0", "array_sort(a0)"},
      {expected});

  // All inputs are null for a group.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeNullableFlatVector<int32_t>(
          {1,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           1}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVectorFromJson<int32_t>({
          "[1, null]",
          "[null]",
      }),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"set_agg(c1)"},
      {"c0", "array_sort(a0)"},
      {expected});
}

TEST_F(SetAggTest, shortDecimal) {
  // Test with short decimal
  auto type = DECIMAL(6, 2);

  auto data = makeRowVector({
      makeFlatVector<int64_t>(
          {kLongMin,
           2000,
           3000,
           -4321,
           kLongMax,
           5000,
           3000,
           kLongMax,
           -2000,
           6000,
           7000},
          type),
  });

  auto expected = makeRowVector({
      makeArrayVector<int64_t>(
          {
              {kLongMin, -4321, -2000, 2000, 3000, 5000, 6000, 7000, kLongMax},
          },
          type),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Test with some NULL inputs (short decimals)
  data = makeRowVector({
      makeNullableFlatVector<int64_t>(
          {1000,
           std::nullopt,
           kLongMin,
           4000,
           std::nullopt,
           4000,
           std::nullopt,
           -1000,
           5000,
           -9999,
           kLongMax},
          type),
  });

  expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<int64_t>>>{
              {kLongMin,
               -9999,
               -1000,
               1000,
               4000,
               5000,
               kLongMax,
               std::nullopt}},
          ARRAY(type)),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Test with all NULL inputs (short decimals)
  data = makeRowVector({
      makeNullableFlatVector<int64_t>(
          {std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt},
          type),
  });

  expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<int64_t>>>{{std::nullopt}},
          ARRAY(type)),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});
}

TEST_F(SetAggTest, longDecimal) {
  // Test with long decimal
  auto type = DECIMAL(20, 2);

  auto data = makeRowVector({
      makeFlatVector<int128_t>(
          {kHugeMin,
           -2000,
           3000,
           4000,
           5000,
           kHugeMax,
           -9630,
           2000,
           6000,
           7000},
          type),
  });

  auto expected = makeRowVector({
      makeArrayVector<int128_t>(
          {
              {kHugeMin,
               -9630,
               -2000,
               2000,
               3000,
               4000,
               5000,
               6000,
               7000,
               kHugeMax},
          },
          type),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Test with some NULL inputs (long decimals)
  data = makeRowVector({
      makeNullableFlatVector<int128_t>(
          {1000,
           std::nullopt,
           3000,
           4000,
           std::nullopt,
           kHugeMax,
           -8424,
           4000,
           std::nullopt,
           -1000,
           5000,
           kHugeMin,
           2000},
          type),
  });

  expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<int128_t>>>{
              {kHugeMin,
               -8424,
               -1000,
               1000,
               2000,
               3000,
               4000,
               5000,
               kHugeMax,
               std::nullopt}},
          ARRAY(type)),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Test with all NULL inputs (long decimals)
  data = makeRowVector({
      makeNullableFlatVector<int128_t>(
          {std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt},
          type),
  });

  expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<int128_t>>>{{std::nullopt}},
          ARRAY(type)),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});
}

std::vector<std::optional<std::string>> generateStrings(
    const std::vector<std::optional<std::string>>& choices,
    vector_size_t size) {
  std::vector<int> indices(size);
  std::iota(indices.begin(), indices.end(), 0); // 0, 1, 2, 3, ...

  std::mt19937 g(std::random_device{}());
  std::shuffle(indices.begin(), indices.end(), g);

  std::vector<std::optional<std::string>> strings(size);
  for (auto i = 0; i < size; ++i) {
    strings[i] = choices[indices[i] % choices.size()];
  }
  return strings;
}

TEST_F(SetAggTest, globalVarchar) {
  std::vector<std::optional<std::string>> strings = {
      "grapes",
      "oranges",
      "sweet fruits: apple",
      "sweet fruits: banana",
      "sweet fruits: papaya",
  };
  auto stringVector = makeNullableFlatVector(generateStrings(strings, 25));
  std::vector<RowVectorPtr> data = {makeRowVector({stringVector})};

  auto expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<std::string>>>{strings}),
  });
  testAggregations(data, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  VectorFuzzer::Options options;
  VectorFuzzer fuzzer(options, pool());
  data = {
      makeRowVector({fuzzer.fuzzDictionary(stringVector, 1'000)}),
      makeRowVector({fuzzer.fuzzDictionary(stringVector, 1'000)}),
  };

  testAggregations(data, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Some nulls.
  auto stringsAndNull = strings;
  stringsAndNull.push_back(std::nullopt);

  auto stringVectorWithNulls =
      makeNullableFlatVector(generateStrings(stringsAndNull, 25));

  data = {makeRowVector({stringVectorWithNulls})};

  expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<std::string>>>{stringsAndNull}),
  });

  testAggregations(data, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  data = {
      makeRowVector({fuzzer.fuzzDictionary(stringVector, 1'000)}),
      makeRowVector({fuzzer.fuzzDictionary(stringVectorWithNulls, 1'000)}),
  };

  testAggregations(data, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // All inputs are null.
  data = {makeRowVector({makeAllNullFlatVector<StringView>(1'000)})};
  expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<StringView>>>{
              {std::nullopt},
          }),
  });
  testAggregations(data, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});
}

TEST_F(SetAggTest, groupByVarchar) {
  std::vector<std::string> strings = {
      "grapes",
      "oranges",
      "sweet fruits: apple",
      "sweet fruits: banana",
      "sweet fruits: papaya",
  };

  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2}),
      makeFlatVector<std::string>({
          strings[0],
          strings[1],
          strings[2],
          strings[3],
          strings[4],
          strings[0],
          strings[3],
      }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<std::string>({
          {strings[0], strings[1]},
          {strings[2], strings[3], strings[4]},
      }),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"set_agg(c1)"},
      {"c0", "array_sort(a0)"},
      {expected});
}

TEST_F(SetAggTest, globalArray) {
  auto data = makeRowVector({
      makeArrayVector<int32_t>({
          {1, 2, 3},
          {4, 5},
          {1, 2, 3},
          {3, 4, 2, 6, 7},
          {1, 2, 3},
          {4, 5},
      }),
  });

  auto expected = makeRowVector({
      makeArrayVector(
          {0},
          makeArrayVector<int32_t>({
              {1, 2, 3},
              {3, 4, 2, 6, 7},
              {4, 5},
          })),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});
}

TEST_F(SetAggTest, groupByArray) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 1, 2, 2, 1}),
      makeArrayVector<int32_t>({
          {1, 2, 3},
          {4, 5},
          {1, 2, 3},
          {3, 4, 2, 6, 7},
          {1, 2, 3},
          {4, 5},
      }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector(
          {0, 2},
          makeArrayVector<int32_t>({
              // First array(array).
              {1, 2, 3},
              {4, 5},
              // Second array(array).
              {1, 2, 3},
              {3, 4, 2, 6, 7},
          })),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"set_agg(c1)"},
      {"c0", "array_sort(a0)"},
      {expected});
}

TEST_F(SetAggTest, arrayCheckNulls) {
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
  });

  testFailingAggregations(
      {batch, batchWithNull},
      {},
      {"set_agg(c0)"},
      "ARRAY comparison not supported for values that contain nulls");
  testFailingAggregations(
      {batch, batchWithNull},
      {"c1"},
      {"set_agg(c0)"},
      "ARRAY comparison not supported for values that contain nulls");
}

TEST_F(SetAggTest, rowCheckNull) {
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
  });

  auto batchWithNull = makeRowVector({
      makeRowVector({
          makeFlatVector<StringView>({
              "a"_sv,
              "b"_sv,
              "c"_sv,
          }),
          makeNullableFlatVector<StringView>({
              "aa"_sv,
              std::nullopt,
              "cc"_sv,
          }),
      }),
      makeFlatVector<int8_t>({1, 2, 3}),
  });

  testFailingAggregations(
      {batch, batchWithNull},
      {},
      {"set_agg(c0)"},
      "ROW comparison not supported for values that contain nulls");
  testFailingAggregations(
      {batch, batchWithNull},
      {"c1"},
      {"set_agg(c0)"},
      "ROW comparison not supported for values that contain nulls");
}

TEST_F(SetAggTest, inputOrder) {
  // Presto preserves order of input.

  auto testInputOrder = [&](const RowVectorPtr& data,
                            const RowVectorPtr& expected) {
    auto plan = PlanBuilder()
                    .values({data})
                    .singleAggregation({}, {"set_agg(c0)"})
                    .planNode();
    assertQuery(plan, expected);
  };

  // Integers.

  auto data = makeRowVector({
      makeNullableFlatVector<int32_t>(
          {1, 2, 3, std::nullopt, 3, 4, 4, 5, 6, 7, std::nullopt}),
  });

  auto expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[1, 2, 3, null, 4, 5, 6, 7]"}),
  });

  testInputOrder(data, expected);

  // Strings.
  data = makeRowVector({
      makeNullableFlatVector<StringView>(
          {"abc",
           "bxy",
           "cde",
           "abc",
           "bxy",
           "cdef",
           "hijk",
           std::nullopt,
           "abc",
           "some very long string to test long strings"}),
  });

  expected = makeRowVector({
      makeNullableArrayVector<StringView>({
          {"abc",
           "bxy",
           "cde",
           "cdef",
           "hijk",
           std::nullopt,
           "some very long string to test long strings"},
      }),
  });

  testInputOrder(data, expected);

  // Complex types.

  data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2]",
          "[5, 6]",
          "null",
          "[3, 4]",
          "[1, 2]",
          "[7, 8]",
      }),
  });

  expected = makeRowVector({
      makeNestedArrayVectorFromJson<int32_t>(
          {"[[1,2], [5, 6], null, [3,4], [7, 8]]"}),
  });

  testInputOrder(data, expected);

  // Group by
  data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 2, 1, 1, 1, 2, 2, 1, 2, 1}),
      makeNullableFlatVector<int32_t>(
          {1, 2, 3, std::nullopt, 3, 4, 4, 5, 6, 7, std::nullopt}),
  });

  expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeArrayVectorFromJson<int32_t>({
          "[1, null, 3, 4, 6]",
          "[2, 3, 4, 5, 7]",
      }),
  });

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({"c0"}, {"set_agg(c1)"})
                  .planNode();

  assertQuery(plan, expected);
}

TEST_F(SetAggTest, nans) {
  // Verify that NaNs with different binary representations are considered equal
  // and deduplicated.
  static const auto kNaN = std::numeric_limits<double>::quiet_NaN();
  static const auto kSNaN = std::numeric_limits<double>::signaling_NaN();

  // Global aggregation, Primitive type.
  auto data = makeRowVector({
      makeFlatVector<double>({1, 2, kNaN, 3, 4, kSNaN, 2, 4, kNaN}),
      makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 2, 2, 2}),
  });

  auto expected = makeRowVector({
      makeArrayVector<double>({
          {1, 2, 3, 4, kNaN},
      }),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Group by aggregation, Primitive type.
  expected = makeRowVector({
      makeArrayVector<double>({
          {1, 2, kNaN},
          {2, 3, 4, kNaN},
      }),
      makeFlatVector<int32_t>({1, 2}),
  });

  testAggregations(
      {data}, {"c1"}, {"set_agg(c0)"}, {"array_sort(a0)", "c1"}, {expected});

  // Global aggregation, Complex type key (ROW(String, Double)).
  // Input Type:  Row(Row(string, double), int)
  data = makeRowVector({
      makeRowVector({
          makeFlatVector<StringView>({
              "a"_sv,
              "b"_sv,
              "c"_sv,
              "a"_sv,
              "b"_sv,
              "c"_sv,
              "a"_sv,
              "b"_sv,
              "c"_sv,
          }),
          makeFlatVector<double>({1, 2, kNaN, 1, 2, kSNaN, 1, 2, kNaN}),
      }),
      makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 2, 2, 2}),
  });

  // Output Type:  Row(Array(Row(string, double)), int)
  expected = makeRowVector({makeArrayVector(
      {0},
      makeRowVector(
          {makeFlatVector<StringView>({
               "a"_sv,
               "b"_sv,
               "c"_sv,
           }),
           makeFlatVector<double>({1, 2, kNaN})}))});

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Group by aggregation, Complex type.
  // Ouput Type:  Row(Array(Row(string, double)), int)
  expected = makeRowVector({
      makeArrayVector(
          {0, 3},
          makeRowVector(
              {makeFlatVector<StringView>({
                   "a"_sv,
                   "b"_sv,
                   "c"_sv,
                   "a"_sv,
                   "b"_sv,
                   "c"_sv,
               }),
               makeFlatVector<double>({1, 2, kNaN, 1, 2, kNaN})})),
      makeFlatVector<int32_t>({1, 2}),
  });

  testAggregations(
      {data}, {"c1"}, {"set_agg(c0)"}, {"array_sort(a0)", "c1"}, {expected});
}

TEST_F(SetAggTest, TimestampWithTimezone) {
  // Global aggregation, Primitive type.
  auto data = makeRowVector({
      makeFlatVector<int64_t>(
          {pack(0, 0),
           pack(1, 0),
           pack(2, 0),
           pack(0, 1),
           pack(1, 1),
           pack(1, 2),
           pack(2, 2),
           pack(3, 3)},
          TIMESTAMP_WITH_TIME_ZONE()),
      makeFlatVector<int32_t>({1, 1, 2, 1, 2, 1, 2, 1}),
  });

  auto expected = makeRowVector({makeArrayVector<int64_t>(
      {{pack(0, 0), pack(1, 0), pack(2, 0), pack(3, 3)}},
      TIMESTAMP_WITH_TIME_ZONE())});

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Group by aggregation, Primitive type.
  expected = makeRowVector({
      makeArrayVector<int64_t>(
          {{pack(0, 0), pack(1, 0), pack(3, 3)}, {pack(1, 1), pack(2, 0)}},
          TIMESTAMP_WITH_TIME_ZONE()),
      makeFlatVector<int32_t>({1, 2}),
  });

  testAggregations(
      {data}, {"c1"}, {"set_agg(c0)"}, {"array_sort(a0)", "c1"}, {expected});

  // Global aggregation, wrapped in complex type.
  data = makeRowVector({
      makeRowVector({makeFlatVector<int64_t>(
          {pack(0, 0),
           pack(1, 0),
           pack(2, 0),
           pack(0, 1),
           pack(1, 1),
           pack(1, 2),
           pack(2, 2),
           pack(3, 3)},
          TIMESTAMP_WITH_TIME_ZONE())}),
      makeFlatVector<int32_t>({1, 1, 2, 1, 2, 1, 2, 1}),
  });

  // Output Type:  Row(Array(Row(string, double)), int)
  expected = makeRowVector({makeArrayVector(
      {0},
      makeRowVector({makeFlatVector<int64_t>(
          {pack(0, 0), pack(1, 0), pack(2, 0), pack(3, 3)},
          TIMESTAMP_WITH_TIME_ZONE())}))});

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Group by aggregation, wrapped in complex type.
  expected = makeRowVector({
      makeArrayVector(
          {0, 3},
          makeRowVector({makeFlatVector<int64_t>(
              {pack(0, 0), pack(1, 0), pack(3, 3), pack(1, 1), pack(2, 0)},
              TIMESTAMP_WITH_TIME_ZONE())})),
      makeFlatVector<int32_t>({1, 2}),
  });

  testAggregations(
      {data}, {"c1"}, {"set_agg(c0)"}, {"array_sort(a0)", "c1"}, {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::test
