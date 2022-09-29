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
#include <boost/algorithm/string/join.hpp>
#include "velox/common/encode/Base64.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

class ChecksumAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  template <typename T>
  void assertSingleGroupChecksum(
      const std::vector<std::optional<T>>& data,
      const std::string& checksum) {
    auto inputVector = makeNullableFlatVector<T>(data);
    assertChecksum(inputVector, checksum);
  }

  void assertChecksum(
      VectorPtr inputVector,
      const std::string& expectedChecksum) {
    auto rowVectors = std::vector{makeRowVector({inputVector})};

    // DuckDB doesn't have checksum aggregation, so we will just pass in
    // expected values to compare.
    const auto expectedDuckDbSql =
        fmt::format("VALUES (CAST(\'{}\' AS VARCHAR))", expectedChecksum);

    testAggregations(
        rowVectors, {}, {"checksum(c0)"}, {"to_base64(a0)"}, expectedDuckDbSql);
  }

  template <typename G, typename T>
  void assertGroupingChecksum(
      const std::vector<std::optional<G>>& groups,
      const std::vector<std::optional<T>>& data,
      const std::vector<std::string>& expectedChecksums) {
    auto groupVector = makeNullableFlatVector<G>(groups);
    auto dataVector = makeNullableFlatVector<T>(data);
    auto rowVectors = std::vector{makeRowVector({groupVector, dataVector})};

    std::vector<std::string> expectedResults;
    expectedResults.reserve(expectedChecksums.size());
    for (const auto& checksum : expectedChecksums) {
      expectedResults.push_back(fmt::format("(\'{}\')", checksum));
    }

    const auto expectedDuckDbSql =
        "VALUES " + boost::algorithm::join(expectedResults, ",");

    testAggregations(
        rowVectors,
        {"c0"},
        {"checksum(c1)"},
        {"to_base64(a0)"},
        expectedDuckDbSql);
  }

  template <typename T>
  void testIntegrals() {
    assertSingleGroupChecksum<T>({1}, "vmaSXOnPGBc=");
    assertSingleGroupChecksum<T>({0}, "AAAAAAAAAAA=");
    assertSingleGroupChecksum<T>({{}}, "h8rrhbF5N54=");
    assertSingleGroupChecksum<T>({1, 2, 3}, "1g6VH0bvnP4=");

    // Test grouping aggregation.
    assertGroupingChecksum<int8_t, T>(
        {'a', 'b', 'a'}, {1, 2, 3}, {"Ke5cLMBy4qc=", "rSA484V8ulY="});

    assertGroupingChecksum<int8_t, T>(
        {'a', 'b', 'a', 'a'}, {1, 2, 3, {}}, {"sLhIsnHsGUY=", "rSA484V8ulY="});
  }
};

TEST_F(ChecksumAggregateTest, longs) {
  testIntegrals<int64_t>();
}

TEST_F(ChecksumAggregateTest, ints) {
  testIntegrals<int32_t>();
}

TEST_F(ChecksumAggregateTest, smallints) {
  testIntegrals<int16_t>();
}

TEST_F(ChecksumAggregateTest, tinyints) {
  testIntegrals<int8_t>();
}

TEST_F(ChecksumAggregateTest, doubles) {
  assertSingleGroupChecksum<double>({1}, "AAAIJ+Q63dI=");
  assertSingleGroupChecksum<double>({{}}, "h8rrhbF5N54=");
  assertSingleGroupChecksum<double>({99.9}, "iVY+6I1lKyo=");
  assertSingleGroupChecksum<double>({1, 2, 3}, "AACEg9cR14o=");

  assertGroupingChecksum<int8_t, double>(
      {'a', 'b', 'a'}, {1, 2, 3}, {"AACEI6XSDyU=", "AAAAYDI/x2U="});

  assertGroupingChecksum<int8_t, double>(
      {'a', 'b', 'a', 'a'}, {1, 2, 3, {}}, {"AAAAYDI/x2U=", "h8pvqVZMR8M="});
}

TEST_F(ChecksumAggregateTest, reals) {
  assertSingleGroupChecksum<float>({1}, "/23UDiDdm9A=");
  assertSingleGroupChecksum<float>({{}}, "h8rrhbF5N54=");
  assertSingleGroupChecksum<float>({99.9}, "IX/UyPhj6MY=");
  assertSingleGroupChecksum<float>({1, 2, 3}, "b/j7Q4YtV+g=");

  assertGroupingChecksum<int8_t, float>(
      {'a', 'b', 'a'}, {1, 2, 3}, {"Vswv9sY4wxY=", "GSzMTb/0k9E="});

  assertGroupingChecksum<int8_t, float>(
      {'a', 'b', 'a', 'a'}, {1, 2, 3, {}}, {"3ZYbfHiy+rQ=", "GSzMTb/0k9E="});
}

TEST_F(ChecksumAggregateTest, dates) {
  assertSingleGroupChecksum<Date>({Date(0)}, "AAAAAAAAAAA=");
  assertSingleGroupChecksum<Date>({Date(1)}, "vmaSXOnPGBc=");
  assertSingleGroupChecksum<Date>({{}}, "h8rrhbF5N54=");
}

TEST_F(ChecksumAggregateTest, timestamps) {
  assertSingleGroupChecksum<Timestamp>({Timestamp(0, 0)}, "AAAAAAAAAAA=");
  assertSingleGroupChecksum<Timestamp>({Timestamp(1000, 0)}, "RPn4MJ+k+O4=");
  assertSingleGroupChecksum<Timestamp>({{}}, "h8rrhbF5N54=");
}

TEST_F(ChecksumAggregateTest, bools) {
  assertSingleGroupChecksum<bool>({true}, "Kd/S+KIswsw=");
  assertSingleGroupChecksum<bool>({false}, "U55ZHMwGD4I=");
}

TEST_F(ChecksumAggregateTest, varchars) {
  assertSingleGroupChecksum<StringView>({{}}, "h8rrhbF5N54=");
  assertSingleGroupChecksum<StringView>({"abcd"_sv}, "lGFxgnIYgPw=");
  assertSingleGroupChecksum<StringView>(
      {"Thanks \u0020\u007F"_sv}, "oEh7YyEV+dM=");
}

TEST_F(ChecksumAggregateTest, arrays) {
  auto arrayVector = makeArrayVector<int64_t>({
      {1, 2},
      {3, 4},
  });
  assertChecksum(arrayVector, "/jjpuD6xkXs=");

  arrayVector = makeNullableArrayVector<int64_t>({{12, std::nullopt}});
  assertChecksum(arrayVector, "sr3HNuzc+7Y=");

  arrayVector = makeNullableArrayVector<int64_t>({{{1, 2}}, std::nullopt});
  assertChecksum(arrayVector, "Nlzernkj88A=");

  arrayVector =
      makeNullableArrayVector<int64_t>({{{1, 2}}, std::nullopt, {{}}});
  assertChecksum(arrayVector, "Nlzernkj88A=");

  // Array of arrays.
  auto baseArrayVector =
      makeNullableArrayVector<int64_t>({{1, 2}, {3, 4}, {4, std::nullopt}, {}});
  auto arrayOfArrayVector = makeArrayVector({0, 2}, baseArrayVector);
  assertChecksum(arrayOfArrayVector, "Wp67EOfWZPA=");
}

TEST_F(ChecksumAggregateTest, maps) {
  auto mapVector = makeMapVector<int64_t, double>(
      {{{1, 17.0}, {2, 36.0}, {3, 8.0}, {4, 28.0}, {5, 24.0}, {6, 32.0}}});

  assertChecksum(mapVector, "T9pb6QUB4xM=");

  auto mapOfArrays = createMapOfArraysVector<int64_t, int64_t>(
      {{{1, {{1, 2, 3}}}}, {{2, {{4, 5, 6}}}}, {{3, {{7, 8, 9}}}}});

  assertChecksum(mapOfArrays, "GGEqhJQZMa4=");

  // Map with nulls.
  auto mapWithNullArrays = createMapOfArraysVector<int64_t, int64_t>(
      {{{1, std::nullopt}}, {{2, {{4, 5, std::nullopt}}}}, {{3, {{7, 8, 9}}}}});

  assertChecksum(mapWithNullArrays, "gwfQ1dI2P68=");
}

TEST_F(ChecksumAggregateTest, rows) {
  auto row = makeRowVector(
      {makeFlatVector<int64_t>({1, 3}), makeFlatVector<int64_t>({2, 4})});

  assertChecksum(row, "jMIvLQ5YEVg=");

  row = makeRowVector(
      {makeNullableFlatVector<int64_t>({1, std::nullopt}),
       makeNullableFlatVector<int64_t>({std::nullopt, 4})});

  assertChecksum(row, "6jtxEIUj7Hg=");
}

TEST_F(ChecksumAggregateTest, globalAggregationNoData) {
  auto row = std::vector{makeRowVector({makeFlatVector<int64_t>(0)})};
  auto agg = PlanBuilder()
                 .values(row)
                 .singleAggregation({}, {"checksum(c0)"})
                 .planNode();
  assertQuery(agg, "VALUES (CAST(NULL AS VARCHAR))");

  agg = PlanBuilder()
            .values(row)
            .partialAggregation({}, {"checksum(c0)"})
            .intermediateAggregation()
            .finalAggregation()
            .planNode();
  assertQuery(agg, "VALUES (CAST(NULL AS VARCHAR))");
}

TEST_F(ChecksumAggregateTest, timestampWithTimezone) {
  auto timestamp =
      makeFlatVector<int64_t>(5, [](auto row) { return 1639426440000; });
  auto timezone = makeFlatVector<int16_t>(5, [](auto row) { return 0; });

  auto timestampWithTzVector = std::make_shared<RowVector>(
      pool_.get(),
      TIMESTAMP_WITH_TIME_ZONE(),
      BufferPtr(nullptr),
      5,
      std::vector<VectorPtr>{timestamp, timezone});

  assertChecksum(timestampWithTzVector, "jwqENA0VLZY=");
}
} // namespace facebook::velox::aggregate::test
