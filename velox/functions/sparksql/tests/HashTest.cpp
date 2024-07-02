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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include <stdint.h>

using facebook::velox::test::assertEqualVectors;

namespace facebook::velox::functions::sparksql::test {
namespace {

class HashTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<int32_t> hash(std::optional<T> arg) {
    return evaluateOnce<int32_t>("hash(c0)", arg);
  }

  VectorPtr hash(VectorPtr vector) {
    return evaluate("hash(c0)", makeRowVector({vector}));
  }

  template <typename T>
  void runSIMDHashAndAssert(
      const T value,
      const int32_t expectedResult,
      const int32_t size,
      int unSelectedRows = 0) {
    // Generate 'size' flat vector to test SIMD code path.
    // We use same value in the vector to make comparing the results easier.
    std::vector<T> inputData;
    inputData.reserve(size);
    std::vector<int32_t> resultData;
    resultData.reserve(size);
    for (auto i = 0; i < size; ++i) {
      inputData.emplace_back(value);
      resultData.emplace_back(expectedResult);
    }
    auto input = makeFlatVector<T>(inputData);
    SelectivityVector rows(size);
    rows.setValidRange(0, unSelectedRows, false);
    auto result =
        evaluate<SimpleVector<int32_t>>("hash(c0)", makeRowVector({input}));
    velox::test::assertEqualVectors(
        makeFlatVector<int32_t>(resultData), result, rows);
  }
};

TEST_F(HashTest, String) {
  EXPECT_EQ(hash<std::string>("Spark"), 228093765);
  EXPECT_EQ(hash<std::string>(""), 142593372);
  EXPECT_EQ(hash<std::string>("abcdefghijklmnopqrstuvwxyz"), -1990933474);
  // String that has a length that is a multiple of four.
  EXPECT_EQ(hash<std::string>("12345678"), 2036199019);
  EXPECT_EQ(hash<std::string>(std::nullopt), 42);
}

TEST_F(HashTest, longDecimal) {
  EXPECT_EQ(hash<int128_t>(12345678), -277285195);
  EXPECT_EQ(hash<int128_t>(0), -783713497);
  EXPECT_EQ(hash<int128_t>(DecimalUtil::kLongDecimalMin), 1400911110);
  EXPECT_EQ(hash<int128_t>(DecimalUtil::kLongDecimalMax), -817514053);
  EXPECT_EQ(hash<int128_t>(-12345678), -1198355617);
  EXPECT_EQ(hash<int128_t>(std::nullopt), 42);
}

// Spark CLI select timestamp_micros(12345678) to get the Timestamp.
// select hash(Timestamp("1970-01-01 00:00:12.345678")) to get the hash value.
TEST_F(HashTest, Timestamp) {
  EXPECT_EQ(hash<Timestamp>(Timestamp::fromMicros(12345678)), 1402875301);
}

TEST_F(HashTest, Int64) {
  EXPECT_EQ(hash<int64_t>(0xcafecafedeadbeef), -256235155);
  EXPECT_EQ(hash<int64_t>(0xdeadbeefcafecafe), 673261790);
  EXPECT_EQ(hash<int64_t>(INT64_MAX), -1604625029);
  EXPECT_EQ(hash<int64_t>(INT64_MIN), -853646085);
  EXPECT_EQ(hash<int64_t>(1), -1712319331);
  EXPECT_EQ(hash<int64_t>(0), -1670924195);
  EXPECT_EQ(hash<int64_t>(-1), -939490007);
  EXPECT_EQ(hash<int64_t>(std::nullopt), 42);
}

TEST_F(HashTest, Int32) {
  EXPECT_EQ(hash<int32_t>(0xdeadbeef), 141248195);
  EXPECT_EQ(hash<int32_t>(0xcafecafe), 638354558);
  EXPECT_EQ(hash<int32_t>(1), -559580957);
  EXPECT_EQ(hash<int32_t>(0), 933211791);
  EXPECT_EQ(hash<int32_t>(-1), -1604776387);
  EXPECT_EQ(hash<int32_t>(std::nullopt), 42);
}

TEST_F(HashTest, Int16) {
  EXPECT_EQ(hash<int16_t>(1), -559580957);
  EXPECT_EQ(hash<int16_t>(0), 933211791);
  EXPECT_EQ(hash<int16_t>(-1), -1604776387);
  EXPECT_EQ(hash<int16_t>(std::nullopt), 42);
}

TEST_F(HashTest, Int8) {
  EXPECT_EQ(hash<int8_t>(1), -559580957);
  EXPECT_EQ(hash<int8_t>(0), 933211791);
  EXPECT_EQ(hash<int8_t>(-1), -1604776387);
  EXPECT_EQ(hash<int8_t>(std::nullopt), 42);
}

TEST_F(HashTest, Bool) {
  EXPECT_EQ(hash<bool>(false), 933211791);
  EXPECT_EQ(hash<bool>(true), -559580957);
  EXPECT_EQ(hash<bool>(std::nullopt), 42);
}

TEST_F(HashTest, StringInt32) {
  auto hash = [&](std::optional<std::string> a, std::optional<int32_t> b) {
    return evaluateOnce<int32_t>("hash(c0, c1)", a, b);
  };

  EXPECT_EQ(hash(std::nullopt, std::nullopt), 42);
  EXPECT_EQ(hash("", std::nullopt), 142593372);
  EXPECT_EQ(hash(std::nullopt, 0), 933211791);
  EXPECT_EQ(hash("", 0), 1143746540);
}

TEST_F(HashTest, Double) {
  using limits = std::numeric_limits<double>;

  EXPECT_EQ(hash<double>(std::nullopt), 42);
  EXPECT_EQ(hash<double>(-0.0), -1670924195);
  EXPECT_EQ(hash<double>(0), -1670924195);
  EXPECT_EQ(hash<double>(1), -460888942);
  EXPECT_EQ(hash<double>(limits::quiet_NaN()), -1281358385);
  EXPECT_EQ(hash<double>(limits::infinity()), 833680482);
  EXPECT_EQ(hash<double>(-limits::infinity()), 461104036);
}

TEST_F(HashTest, Float) {
  using limits = std::numeric_limits<float>;

  EXPECT_EQ(hash<float>(std::nullopt), 42);
  EXPECT_EQ(hash<float>(-0.0f), 933211791);
  EXPECT_EQ(hash<float>(0), 933211791);
  EXPECT_EQ(hash<float>(1), -466301895);
  EXPECT_EQ(hash<float>(limits::quiet_NaN()), -349261430);
  EXPECT_EQ(hash<float>(limits::infinity()), 2026854605);
  EXPECT_EQ(hash<float>(-limits::infinity()), 427440766);
}

TEST_F(HashTest, array) {
  assertEqualVectors(
      makeFlatVector<int32_t>({2101165938, 42, 1045631400}),
      hash(makeArrayVector<int64_t>({{1, 2, 3, 4, 5}, {}, {1, 2, 3}})));

  assertEqualVectors(
      makeFlatVector<int32_t>({-559580957, 1765031574, 42}),
      hash(makeNullableArrayVector<int32_t>(
          {{1, std::nullopt}, {std::nullopt, 2}, {std::nullopt}})));

  // Nested array.
  {
    auto arrayVector = makeNestedArrayVectorFromJson<int64_t>(
        {"[[1, null, 2, 3], [4, 5]]",
         "[[1, null, 2, 3], [6, 7, 8]]",
         "[[]]",
         "[[null]]",
         "[null]"});
    assertEqualVectors(
        makeFlatVector<int32_t>({2101165938, -992561130, 42, 42, 42}),
        hash(arrayVector));
  }

  // Array of map.
  {
    using S = StringView;
    using P = std::pair<int64_t, std::optional<S>>;
    std::vector<P> a{P{1, S{"a"}}, P{2, std::nullopt}};
    std::vector<P> b{P{3, S{"c"}}};
    std::vector<std::vector<std::vector<P>>> data = {{a, b}};
    auto arrayVector = makeArrayOfMapVector<int64_t, S>(data);
    assertEqualVectors(
        makeFlatVector<int32_t>(std::vector<int32_t>{-718462205}),
        hash(arrayVector));
  }

  // Array of row.
  {
    std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
        data = {
            {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
            {{{1, "red"}}, std::nullopt, {{3, "green"}}},
            {std::nullopt},
        };
    auto arrayVector = makeArrayOfRowVector(data, ROW({INTEGER(), VARCHAR()}));
    assertEqualVectors(
        makeFlatVector<int32_t>({-1458343314, 551500425, 42}),
        hash(arrayVector));
  }
}

TEST_F(HashTest, map) {
  auto mapVector = makeMapVector<int64_t, double>(
      {{{1, 17.0}, {2, 36.0}, {3, 8.0}, {4, 28.0}, {5, 24.0}, {6, 32.0}}});
  assertEqualVectors(
      makeFlatVector<int32_t>(std::vector<int32_t>{1263683448}),
      hash(mapVector));

  auto mapOfArrays = createMapOfArraysVector<int32_t, int32_t>(
      {{{1, {{1, 2, 3}}}}, {{2, {{4, 5, 6}}}}, {{3, {{7, 8, 9}}}}});
  assertEqualVectors(
      makeFlatVector<int32_t>({-1818148947, 529298908, 825098912}),
      hash(mapOfArrays));

  auto mapWithNullArrays = createMapOfArraysVector<int64_t, int64_t>(
      {{{1, std::nullopt}}, {{2, {{4, 5, std::nullopt}}}}, {{3, {{}}}}});
  assertEqualVectors(
      makeFlatVector<int32_t>({-1712319331, 2060637564, 519220707}),
      hash(mapWithNullArrays));
}

TEST_F(HashTest, row) {
  auto row = makeRowVector({
      makeFlatVector<int64_t>({1, 3}),
      makeFlatVector<int64_t>({2, 4}),
  });
  assertEqualVectors(
      makeFlatVector<int32_t>({-1181176833, 1717636039}), hash(row));

  row = makeRowVector({
      makeNullableFlatVector<int64_t>({1, std::nullopt}),
      makeNullableFlatVector<int64_t>({std::nullopt, 4}),
  });
  assertEqualVectors(
      makeFlatVector<int32_t>({-1712319331, 1344313940}), hash(row));

  row->setNull(0, true);
  assertEqualVectors(makeFlatVector<int32_t>({42, 1344313940}), hash(row));

  row->setNull(1, true);
  assertEqualVectors(makeFlatVector<int32_t>({42, 42}), hash(row));
}

TEST_F(HashTest, simd) {
  runSIMDHashAndAssert<int8_t>(1, -559580957, 1024, 10);
  runSIMDHashAndAssert<int16_t>(-1, -1604776387, 4096);
  runSIMDHashAndAssert<int32_t>(0xcafecafe, 638354558, 33);
  runSIMDHashAndAssert<int64_t>(-1, -939490007, 34);
  runSIMDHashAndAssert<std::string>("Spark", 228093765, 33);
  runSIMDHashAndAssert<float>(1, -466301895, 77);
  runSIMDHashAndAssert<double>(1, -460888942, 1000, 32);
  runSIMDHashAndAssert<Timestamp>(
      Timestamp::fromMicros(12345678), 1402875301, 1000);

  runSIMDHashAndAssert<int64_t>(-1, -939490007, 1024, 1023);
  runSIMDHashAndAssert<int64_t>(-1, -939490007, 1024, 512);
  runSIMDHashAndAssert<int64_t>(-1, -939490007, 1024, 3);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
