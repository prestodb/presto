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

#include <optional>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;

class GreatestLeastTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T>
  void runTest(
      const std::string& query,
      const std::vector<std::vector<T>>& inputs,
      const std::vector<std::optional<T>>& output,
      std::optional<size_t> stringBuffersExpectedCount = std::nullopt) {
    // Create input vectors
    auto vectorSize = inputs[0].size();
    std::vector<VectorPtr> inputColumns(inputs.size());
    for (auto i = 0; i < inputColumns.size(); ++i) {
      inputColumns[i] = makeFlatVector<T>(inputs[i]);
      for (auto j = 0; j < vectorSize; ++j) {
        inputColumns[i]->asFlatVector<T>()->set(j, inputs[i][j]);
      }
    }

    // Call evaluate to run the query on the created input
    auto result = evaluate<SimpleVector<T>>(query, makeRowVector(inputColumns));
    for (int32_t i = 0; i < vectorSize; ++i) {
      if (output[i].has_value()) {
        ASSERT_EQ(result->valueAt(i), output[i]);
      } else {
        ASSERT_TRUE(result->isNullAt(i));
      }
    }

    if (stringBuffersExpectedCount.has_value()) {
      ASSERT_EQ(
          *stringBuffersExpectedCount,
          result->template asFlatVector<StringView>()->stringBuffers().size());
    }
  }

  void runDecimalTest(
      const std::string& query,
      const std::vector<VectorPtr>& input,
      const VectorPtr& output) {
    auto result = evaluate(query, makeRowVector(input));
    test::assertEqualVectors(output, result);
  }
};

TEST_F(GreatestLeastTest, leastDouble) {
  runTest<double>("least(c0)", {{0, 1.1, -1.1}}, {0, 1.1, -1.1});
  runTest<double>("least(c0, 1.0)", {{0, 1.1, -1.1}}, {0, 1, -1.1});
  runTest<double>(
      "least(c0, 1.0 , c1)", {{0, 1.1, -1.1}, {100, -100, 0}}, {0, -100, -1.1});
}

TEST_F(GreatestLeastTest, nanInput) {
  std::vector<double> input{0, 1.1, std::nan("1")};
  VELOX_ASSERT_THROW(
      runTest<double>("least(c0)", {{0.0 / 0.0}}, {0}),
      "Invalid argument to least(): NaN");
  runTest<double>("try(least(c0, 1.0))", {input}, {0, 1.0, std::nullopt});

  VELOX_ASSERT_THROW(
      runTest<double>("greatest(c0)", {1, {0.0 / 0.0}}, {1, 0}),
      "Invalid argument to greatest(): NaN");
  runTest<double>("try(greatest(c0, 1.0))", {input}, {1.0, 1.1, std::nullopt});
}

TEST_F(GreatestLeastTest, greatestDouble) {
  runTest<double>("greatest(c0)", {{0, 1.1, -1.1}}, {0, 1.1, -1.1});
  runTest<double>("greatest(c0, 1.0)", {{0, 1.1, -1.1}}, {1, 1.1, 1});
  runTest<double>(
      "greatest(c0, 1.0 , c1)",
      {{0, 1.1, -1.1}, {100, -100, 0}},
      {100, 1.1, 1});
}

TEST_F(GreatestLeastTest, leastInteger) {
  // TinyInt
  runTest<int8_t>("least(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int8_t>("least(c0, c1)", {{0, 1, -1}, {100, -100, 0}}, {0, -100, -1});
  // SmallInt
  runTest<int16_t>("least(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int16_t>(
      "least(c0, c1)", {{0, 1, -1}, {100, -100, 0}}, {0, -100, -1});
  // Integer
  runTest<int32_t>("least(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int32_t>(
      "least(c0, c1)", {{0, 1, -1}, {100, -100, 0}}, {0, -100, -1});
  // BigInt
  runTest<int64_t>("least(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int64_t>("least(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int64_t>(
      "least(c0, c1)", {{0, 1, -1}, {100, -100, 0}}, {0, -100, -1});
  runTest<int64_t>("least(c0, 1)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int64_t>(
      "least(c0, 1 , c1)", {{0, 1, -1}, {100, -100, 0}}, {0, -100, -1});
}

TEST_F(GreatestLeastTest, greatestInteger) {
  // TinyInt
  runTest<int8_t>("greatest(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int8_t>(
      "greatest(c0, c1)", {{0, 1, -1}, {100, -100, 0}}, {100, 1, 0});
  // SmallInt
  runTest<int16_t>("greatest(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int16_t>(
      "greatest(c0, c1)", {{0, 1, -1}, {100, -100, 0}}, {100, 1, 0});
  // Integer
  runTest<int32_t>("greatest(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int32_t>(
      "greatest(c0, c1)", {{0, 1, -1}, {100, -100, 0}}, {100, 1, 0});
  // BigInt
  runTest<int64_t>("greatest(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int64_t>("greatest(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int64_t>("greatest(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int64_t>("greatest(c0, 1)", {{0, 1, -1}}, {1, 1, 1});
  runTest<int64_t>(
      "greatest(c0, 1 , c1)", {{0, 1, -1}, {100, -100, 0}}, {100, 1, 1});
}

TEST_F(GreatestLeastTest, greatestVarchar) {
  runTest<StringView>(
      "greatest(c0)", {{"a"_sv, "b"_sv, "c"_sv}}, {"a"_sv, "b"_sv, "c"_sv});

  runTest<StringView>(
      "greatest(c0, 'aaa')", {{""_sv, "abb"_sv}}, {"aaa"_sv, "abb"_sv});
}

TEST_F(GreatestLeastTest, leasstVarchar) {
  runTest<StringView>(
      "least(c0)", {{"a"_sv, "b"_sv, "c"_sv}}, {"a"_sv, "b"_sv, "c"_sv});

  runTest<StringView>(
      "least(c0, 'aaa')", {{""_sv, "abb"_sv}}, {""_sv, "aaa"_sv});
}

TEST_F(GreatestLeastTest, greatestTimeStamp) {
  runTest<Timestamp>(
      "greatest(c0, c1, c2)",
      {{Timestamp(0, 0), Timestamp(10, 100), Timestamp(100, 10)},
       {Timestamp(1, 0), Timestamp(10, 1), Timestamp(100, 10)},
       {Timestamp(0, 1), Timestamp(312, 100), Timestamp(100, 11)}},
      {Timestamp(1, 0), Timestamp(312, 100), Timestamp(100, 11)});
}

TEST_F(GreatestLeastTest, leastTimeStamp) {
  runTest<Timestamp>(
      "least(c0, c1, c2)",
      {{Timestamp(0, 0), Timestamp(10, 100), Timestamp(100, 10)},
       {Timestamp(1, 0), Timestamp(10, 1), Timestamp(100, 10)},
       {Timestamp(0, 1), Timestamp(312, 100), Timestamp(1, 10)}},
      {Timestamp(0, 0), Timestamp(10, 1), Timestamp(1, 10)});
}

TEST_F(GreatestLeastTest, greatestDate) {
  runTest<Date>(
      "greatest(c0, c1, c2)",
      {{Date(0), Date(5), Date(0)},
       {Date(1), Date(0), Date(-5)},
       {Date(5), Date(-5), Date(-10)}},
      {Date(5), Date(5), Date(0)});
}

TEST_F(GreatestLeastTest, leastDate) {
  runTest<Date>(
      "least(c0, c1, c2)",
      {{Date(0), Date(0), Date(5)},
       {Date(1), Date(-1), Date(-1)},
       {Date(5), Date(5), Date(-5)}},
      {Date(0), Date(-1), Date(-5)});
}

TEST_F(GreatestLeastTest, stringBuffersMoved) {
  runTest<StringView>(
      "least(c0, c1)",
      {{"aaaaaaaaaaaaaa"_sv, "bbbbbbbbbbbbbb"_sv},
       {"cccccccccccccc"_sv, "dddddddddddddd"_sv}},
      {"aaaaaaaaaaaaaa"_sv, "bbbbbbbbbbbbbb"_sv},
      1);

  runTest<StringView>(
      "least(c0, c1, '')",
      {{"aaaaaaaaaaaaaa"_sv, "bbbbbbbbbbbbbb"_sv},
       {"cccccccccccccc"_sv, "dddddddddddddd"_sv}},
      {""_sv, ""_sv},
      0);
}

TEST_F(GreatestLeastTest, clearNulls) {
  auto c0 = makeFlatVector<int64_t>(10, folly::identity);
  auto result = evaluate<SimpleVector<int64_t>>(
      "SWITCH(c0 > 5, null::BIGINT, greatest(c0))", makeRowVector({c0}));
  ASSERT_EQ(result->size(), 10);
  for (int i = 0; i < result->size(); ++i) {
    if (i > 5) {
      ASSERT_TRUE(result->isNullAt(i));
    } else {
      ASSERT_EQ(result->valueAt(i), i);
    }
  }
}

TEST_F(GreatestLeastTest, shortDecimal) {
  const auto type = DECIMAL(10, 4);
  static const auto kMin = DecimalUtil::kLongDecimalMin + 1;
  static const auto kMax = DecimalUtil::kLongDecimalMax - 1;

  const auto a = makeNullableShortDecimalFlatVector(
      {10000, -10000, 20000, kMax, kMin, std::nullopt}, type);
  const auto b = makeNullableShortDecimalFlatVector(
      {-10000, 10000, -20000, kMin, kMax, 1}, type);
  runDecimalTest("least(c0)", {a}, a);
  runDecimalTest("greatest(c0)", {a}, a);

  auto expected = makeNullableShortDecimalFlatVector(
      {-10000, -10000, -20000, kMin, kMin, std::nullopt}, type);
  runDecimalTest("least(c0, c1)", {a, b}, expected);

  expected = makeNullableShortDecimalFlatVector(
      {10000, 10000, 20000, kMax, kMax, std::nullopt}, type);
  runDecimalTest("greatest(c0, c1)", {a, b}, expected);
}

TEST_F(GreatestLeastTest, longDecimal) {
  const auto type = DECIMAL(38, 10);
  static const auto kMin = DecimalUtil::kLongDecimalMin + 1;
  static const auto kMax = DecimalUtil::kLongDecimalMax - 1;

  const auto a = makeNullableLongDecimalFlatVector(
      {HugeInt::build(10, 300),
       HugeInt::build(-10, 300),
       HugeInt::build(200, 300),
       kMax,
       kMin,
       std::nullopt},
      type);
  const auto b = makeNullableLongDecimalFlatVector(
      {HugeInt::build(-10, 300),
       HugeInt::build(10, 300),
       HugeInt::build(-200, 300),
       kMin,
       kMax,
       HugeInt::build(1, 1)},
      type);
  runDecimalTest("least(c0)", {a}, a);
  runDecimalTest("greatest(c0)", {a}, a);

  auto expected = makeNullableLongDecimalFlatVector(
      {HugeInt::build(-10, 300),
       HugeInt::build(-10, 300),
       HugeInt::build(-200, 300),
       kMin,
       kMin,
       std::nullopt},
      type);
  runDecimalTest("least(c0, c1)", {a, b}, expected);

  expected = makeNullableLongDecimalFlatVector(
      {HugeInt::build(10, 300),
       HugeInt::build(10, 300),
       HugeInt::build(200, 300),
       kMax,
       kMax,
       std::nullopt},
      type);
  runDecimalTest("greatest(c0, c1)", {a, b}, expected);
}
