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
#include "velox/functions/prestosql/Comparisons.h"
#include <string>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/OptionalEmpty.h"
#include "velox/functions/Udf.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/tests/utils/CustomTypesForTesting.h"
#include "velox/type/tz/TimeZoneMap.h"

using namespace facebook::velox;

class ComparisonsTest : public functions::test::FunctionBaseTest {
 public:
  void SetUp() override {
    this->options_.parseDecimalAsDouble = false;
  }

 protected:
  void testBetweenExpr(
      const std::string& exprStr,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expectedResult) {
    auto actual = evaluate(exprStr, makeRowVector(input));
    test::assertEqualVectors(expectedResult, actual);
  }

  template <typename vectorType>
  void testCompareComplexTypes(
      vectorType vector1,
      vectorType vector2,
      const std::optional<std::vector<std::optional<bool>>>& expectedResults,
      const std::string& expectedException) {
    auto input = makeRowVector({vector1, vector2});
    std::vector<std::optional<bool>> results;
    std::string comparisonExpressions[4] = {">", ">=", "<", "<="};
    if (expectedResults.has_value()) {
      for (auto expression : comparisonExpressions) {
        results.push_back(
            evaluate<SimpleVector<bool>>("c0" + expression + "c1", input)
                ->valueAt(0));
      }
      ASSERT_EQ(results, expectedResults);
    } else {
      for (auto expression : comparisonExpressions) {
        VELOX_ASSERT_THROW(
            evaluate<SimpleVector<bool>>("c0" + expression + "c1", input),
            expectedException);
      }
    }
  }

  template <typename T, typename T2>
  void testCompareRow(
      const std::tuple<std::optional<T>, std::optional<T2>>& row1,
      const std::tuple<std::optional<T>, std::optional<T2>>& row2,
      std::optional<std::vector<std::optional<bool>>> expectedResults,
      std::string expectedException = "") {
    // Make row vectors.
    auto vector1 = vectorMaker_.rowVector(
        {vectorMaker_.flatVectorNullable<T>({std::get<0>(row1)}),
         vectorMaker_.flatVectorNullable<T2>({std::get<1>(row1)})});
    auto vector2 = vectorMaker_.rowVector(
        {vectorMaker_.flatVectorNullable<T>({std::get<0>(row2)}),
         vectorMaker_.flatVectorNullable<T2>({std::get<1>(row2)})});
    testCompareComplexTypes<RowVectorPtr>(
        vector1, vector2, expectedResults, expectedException);
  }

  template <typename T, typename T2>
  void testCompareRowDifferentTypesAndSizes(
      const std::tuple<std::optional<T>, std::optional<T>>& row1,
      const std::tuple<std::optional<T2>, std::optional<T2>>& row2,
      std::optional<std::vector<std::optional<bool>>> expectedResults,
      std::string expectedException = "") {
    // Make row vectors.
    auto vector1 = vectorMaker_.rowVector(
        {vectorMaker_.flatVectorNullable<T>({std::get<0>(row1)}),
         vectorMaker_.flatVectorNullable<T>({std::get<1>(row1)})});
    auto vector2 = vectorMaker_.rowVector(
        {vectorMaker_.flatVectorNullable<T2>({std::get<0>(row2)}),
         vectorMaker_.flatVectorNullable<T2>({std::get<1>(row2)})});
    testCompareComplexTypes<RowVectorPtr>(
        vector1, vector2, expectedResults, expectedException);

    // Compare rows with different sizes by removing one element from vector1.
    auto vector3 = vectorMaker_.rowVector(
        {vectorMaker_.flatVectorNullable<T>({std::get<0>(row1)})});
    testCompareComplexTypes<RowVectorPtr>(
        vector1, vector3, expectedResults, expectedException);
  }

  template <typename T>
  void testCompareArray(
      const std::vector<std::optional<T>>& array1,
      const std::vector<std::optional<T>>& array2,
      std::optional<std::vector<std::optional<bool>>> expectedResults,
      std::string expectedException = "") {
    // Make array vectors.
    auto vector1 = vectorMaker_.arrayVectorNullable<T>({array1});
    auto vector2 = vectorMaker_.arrayVectorNullable<T>({array2});
    testCompareComplexTypes<ArrayVectorPtr>(
        vector1, vector2, expectedResults, expectedException);
  }

  template <typename T>
  void testCompareMap(
      const std::optional<std::vector<std::pair<T, std::optional<T>>>>& map1,
      const std::optional<std::vector<std::pair<T, std::optional<T>>>>& map2,
      std::string expectedException) {
    // Make map vectors.
    auto vector1 = makeNullableMapVector<T, T>({map1});
    auto vector2 = makeNullableMapVector<T, T>({map2});
    testCompareComplexTypes<MapVectorPtr>(
        vector1, vector2, std::nullopt, expectedException);
  }

  void registerSimpleComparisonFunctions() {
    using namespace facebook::velox::functions;
    registerBinaryScalar<EqFunction, bool>({"simple_eq"});
    registerBinaryScalar<NeqFunction, bool>({"simple_neq"});
    registerBinaryScalar<LtFunction, bool>({"simple_lt"});
    registerBinaryScalar<LteFunction, bool>({"simple_lte"});
    registerBinaryScalar<GtFunction, bool>({"simple_gt"});
    registerBinaryScalar<GteFunction, bool>({"simple_gte"});
  }
};

TEST_F(ComparisonsTest, between) {
  std::vector<std::tuple<int32_t, bool>> testData = {
      {0, false}, {1, true}, {4, true}, {5, true}, {10, false}, {-1, false}};

  auto result = evaluate<SimpleVector<bool>>(
      "c0 between 1 and 5",
      makeRowVector({makeFlatVector<int32_t, 0>(testData)}));

  for (int i = 0; i < testData.size(); ++i) {
    EXPECT_EQ(result->valueAt(i), std::get<1>(testData[i])) << "at " << i;
  }
}

TEST_F(ComparisonsTest, betweenVarchar) {
  using S = StringView;

  const auto between = [&](std::optional<std::string> s) {
    auto expr = "c0 between 'mango' and 'pear'";
    if (s.has_value()) {
      return evaluateOnce<bool>(expr, std::optional(S(s.value())));
    } else {
      return evaluateOnce<bool>(expr, std::optional<S>());
    }
  };

  EXPECT_EQ(std::nullopt, between(std::nullopt));
  EXPECT_EQ(false, between(""));
  EXPECT_EQ(false, between("apple"));
  EXPECT_EQ(false, between("pineapple"));
  EXPECT_EQ(true, between("mango"));
  EXPECT_EQ(true, between("orange"));
  EXPECT_EQ(true, between("pear"));
}

TEST_F(ComparisonsTest, betweenDate) {
  std::vector<std::tuple<int32_t, bool>> testData = {
      {parseDate("2019-05-01"), false},
      {parseDate("2019-06-01"), true},
      {parseDate("2019-07-01"), true},
      {parseDate("2020-05-31"), true},
      {parseDate("2020-06-01"), true},
      {parseDate("2020-07-01"), false}};

  auto result = evaluate<SimpleVector<bool>>(
      "c0 between cast(\'2019-06-01\' as date) and cast(\'2020-06-01\' as date)",
      makeRowVector({makeFlatVector<int32_t, 0>(testData, DATE())}));

  for (int i = 0; i < testData.size(); ++i) {
    EXPECT_EQ(result->valueAt(i), std::get<1>(testData[i])) << "at " << i;
  }
}

TEST_F(ComparisonsTest, betweenTimestamp) {
  const auto between = [&](std::optional<std::string> s) {
    auto expr =
        "c0 between cast(\'2019-02-28 10:00:00.500\' as timestamp) and"
        " cast(\'2019-02-28 10:00:00.600\' as timestamp)";
    if (!s.has_value()) {
      return evaluateOnce<bool>(expr, std::optional<Timestamp>());
    }
    return evaluateOnce<bool>(expr, std::optional{parseTimestamp(s.value())});
  };

  EXPECT_EQ(std::nullopt, between(std::nullopt));
  EXPECT_FALSE(between("2019-02-28 10:00:00.000").value());
  EXPECT_TRUE(between("2019-02-28 10:00:00.500").value());
  EXPECT_TRUE(between("2019-02-28 10:00:00.600").value());
  EXPECT_FALSE(between("2019-02-28 10:00:00.650").value());
}

TEST_F(ComparisonsTest, betweenDecimal) {
  auto runAndCompare = [&](const std::string& exprStr,
                           VectorPtr input,
                           VectorPtr expectedResult) {
    auto actual = evaluate<SimpleVector<bool>>(exprStr, makeRowVector({input}));
    test::assertEqualVectors(expectedResult, actual);
  };

  auto shortFlat = makeNullableFlatVector<int64_t>(
      {100, 250, 300, 500, std::nullopt}, DECIMAL(3, 2));
  auto expectedResult =
      makeNullableFlatVector<bool>({false, true, true, false, std::nullopt});

  runAndCompare("c0 between 2.00 and 3.00", shortFlat, expectedResult);

  auto longFlat = makeNullableFlatVector<int128_t>(
      {100, 250, 300, 500, std::nullopt}, DECIMAL(20, 2));

  runAndCompare(
      "c0 between cast(2.00 as DECIMAL(20, 2)) and cast(3.00 as DECIMAL(20, 2))",
      longFlat,
      expectedResult);

  // Comparing LONG_DECIMAL and SHORT_DECIMAL must throw error.
  VELOX_ASSERT_THROW(
      runAndCompare("c0 between 2.00 and 3.00", longFlat, expectedResult),
      "Scalar function signature is not supported: "
      "between(DECIMAL(20, 2), DECIMAL(3, 2), DECIMAL(3, 2)).");
}

TEST_F(ComparisonsTest, betweenDecimalNonConstantVectors) {
  // Short decimal tests.

  // Fast path when c1 vector is constant and c2 is flat.
  testBetweenExpr(
      "c0 between c1 and c2",
      {
          makeFlatVector<int64_t>({100, 200, 300, 400}, DECIMAL(5, 1)),
          makeConstant((int64_t)100, 4, DECIMAL(5, 1)),
          makeFlatVector<int64_t>({500, 200, 500, 110}, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({true, true, true, false}));

  // Fast path when c1 vector is flat and c2 is constant.
  testBetweenExpr(
      "c0 between c1 and c2",
      {
          makeFlatVector<int64_t>({100, 200, 300, 400}, DECIMAL(5, 1)),
          makeFlatVector<int64_t>({100, 100, 100, 200}, DECIMAL(5, 1)),
          makeConstant((int64_t)300, 4, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({true, true, true, false}));

  // Fast path when all three vectors are flat.
  testBetweenExpr(
      "c0 between c1 and c2",
      {
          makeFlatVector<int64_t>({100, 200, 300, 400}, DECIMAL(5, 1)),
          makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1)),
          makeFlatVector<int64_t>({150, 200, 310, 370}, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({true, true, true, false}));

  // General case when vectors are dictionary-encoded.
  testBetweenExpr(
      "c0 between c1 and c2",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({100, 200, 300, 400}, DECIMAL(5, 1))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({150, 200, 310, 370}, DECIMAL(5, 1))),
      },
      makeFlatVector<bool>({true, true, true, false}));

  // General case of short decimals with nulls.
  testBetweenExpr(
      "c0 between c1 and c2",
      {
          makeFlatVector<int64_t>({100, 200, 300, 400}, DECIMAL(5, 1)),
          makeNullableFlatVector<int64_t>(
              {100, std::nullopt, 130, 350}, DECIMAL(5, 1)),
          makeNullableFlatVector<int64_t>(
              {150, 200, std::nullopt, 370}, DECIMAL(5, 1)),
      },
      makeNullableFlatVector<bool>({true, std::nullopt, std::nullopt, false}));

  // Long decimal tests.

  // Fast path when c1 vector is constant and c2 is flat.
  testBetweenExpr(
      "c0 between c1 and c2",
      {
          makeFlatVector<int128_t>({100, 200, 300, 400}, DECIMAL(30, 1)),
          makeConstant(HugeInt::build(0, 100), 4, DECIMAL(30, 1)),
          makeFlatVector<int128_t>({500, 200, 500, 110}, DECIMAL(30, 1)),
      },
      makeFlatVector<bool>({true, true, true, false}));

  // Fast path when c1 vector is flat and c2 is constant.
  testBetweenExpr(
      "c0 between c1 and c2",
      {
          makeFlatVector<int128_t>({100, 200, 300, 400}, DECIMAL(30, 1)),
          makeFlatVector<int128_t>({100, 100, 100, 200}, DECIMAL(30, 1)),
          makeConstant(HugeInt::build(0, 300), 4, DECIMAL(30, 1)),
      },
      makeFlatVector<bool>({true, true, true, false}));

  // Fast path when all three vectors are flat.
  testBetweenExpr(
      "c0 between c1 and c2",
      {
          makeFlatVector<int128_t>({100, 200, 300, 400}, DECIMAL(30, 1)),
          makeFlatVector<int128_t>({100, 120, 130, 350}, DECIMAL(30, 1)),
          makeFlatVector<int128_t>({150, 200, 310, 370}, DECIMAL(30, 1)),
      },
      makeFlatVector<bool>({true, true, true, false}));

  // General case when vectors are dictionary-encoded.
  testBetweenExpr(
      "c0 between c1 and c2",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int128_t>({100, 200, 300, 400}, DECIMAL(30, 1))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int128_t>({100, 120, 130, 350}, DECIMAL(30, 1))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int128_t>({150, 200, 310, 370}, DECIMAL(30, 1))),
      },
      makeFlatVector<bool>({true, true, true, false}));

  // General case of long decimals with nulls.
  testBetweenExpr(
      "c0 between c1 and c2",
      {
          makeFlatVector<int128_t>({100, 200, 300, 400}, DECIMAL(30, 1)),
          makeNullableFlatVector<int128_t>(
              {100, std::nullopt, 130, 350}, DECIMAL(30, 1)),
          makeNullableFlatVector<int128_t>(
              {150, 200, std::nullopt, 370}, DECIMAL(30, 1)),
      },
      makeNullableFlatVector<bool>({true, std::nullopt, std::nullopt, false}));
}

TEST_F(ComparisonsTest, eqNeqDecimal) {
  auto runAndCompare = [&](const std::vector<VectorPtr>& inputs,
                           const VectorPtr& expectedResult,
                           const std::string& op) {
    auto actual = evaluate<SimpleVector<bool>>(
        fmt::format("c0 {} c1", op), makeRowVector(inputs));
    test::assertEqualVectors(actual, expectedResult);
  };

  std::vector<VectorPtr> inputs = {
      makeNullableFlatVector<int64_t>(
          {1, std::nullopt, 3, -3, std::nullopt, 4}, DECIMAL(10, 5)),
      makeNullableFlatVector<int64_t>(
          {1, 2, 3, -3, std::nullopt, 5}, DECIMAL(10, 5))};
  // Equal on decimals.
  auto expected = makeNullableFlatVector<bool>(
      {true, std::nullopt, true, true, std::nullopt, false});
  runAndCompare(inputs, expected, "=");

  std::vector<VectorPtr> inputsLong = {
      makeNullableFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMin,
           std::nullopt,
           DecimalUtil::kLongDecimalMax,
           -3,
           std::nullopt,
           4},
          DECIMAL(30, 5)),
      makeNullableFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMin,
           std::nullopt,
           DecimalUtil::kLongDecimalMax,
           -3,
           std::nullopt,
           5},
          DECIMAL(30, 5))};
  runAndCompare(inputs, expected, "=");
  // Not-Equal on decimals.
  expected = makeNullableFlatVector<bool>(
      {false, std::nullopt, false, false, std::nullopt, true});
  runAndCompare(inputs, expected, "!=");
  runAndCompare(inputsLong, expected, "!=");
  // Test with different data types.
  inputs = {
      makeFlatVector(std::vector<int64_t>{1}, DECIMAL(10, 5)),
      makeFlatVector(std::vector<int64_t>{1}, DECIMAL(10, 4))};
  VELOX_ASSERT_THROW(
      runAndCompare(inputs, expected, "="),
      "Scalar function signature is not supported: "
      "eq(DECIMAL(10, 5), DECIMAL(10, 4))");
  VELOX_ASSERT_THROW(
      runAndCompare(inputs, expected, "!="),
      "Scalar function signature is not supported: "
      "neq(DECIMAL(10, 5), DECIMAL(10, 4))");
}

TEST_F(ComparisonsTest, gtLtDecimal) {
  auto runAndCompare = [&](std::string expr,
                           std::vector<VectorPtr>& inputs,
                           VectorPtr expectedResult) {
    auto actual = evaluate<SimpleVector<bool>>(expr, makeRowVector(inputs));
    test::assertEqualVectors(expectedResult, actual);
  };

  // Short Decimals test.
  std::vector<VectorPtr> shortDecimalInputs = {
      makeNullableFlatVector<int64_t>(
          {1, std::nullopt, 3, -3, std::nullopt, 4}, DECIMAL(10, 5)),
      makeNullableFlatVector<int64_t>(
          {0, 2, 3, -5, std::nullopt, 5}, DECIMAL(10, 5))};
  auto expectedGtLt = makeNullableFlatVector<bool>(
      {true, std::nullopt, false, true, std::nullopt, false});
  auto expectedGteLte = makeNullableFlatVector<bool>(
      {true, std::nullopt, true, true, std::nullopt, false});

  runAndCompare("c0 > c1", shortDecimalInputs, expectedGtLt);
  runAndCompare("c1 < c0", shortDecimalInputs, expectedGtLt);
  // Gte/Lte
  runAndCompare("c0 >= c1", shortDecimalInputs, expectedGteLte);
  runAndCompare("c1 <= c0", shortDecimalInputs, expectedGteLte);

  // Long Decimals test.
  std::vector<VectorPtr> longDecimalsInputs = {
      makeNullableFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax,
           std::nullopt,
           3,
           DecimalUtil::kLongDecimalMin + 1,
           std::nullopt,
           4},
          DECIMAL(38, 5)),
      makeNullableFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax - 1,
           2,
           3,
           DecimalUtil::kLongDecimalMin,
           std::nullopt,
           5},
          DECIMAL(38, 5))};
  runAndCompare("c0 > c1", longDecimalsInputs, expectedGtLt);
  runAndCompare("c1 < c0", longDecimalsInputs, expectedGtLt);

  // Gte/Lte
  runAndCompare("c0 >= c1", longDecimalsInputs, expectedGteLte);
  runAndCompare("c1 <= c0", longDecimalsInputs, expectedGteLte);
};

TEST_F(ComparisonsTest, eqNeqArray) {
  auto test =
      [&](const std::optional<std::vector<std::optional<int64_t>>>& array1,
          const std::optional<std::vector<std::optional<int64_t>>>& array2,
          std::optional<bool> expected) {
        auto vector1 = vectorMaker_.arrayVectorNullable<int64_t>({array1});
        auto vector2 = vectorMaker_.arrayVectorNullable<int64_t>({array2});
        auto eqResult = evaluate<SimpleVector<bool>>(
            "c0 == c1", makeRowVector({vector1, vector2}));

        auto neqResult = evaluate<SimpleVector<bool>>(
            "c0 != c1", makeRowVector({vector1, vector2}));

        ASSERT_EQ(expected.has_value(), !eqResult->isNullAt(0));
        ASSERT_EQ(expected.has_value(), !neqResult->isNullAt(0));
        if (expected.has_value()) {
          // equals check
          ASSERT_EQ(expected.value(), eqResult->valueAt(0));
          // not equal check
          ASSERT_EQ(!expected.value(), neqResult->valueAt(0));
        }
      };

  // eq and neq function test
  test(std::nullopt, std::nullopt, std::nullopt);
  test(std::nullopt, {{1}}, std::nullopt);
  test({{1}}, std::nullopt, std::nullopt);

  test(common::testutil::optionalEmpty, common::testutil::optionalEmpty, true);

  test({{1, 2, 3}}, {{1, 2, 3}}, true);
  test({{1, 2, 3}}, {{1, 2, 4}}, false);

  // Checking the first element is enough to determine the result of the
  // compare.
  test({{1, std::nullopt}}, {{6, 2}}, false);

  test({{1, std::nullopt}}, {{1, 2}}, std::nullopt);

  // Different size arrays.
  test(common::testutil::optionalEmpty, {{std::nullopt, std::nullopt}}, false);
  test({{1, 2}}, {{1, 2, std::nullopt}}, false);
  test(
      {{std::nullopt, std::nullopt}},
      {{std::nullopt, std::nullopt, std::nullopt}},
      false);

  test(
      {{std::nullopt, std::nullopt}},
      {{std::nullopt, std::nullopt}},
      std::nullopt);
}

TEST_F(ComparisonsTest, eqNeqMap) {
  using map_t =
      std::optional<std::vector<std::pair<int64_t, std::optional<int64_t>>>>;
  auto test =
      [&](const map_t& map1, const map_t& map2, std::optional<bool> expected) {
        auto vector1 = makeNullableMapVector<int64_t, int64_t>({map1});
        auto vector2 = makeNullableMapVector<int64_t, int64_t>({map2});

        auto eqResult = evaluate<SimpleVector<bool>>(
            "c0 == c1", makeRowVector({vector1, vector2}));

        auto neqResult = evaluate<SimpleVector<bool>>(
            "c0 != c1", makeRowVector({vector1, vector2}));

        ASSERT_EQ(expected.has_value(), !eqResult->isNullAt(0));
        ASSERT_EQ(expected.has_value(), !neqResult->isNullAt(0));
        if (expected.has_value()) {
          ASSERT_EQ(expected.value(), eqResult->valueAt(0));
          ASSERT_EQ(!expected.value(), neqResult->valueAt(0));
        }
      };

  // eq and neq function test
  test({{{1, 2}, {3, 4}}}, {{{1, 2}, {3, 4}}}, true);

  // Elements checked in sorted order.
  test({{{3, 4}, {1, 2}}}, {{{1, 2}, {3, 4}}}, true);

  test(common::testutil::optionalEmpty, common::testutil::optionalEmpty, true);

  test({{{1, 2}, {3, 5}}}, {{{1, 2}, {3, 4}}}, false);

  test({{{1, 2}, {3, 4}}}, {{{11, 2}, {3, 4}}}, false);

  // Null map entries.
  test(std::nullopt, {{{1, 2}, {3, 4}}}, std::nullopt);
  test({{{1, 2}, {3, 4}}}, std::nullopt, std::nullopt);

  // Null in values should be read.
  test({{{1, std::nullopt}, {3, 4}}}, {{{1, 2}, {3, 4}}}, std::nullopt);

  test({{{1, 2}, {3, 4}}}, {{{1, 2}, {3, std::nullopt}}}, std::nullopt);

  // Compare will find results before reading null.

  // Keys are same, but first value is different.
  test({{{1, 2}, {3, 4}}}, {{{1, 100}, {3, std::nullopt}}}, false);
  test({{{3, 4}, {1, 2}}}, {{{3, std::nullopt}, {1, 100}}}, false);

  // Keys are different.
  test(
      {{{1, std::nullopt}, {2, std::nullopt}}},
      {{{1, std::nullopt}, {3, std::nullopt}}},
      false);
  test(
      {{{2, std::nullopt}, {1, std::nullopt}}},
      {{{1, std::nullopt}, {3, std::nullopt}}},
      false);

  // Different sizes.
  test({{{1, 2}, {10, std::nullopt}}}, {{{1, std::nullopt}}}, false);
  test({{{1, 2}, {10, std::nullopt}}}, {{{1, std::nullopt}}}, false);
}

TEST_F(ComparisonsTest, eqRow) {
  auto test =
      [&](const std::tuple<std::optional<int64_t>, std::optional<int64_t>>&
              row1,
          const std::tuple<std::optional<int64_t>, std::optional<int64_t>>&
              row2,
          std::optional<bool> expected) {
        auto vector1 = vectorMaker_.rowVector(
            {vectorMaker_.flatVectorNullable<int64_t>({std::get<0>(row1)}),
             vectorMaker_.flatVectorNullable<int64_t>({std::get<1>(row1)})});

        auto vector2 = vectorMaker_.rowVector(
            {vectorMaker_.flatVectorNullable<int64_t>({std::get<0>(row2)}),
             vectorMaker_.flatVectorNullable<int64_t>({std::get<1>(row2)})});

        auto result = evaluate<SimpleVector<bool>>(
            "c0 == c1", makeRowVector({vector1, vector2}));

        ASSERT_EQ(expected.has_value(), !result->isNullAt(0));

        if (expected.has_value()) {
          ASSERT_EQ(expected.value(), result->valueAt(0));
        }
      };

  test({1, 2}, {2, 3}, false);
  test({1, 2}, {1, 2}, true);

  test({2, std::nullopt}, {1, 2}, false);

  test({1, 2}, {1, std::nullopt}, std::nullopt);
  test({1, std::nullopt}, {1, 2}, std::nullopt);
  test({1, 2}, {std::nullopt, 2}, std::nullopt);
}

TEST_F(ComparisonsTest, gtLtRow) {
  // Row Comparison in order of >, >=, <, <=

  // Row<int64_t, int64_t>
  std::vector<std::optional<bool>> expectedResults = {false, false, true, true};
  testCompareRow<int64_t, int64_t>({1, 2}, {2, 3}, expectedResults);

  expectedResults = {false, true, false, true};
  testCompareRow<int64_t, int64_t>({1, 2}, {1, 2}, expectedResults);

  // Row<int64_t, int64_t> with nulls
  // User Exception thrown when nulls encountered before result is determined.
  auto expectedException = "Ordering nulls is not supported";
  testCompareRow<int64_t, int64_t>(
      {1, std::nullopt}, {1, 2}, std::nullopt, expectedException);

  // Row<double, double>
  expectedResults = {false, false, true, true};
  testCompareRow<double, double>({1.0, 2.0}, {1.0, 3.0}, expectedResults);

  // Row<double, double> with NaNs
  // NaNs are considered larger than all other numbers
  static const auto NaN = std::numeric_limits<double>::quiet_NaN();
  expectedResults = {true, true, false, false};
  testCompareRow<double, double>({NaN, 0.0}, {2.0, 1.0}, expectedResults);

  expectedResults = {false, false, true, true};
  testCompareRow<double, double>({NaN, 0.0}, {NaN, 1.0}, expectedResults);

  // Row<int64_t, double>
  expectedResults = {false, false, true, true};
  testCompareRow<int64_t, double>({1, 3.0}, {2, 1.0}, expectedResults);

  // Row<int64_t, Timestamp>
  const Timestamp ts1(998474645, 321000000);
  const Timestamp ts2(998474645, 321000001);
  expectedResults = {false, false, true, true};
  testCompareRow<int64_t, Timestamp>({1, ts1}, {1, ts2}, expectedResults);

  // Comparing two rows of different types or different sizes is invalid
  expectedException = "Scalar function signature is not supported";

  // Row<int64_t, int64_t>, Row<double, double>
  testCompareRowDifferentTypesAndSizes<int64_t, double>(
      {1, 1}, {1.0, 1.2}, std::nullopt, expectedException);
  // Row<int64_t, int64_t>, Row<Timestamp, Timestamp>
  testCompareRowDifferentTypesAndSizes<int64_t, Timestamp>(
      {1, 1}, {ts1, ts2}, std::nullopt, expectedException);
}

TEST_F(ComparisonsTest, gtLtArray) {
  // Array Comparison in order of >, >=, <, <=

  // Array<int64_t>
  std::vector<std::optional<bool>> expectedResults = {false, true, false, true};
  testCompareArray<int64_t>({1, 2}, {1, 2}, expectedResults);

  expectedResults = {false, false, true, true};
  testCompareArray<int64_t>({1, 2}, {1, 3}, expectedResults);

  // Array<int64_t> of different sizes
  expectedResults = {false, false, true, true};
  testCompareArray<int64_t>({1, 2}, {1, 2, 3}, expectedResults);

  expectedResults = {true, true, false, false};
  testCompareArray<int64_t>({1, 3}, {1, 2, 3}, expectedResults);

  // Array<int64_t> with nulls
  // When nulls present, compare based on CompareFlag rules.
  expectedResults = {true, true, false, false};
  testCompareArray<int64_t>({1, std::nullopt}, {1}, expectedResults);

  expectedResults = {false, false, true, true};
  testCompareArray<int64_t>(
      {1, std::nullopt}, {2, std::nullopt}, expectedResults);

  // User Exception thrown when nulls encountered before result is determined.
  auto expectedException = "Ordering nulls is not supported";
  testCompareArray<int64_t>(
      {1, std::nullopt, std::nullopt},
      {1, 2, 3},
      std::nullopt,
      expectedException);

  // Array<double>
  expectedResults = {false, false, true, true};
  testCompareArray<double>({1.0, 2.0}, {1.1, 1.9}, expectedResults);

  static const auto NaN = std::numeric_limits<double>::quiet_NaN();
  expectedResults = {false, true, false, true};
  testCompareArray<double>({NaN, NaN}, {NaN, NaN}, expectedResults);

  // Array<double>  with NaNs
  // NaNs are considered larger than all other numbers
  expectedResults = {true, true, false, false};
  testCompareArray<double>({NaN}, {3.0}, expectedResults);

  expectedResults = {false, false, true, true};
  testCompareArray<double>({NaN}, {NaN, 3.0}, expectedResults);
}

TEST_F(ComparisonsTest, distinctFrom) {
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({3, 1, 1, std::nullopt, std::nullopt}),
      makeNullableFlatVector<int64_t>({3, 2, std::nullopt, 2, std::nullopt}),
  });

  auto result = evaluate("c0 is distinct from c1", input);
  auto expected = makeFlatVector<bool>({false, true, true, true, false});
  test::assertEqualVectors(expected, result);

  const Timestamp ts1(998474645, 321000000);
  const Timestamp ts2(998423705, 321000000);
  const Timestamp ts3(400000000, 123000000);
  input = makeRowVector({
      makeNullableFlatVector<Timestamp>(
          {ts3, ts1, ts1, std::nullopt, std::nullopt}),
      makeNullableFlatVector<Timestamp>(
          {ts3, ts2, std::nullopt, ts2, std::nullopt}),
  });

  result = evaluate("c0 is distinct from c1", input);
  expected = makeFlatVector<bool>({false, true, true, true, false});
  test::assertEqualVectors(expected, result);

  input = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3]",
          "null",
          "[]",
          "[1, null]",
          "[null, 2, 3]",
          "null",
      }),
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 4]",
          "null",
          "[]",
          "[1, null]",
          "[null, 2, 4]",
          "[]",
      }),
  });

  result = evaluate("c0 is distinct from c1", input);
  expected = makeFlatVector<bool>({true, false, false, false, true, true});
  test::assertEqualVectors(expected, result);
}

TEST_F(ComparisonsTest, eqNestedComplex) {
  // Compare Row(Array<Array<int>>, int, Map<int, int>)
  using array_type = std::optional<std::vector<std::optional<int64_t>>>;
  array_type array1 = {{1, 2}};
  array_type array2 = common::testutil::optionalEmpty;
  array_type array3 = {{1, 100, 2}};

  auto vector1 =
      makeNullableNestedArrayVector<int64_t>({{{array1, array2, array3}}});
  auto vector2 = makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6});
  auto vector3 = makeMapVector<int64_t, int64_t>({{{1, 2}, {3, 4}}});
  auto row1 = makeRowVector({vector1, vector2, vector3});

  // True result.
  auto result =
      evaluate<SimpleVector<bool>>("c0 == c1", makeRowVector({row1, row1}));
  ASSERT_EQ(result->isNullAt(0), false);
  ASSERT_EQ(result->valueAt(0), true);

  // False result.
  {
    auto differentMap = makeMapVector<int64_t, int64_t>({{{1, 2}, {3, 10}}});
    auto row2 = makeRowVector({vector1, vector2, differentMap});

    auto result =
        evaluate<SimpleVector<bool>>("c0 == c1", makeRowVector({row1, row2}));
    ASSERT_EQ(result->isNullAt(0), false);
    ASSERT_EQ(result->valueAt(0), false);
  }

  // Null result.
  {
    auto mapWithNull =
        makeMapVector<int64_t, int64_t>({{{1, 2}, {3, std::nullopt}}});
    auto row2 = makeRowVector({vector1, vector2, mapWithNull});

    auto result =
        evaluate<SimpleVector<bool>>("c0 == c1", makeRowVector({row1, row2}));
    ASSERT_EQ(result->isNullAt(0), true);
  }
}

TEST_F(ComparisonsTest, overflowTest) {
  auto makeFlatVector = [&](size_t numRows, size_t delta) {
    BufferPtr values =
        AlignedBuffer::allocate<int64_t>(numRows + delta, pool());
    auto rawValues = values->asMutable<int64_t>();
    for (size_t i = 0; i < numRows + delta; ++i) {
      rawValues[i] = i;
    }
    return std::make_shared<FlatVector<int64_t>>(
        pool(), BIGINT(), nullptr, numRows, values, std::vector<BufferPtr>{});
  };

  size_t numRows = 1006;
  size_t delta = 2;
  auto rowVector = makeRowVector(
      {makeFlatVector(numRows, delta), makeFlatVector(numRows, delta)});
  auto result =
      evaluate<SimpleVector<bool>>(fmt::format("{}(c0, c1)", "eq"), rowVector);
  for (auto i = 0; i < result->size(); ++i) {
    ASSERT_TRUE(result->valueAt(i));
  }

  auto flatResult = result->asFlatVector<bool>();
  auto rawResult = flatResult->mutableRawValues();
  for (auto i = 0; i < result->size(); ++i) {
    ASSERT_TRUE(
        bits::isBitSet(reinterpret_cast<const uint64_t*>(rawResult), i));
  }
  for (auto i = result->size(); i < result->size() + delta; ++i) {
    ASSERT_FALSE(
        bits::isBitSet(reinterpret_cast<const uint64_t*>(rawResult), i));
  }
}

TEST_F(ComparisonsTest, nanComparison) {
  registerSimpleComparisonFunctions();
  static const auto kNaN = std::numeric_limits<double>::quiet_NaN();
  static const auto kSNaN = std::numeric_limits<double>::signaling_NaN();
  static const auto kInf = std::numeric_limits<double>::infinity();

  auto testNaN =
      [&](std::string prefix, RowVectorPtr rowVector, bool primitiveInput) {
        auto eval = [&](const std::string& expr,
                        const std::string& lhs,
                        const std::string& rhs) {
          return evaluate<SimpleVector<bool>>(
              fmt::format("{}({}, {})", expr, lhs, rhs), rowVector);
        };

        auto allFalse = makeFlatVector<bool>({false, false});
        auto allTrue = makeFlatVector<bool>({true, true});

        // NaN compared with NaN (multiple binary representations)
        test::assertEqualVectors(eval(prefix + "eq", "c0", "c1"), allTrue);
        test::assertEqualVectors(eval(prefix + "neq", "c0", "c1"), allFalse);
        if (primitiveInput) {
          test::assertEqualVectors(eval(prefix + "lt", "c0", "c1"), allFalse);
          test::assertEqualVectors(eval(prefix + "gt", "c0", "c1"), allFalse);
          test::assertEqualVectors(eval(prefix + "lte", "c0", "c1"), allTrue);
          test::assertEqualVectors(eval(prefix + "gte", "c0", "c1"), allTrue);
          // NaN between Infinity and NaN
          test::assertEqualVectors(
              evaluate<SimpleVector<bool>>("c0 BETWEEN c2 and c1", rowVector),
              allTrue);
          // NaN distinct from NaN
          test::assertEqualVectors(
              evaluate<SimpleVector<bool>>("c0 IS DISTINCT FROM c1", rowVector),
              allFalse);
        }

        // NaN compared with Inf
        test::assertEqualVectors(eval(prefix + "eq", "c0", "c2"), allFalse);
        test::assertEqualVectors(eval(prefix + "neq", "c0", "c2"), allTrue);
        if (primitiveInput) {
          test::assertEqualVectors(eval(prefix + "lt", "c0", "c2"), allFalse);
          test::assertEqualVectors(eval(prefix + "gt", "c0", "c2"), allTrue);
          test::assertEqualVectors(eval(prefix + "lte", "c0", "c2"), allFalse);
          test::assertEqualVectors(eval(prefix + "gte", "c0", "c2"), allTrue);
          // NaN between 0 and Infinity
          test::assertEqualVectors(
              evaluate<SimpleVector<bool>>(
                  "c0 BETWEEN cast(0 as double) and c2", rowVector),
              allFalse);
          // NaN distinct from Infinity
          test::assertEqualVectors(
              evaluate<SimpleVector<bool>>("c0 IS DISTINCT FROM c2", rowVector),
              allTrue);
        }
      };

  // Primitive type input
  auto input = makeRowVector(
      {makeFlatVector<double>({kNaN, kSNaN}),
       makeFlatVector<double>({kNaN, kNaN}),
       makeFlatVector<double>({kInf, kInf})});
  // Test the Vector function ComparisonSimdFunction.
  testNaN("", input, true);
  // Test the Simple functions.
  testNaN("simple_", input, true);

  // Complex type input
  input = makeRowVector({
      makeRowVector({
          makeFlatVector<double>({kNaN, kSNaN}),
          makeFlatVector<int32_t>({1, 1}),
      }),
      makeRowVector({
          makeFlatVector<double>({kNaN, kNaN}),
          makeFlatVector<int32_t>({1, 1}),
      }),
      makeRowVector({
          makeFlatVector<double>({kInf, kInf}),
          makeFlatVector<int32_t>({1, 1}),
      }),
  });
  // Note: Complex comparison functions are only registered as simple functions.
  testNaN("", input, false);
}

TEST_F(ComparisonsTest, TimestampWithTimezone) {
  auto makeTimestampWithTimezone = [](int64_t millis, const std::string& tz) {
    return pack(millis, tz::getTimeZoneID(tz));
  };

  auto lhs = makeFlatVector<int64_t>(
      {makeTimestampWithTimezone(1639426440000, "+01:00"),
       makeTimestampWithTimezone(1639426440000, "+01:00"),
       makeTimestampWithTimezone(1639426440000, "+03:00"),
       makeTimestampWithTimezone(1549770072000, "+01:00"),
       makeTimestampWithTimezone(1549770072000, "+01:00"),
       makeTimestampWithTimezone(1549770072000, "+03:00"),
       makeTimestampWithTimezone(1639426440000, "+01:00"),
       makeTimestampWithTimezone(1639426440000, "+01:00"),
       makeTimestampWithTimezone(1639426440000, "+03:00"),
       makeTimestampWithTimezone(-1639426440000, "+01:00"),
       makeTimestampWithTimezone(-1539426440000, "+03:00"),
       makeTimestampWithTimezone(-1639426440000, "-14:00"),
       makeTimestampWithTimezone(1639426440000, "+03:00"),
       makeTimestampWithTimezone(-1639426440000, "-14:00")},
      TIMESTAMP_WITH_TIME_ZONE());

  auto rhs = makeFlatVector<int64_t>(
      {makeTimestampWithTimezone(1639426440000, "+03:00"),
       makeTimestampWithTimezone(1639426440000, "-14:00"),
       makeTimestampWithTimezone(1639426440000, "-14:00"),
       makeTimestampWithTimezone(1639426440000, "+03:00"),
       makeTimestampWithTimezone(1639426440000, "-14:00"),
       makeTimestampWithTimezone(1639426440000, "-14:00"),
       makeTimestampWithTimezone(1549770072000, "+03:00"),
       makeTimestampWithTimezone(1549770072000, "-14:00"),
       makeTimestampWithTimezone(1549770072000, "-14:00"),
       makeTimestampWithTimezone(-1639426440000, "+01:00"),
       makeTimestampWithTimezone(-1639426440000, "-14:00"),
       makeTimestampWithTimezone(-1539426440000, "+03:00"),
       makeTimestampWithTimezone(-1639426440000, "+03:00"),
       makeTimestampWithTimezone(1639426440000, "+01:00")},
      TIMESTAMP_WITH_TIME_ZONE());

  auto input = makeRowVector({lhs, rhs});

  auto eval = [&](const std::string& expr) {
    return evaluate<SimpleVector<bool>>(fmt::format("c0 {} c1", expr), input);
  };

  test::assertEqualVectors(
      eval("="),
      makeFlatVector<bool>(
          {true,
           true,
           true,
           false,
           false,
           false,
           false,
           false,
           false,
           true,
           false,
           false,
           false,
           false}));
  test::assertEqualVectors(
      eval("<>"),
      makeFlatVector<bool>(
          {false,
           false,
           false,
           true,
           true,
           true,
           true,
           true,
           true,
           false,
           true,
           true,
           true,
           true}));
  test::assertEqualVectors(
      eval("<"),
      makeFlatVector<bool>(
          {false,
           false,
           false,
           true,
           true,
           true,
           false,
           false,
           false,
           false,
           false,
           true,
           false,
           true}));
  test::assertEqualVectors(
      eval(">"),
      makeFlatVector<bool>(
          {false,
           false,
           false,
           false,
           false,
           false,
           true,
           true,
           true,
           false,
           true,
           false,
           true,
           false}));
  test::assertEqualVectors(
      eval("<="),
      makeFlatVector<bool>(
          {true,
           true,
           true,
           true,
           true,
           true,
           false,
           false,
           false,
           true,
           false,
           true,
           false,
           true}));
  test::assertEqualVectors(
      eval(">="),
      makeFlatVector<bool>(
          {true,
           true,
           true,
           false,
           false,
           false,
           true,
           true,
           true,
           true,
           true,
           false,
           true,
           false}));
  test::assertEqualVectors(
      eval("is distinct from"),
      makeFlatVector<bool>(
          {false,
           false,
           false,
           true,
           true,
           true,
           true,
           true,
           true,
           false,
           true,
           true,
           true,
           true}));

  auto betweenInput = makeRowVector({
      makeFlatVector<int64_t>(
          {makeTimestampWithTimezone(1639426440000, "+01:00"),
           makeTimestampWithTimezone(1639426440000, "+01:00"),
           makeTimestampWithTimezone(1639426440000, "+01:00"),
           makeTimestampWithTimezone(1639426440000, "+01:00"),
           makeTimestampWithTimezone(1639426440000, "+01:00"),
           makeTimestampWithTimezone(1639426440000, "+03:00"),
           makeTimestampWithTimezone(1539426440000, "+03:00"),
           makeTimestampWithTimezone(1739426440000, "+01:00"),
           makeTimestampWithTimezone(-1639426440000, "+01:00"),
           makeTimestampWithTimezone(-1639426440000, "+03:00"),
           makeTimestampWithTimezone(1639426440000, "-14:00"),
           makeTimestampWithTimezone(-1739426440000, "+01:00"),
           makeTimestampWithTimezone(1639426440000, "-14:00")},
          TIMESTAMP_WITH_TIME_ZONE()),
      makeFlatVector<int64_t>(
          {makeTimestampWithTimezone(1639426440000, "+01:00"),
           makeTimestampWithTimezone(1639426440000, "+03:00"),
           makeTimestampWithTimezone(1539426440000, "+01:00"),
           makeTimestampWithTimezone(1539426440000, "+03:00"),
           makeTimestampWithTimezone(1539426440000, "+03:00"),
           makeTimestampWithTimezone(1539426440000, "+03:00"),
           makeTimestampWithTimezone(1639426440000, "+01:00"),
           makeTimestampWithTimezone(1539426440000, "+03:00"),
           makeTimestampWithTimezone(-1739426440000, "+01:00"),
           makeTimestampWithTimezone(-1739426440000, "+01:00"),
           makeTimestampWithTimezone(-1639426440000, "+01:00"),
           makeTimestampWithTimezone(-1639426440000, "+01:00"),
           makeTimestampWithTimezone(-1639426440000, "+01:00")},
          TIMESTAMP_WITH_TIME_ZONE()),
      makeFlatVector<int64_t>(
          {makeTimestampWithTimezone(1739426440000, "+01:00"),
           makeTimestampWithTimezone(1739426440000, "+01:00"),
           makeTimestampWithTimezone(1639426440000, "+01:00"),
           makeTimestampWithTimezone(1639426440000, "+03:00"),
           makeTimestampWithTimezone(1739426440000, "+03:00"),
           makeTimestampWithTimezone(1739426440000, "+03:00"),
           makeTimestampWithTimezone(1739426440000, "+01:00"),
           makeTimestampWithTimezone(1639426440000, "+01:00"),
           makeTimestampWithTimezone(-1539426440000, "+03:00"),
           makeTimestampWithTimezone(1539426440000, "+03:00"),
           makeTimestampWithTimezone(1739426440000, "+01:00"),
           makeTimestampWithTimezone(-1539426440000, "+03:00"),
           makeTimestampWithTimezone(-1539426440000, "+03:00")},
          TIMESTAMP_WITH_TIME_ZONE()),
  });

  test::assertEqualVectors(
      evaluate<SimpleVector<bool>>(
          fmt::format("c0 between c1 and c2"), betweenInput),
      makeFlatVector<bool>(
          {true,
           true,
           true,
           true,
           true,
           true,
           false,
           false,
           true,
           true,
           true,
           false,
           false}));
}

TEST_F(ComparisonsTest, IPPrefixType) {
  auto ipprefix = [](const std::string& ipprefixString) {
    auto tryIpPrefix = ipaddress::tryParseIpPrefixString(ipprefixString);
    return variant::row(
        {tryIpPrefix.value().first, tryIpPrefix.value().second});
  };

  // Comparison Operator Tests
  {
    auto left = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.5/32")},
         {ipprefix("1.2.0.0/25")},
         {ipprefix("::1/128")},
         {ipprefix("::1/128")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/80")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")}});
    auto right = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.0.0/24")},
         {ipprefix("::1/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8321/128")}});
    auto expected = makeFlatVector<bool>(
        {false, false, true, true, true, false, false, true, true});
    auto result = evaluate("c0 > c1", makeRowVector({left, right}));
    test::assertEqualVectors(result, expected);
  }

  {
    auto left = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.5/32")},
         {ipprefix("1.2.0.0/25")},
         {ipprefix("::1/128")},
         {ipprefix("::1/128")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/80")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")}});
    auto right = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.0.0/24")},
         {ipprefix("::1/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8321/128")}});
    auto expected = makeFlatVector<bool>(
        {false, true, false, false, false, false, true, false, false});
    auto result = evaluate("c0 < c1", makeRowVector({left, right}));
    test::assertEqualVectors(result, expected);
  }

  {
    auto left = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.5/32")},
         {ipprefix("1.2.0.0/25")},
         {ipprefix("::1/128")},
         {ipprefix("::1/128")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/80")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")}});
    auto right = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.0.0/24")},
         {ipprefix("::1/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8321/128")}});
    auto expected = makeFlatVector<bool>(
        {true, false, true, true, true, true, false, true, true});
    auto result = evaluate("c0 >= c1", makeRowVector({left, right}));
    test::assertEqualVectors(result, expected);
  }

  {
    auto left = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.5/32")},
         {ipprefix("1.2.0.0/25")},
         {ipprefix("::1/128")},
         {ipprefix("::1/128")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/80")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")}});
    auto right = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.0.0/24")},
         {ipprefix("::1/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8321/128")}});
    auto expected = makeFlatVector<bool>(
        {true, true, false, false, false, true, true, false, false});
    auto result = evaluate("c0 <= c1", makeRowVector({left, right}));
    test::assertEqualVectors(result, expected);
  }

  {
    auto left = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.0.0/25")},
         {ipprefix("::1/128")},
         {ipprefix("::1/128")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/80")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/24")}});
    auto right = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.0.0/24")},
         {ipprefix("::1/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8321/128")},
         {ipprefix("1.2.3.5/24")}});
    auto expected = makeFlatVector<bool>(
        {true, false, false, false, true, false, false, false, true});
    auto result = evaluate("c0 = c1", makeRowVector({left, right}));
    test::assertEqualVectors(result, expected);
  }

  {
    auto left = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.5/32")},
         {ipprefix("1.2.0.0/25")},
         {ipprefix("::1/128")},
         {ipprefix("::1/128")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/80")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")}});
    auto right = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.0.0/24")},
         {ipprefix("::1/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8321/128")}});
    auto expected = makeFlatVector<bool>(
        {false, true, true, true, true, false, true, true, true});
    auto result = evaluate("c0 <> c1", makeRowVector({left, right}));
    test::assertEqualVectors(result, expected);
  }

  // Distinct from test
  {
    auto left = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.5/32")},
         {ipprefix("1.2.0.0/25")},
         {ipprefix("::1/128")},
         {ipprefix("::1/128")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/80")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")}});
    auto right = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("1.2.0.0/24")},
         {ipprefix("::1/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8321/128")}});
    auto expected = makeFlatVector<bool>(
        {false, true, true, true, true, false, true, true, true});
    auto result =
        evaluate("c0 is distinct from c1", makeRowVector({left, right}));
    test::assertEqualVectors(result, expected);
  }

  // Inbetween test
  {
    auto inbetween = makeArrayOfRowVector(
        IPPREFIX(),
        {{ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.4/32")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("1.2.3.5/32")},
         {ipprefix("1.2.0.0/25")},
         {ipprefix("::1/128")},
         {ipprefix("::1/128")},
         {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/80")},
         {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
         {ipprefix("::1/128")},
         {ipprefix("::2222/128")}

        });
    auto left = makeArrayOfRowVector(
        IPPREFIX(),
        {
            {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
            {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
            {ipprefix("1.2.3.4/32")},
            {ipprefix("1.2.3.4/32")},
            {ipprefix("1.2.0.0/24")},
            {ipprefix("::1/128")},
            {ipprefix("1.2.3.4/32")},
            {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64")},
            {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8321/128")},
            {ipprefix("::1/128")},
            {ipprefix("::1/128")},
        });
    auto right = makeArrayOfRowVector(
        IPPREFIX(),
        {
            {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
            {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8329/128")},
            {ipprefix("1.2.3.4/32")},
            {ipprefix("1.2.3.4/32")},
            {ipprefix("1.2.0.0/24")},
            {ipprefix("::1/128")},
            {ipprefix("1.2.3.4/32")},
            {ipprefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64")},
            {ipprefix("2001:0db8:0000:0000:0000:ff00:0042:8321/128")},
            {ipprefix("::1234/128")},
            {ipprefix("::1234/128")},

        });
    auto expected = makeFlatVector<bool>(
        {true,
         false,
         false,
         false,
         false,
         true,
         false,
         false,
         false,
         true,
         false});

    auto result = evaluate(
        "c0 between c1 and c2",
        makeRowVector(
            {inbetween->elements(), left->elements(), right->elements()}));
    test::assertEqualVectors(result, expected);
  }
}

TEST_F(ComparisonsTest, IpAddressType) {
  auto makeIpAdressFromString = [](const std::string& ipAddr) -> int128_t {
    auto ret = ipaddress::tryGetIPv6asInt128FromString(ipAddr);
    return ret.value();
  };

  auto runAndCompare = [&](const std::string& expr,
                           RowVectorPtr inputs,
                           VectorPtr expectedResult) {
    auto actual = evaluate<SimpleVector<bool>>(expr, inputs);
    test::assertEqualVectors(expectedResult, actual);
  };

  auto lhs = makeNullableFlatVector<int128_t>(
      {
          makeIpAdressFromString("1.1.1.1"),
          makeIpAdressFromString("255.255.255.255"),
          makeIpAdressFromString("1.2.3.4"),
          makeIpAdressFromString("1.1.1.2"),
          makeIpAdressFromString("1.1.2.1"),
          makeIpAdressFromString("1.1.1.1"),
          makeIpAdressFromString("1.1.1.1"),
          makeIpAdressFromString("::1"),
          makeIpAdressFromString("2001:0db8:0000:0000:0000:ff00:0042:8329"),
          makeIpAdressFromString("::ffff:1.2.3.4"),
          makeIpAdressFromString("::ffff:0.1.1.1"),
          makeIpAdressFromString("::FFFF:FFFF:FFFF"),
          makeIpAdressFromString("::0001:255.255.255.255"),
          makeIpAdressFromString("::ffff:ffff:ffff"),
          std::nullopt,
          makeIpAdressFromString("::0001:255.255.255.255"),
      },
      IPADDRESS());

  auto rhs = makeNullableFlatVector<int128_t>(
      {
          makeIpAdressFromString("1.1.1.1"),
          makeIpAdressFromString("255.255.255.255"),
          makeIpAdressFromString("1.1.1.1"),
          makeIpAdressFromString("1.1.1.1"),
          makeIpAdressFromString("1.1.1.2"),
          makeIpAdressFromString("1.1.2.1"),
          makeIpAdressFromString("255.1.1.1"),
          makeIpAdressFromString("::1"),
          makeIpAdressFromString("2001:db8::ff00:42:8329"),
          makeIpAdressFromString("1.2.3.4"),
          makeIpAdressFromString("::ffff:1.1.1.0"),
          makeIpAdressFromString("::0001:255.255.255.255"),
          makeIpAdressFromString("255.255.255.255"),
          makeIpAdressFromString("255.255.255.255"),
          makeIpAdressFromString("255.255.255.255"),
          std::nullopt,
      },
      IPADDRESS());

  auto input = makeRowVector({lhs, rhs});

  runAndCompare(
      "c0 = c1",
      input,
      makeNullableFlatVector<bool>(
          {true,
           true,
           false,
           false,
           false,
           false,
           false,
           true,
           true,
           true,
           false,
           false,
           false,
           true,
           std::nullopt,
           std::nullopt}));

  runAndCompare(
      "c0 <> c1",
      input,
      makeNullableFlatVector<bool>(
          {false,
           false,
           true,
           true,
           true,
           true,
           true,
           false,
           false,
           false,
           true,
           true,
           true,
           false,
           std::nullopt,
           std::nullopt}));

  runAndCompare(
      "c0 < c1",
      input,
      makeNullableFlatVector<bool>(
          {false,
           false,
           false,
           false,
           false,
           true,
           true,
           false,
           false,
           false,
           true,
           false,
           true,
           false,
           std::nullopt,
           std::nullopt}));

  runAndCompare(
      "c0 > c1",
      input,
      makeNullableFlatVector<bool>(
          {false,
           false,
           true,
           true,
           true,
           false,
           false,
           false,
           false,
           false,
           false,
           true,
           false,
           false,
           std::nullopt,
           std::nullopt}));

  runAndCompare(
      "c0 <= c1",
      input,
      makeNullableFlatVector<bool>(
          {true,
           true,
           false,
           false,
           false,
           true,
           true,
           true,
           true,
           true,
           true,
           false,
           true,
           true,
           std::nullopt,
           std::nullopt}));

  runAndCompare(
      "c0 >= c1",
      input,
      makeNullableFlatVector<bool>(
          {true,
           true,
           true,
           true,
           true,
           false,
           false,
           true,
           true,
           true,
           false,
           true,
           false,
           true,
           std::nullopt,
           std::nullopt}));

  runAndCompare(
      "c0 is distinct from c1",
      input,
      makeNullableFlatVector<bool>(
          {false,
           false,
           true,
           true,
           true,
           true,
           true,
           false,
           false,
           false,
           true,
           true,
           true,
           false,
           true,
           true}));

  auto betweenInput = makeRowVector({
      makeNullableFlatVector<int128_t>(
          {makeIpAdressFromString("2001:db8::ff00:42:8329"),
           makeIpAdressFromString("1.1.1.1"),
           makeIpAdressFromString("255.255.255.255"),
           makeIpAdressFromString("::ffff:1.1.1.1"),
           makeIpAdressFromString("1.1.1.1"),
           makeIpAdressFromString("0.0.0.0"),
           makeIpAdressFromString("::ffff"),
           makeIpAdressFromString("0.0.0.0"),
           std::nullopt,
           makeIpAdressFromString("0.0.0.0"),
           makeIpAdressFromString("0.0.0.0")},
          IPADDRESS()),
      makeNullableFlatVector<int128_t>(
          {makeIpAdressFromString("::ffff"),
           makeIpAdressFromString("1.1.1.1"),
           makeIpAdressFromString("255.255.255.255"),
           makeIpAdressFromString("::ffff:0.1.1.1"),
           makeIpAdressFromString("0.1.1.1"),
           makeIpAdressFromString("0.0.0.1"),
           makeIpAdressFromString("::ffff:0.0.0.1"),
           makeIpAdressFromString("2001:db8::0:0:0:1"),
           makeIpAdressFromString("2001:db8::0:0:0:1"),
           std::nullopt,
           makeIpAdressFromString("2001:db8::0:0:0:1")},
          IPADDRESS()),
      makeNullableFlatVector<int128_t>(
          {makeIpAdressFromString("2001:db8::ff00:42:8329"),
           makeIpAdressFromString("1.1.1.1"),
           makeIpAdressFromString("255.255.255.255"),
           makeIpAdressFromString("2001:0db8:0000:0000:0000:ff00:0042:8329"),
           makeIpAdressFromString("2001:0db8:0000:0000:0000:ff00:0042:8329"),
           makeIpAdressFromString("0.0.0.2"),
           makeIpAdressFromString("0.0.0.2"),
           makeIpAdressFromString("2001:db8::1:0:0:1"),
           makeIpAdressFromString("2001:db8::1:0:0:1"),
           makeIpAdressFromString("2001:db8::1:0:0:1"),
           std::nullopt},
          IPADDRESS()),
  });

  runAndCompare(
      "c0 between c1 and c2",
      betweenInput,
      makeNullableFlatVector<bool>(
          {true,
           true,
           true,
           true,
           true,
           false,
           false,
           false,
           std::nullopt,
           std::nullopt,
           std::nullopt}));
}

TEST_F(ComparisonsTest, CustomComparisonWithGenerics) {
  // Tests that functions that support signatures with generics handle custom
  // comparison correctly.
  auto input = makeRowVector({
      makeFlatVector<int64_t>(
          {0, 1}, test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON()),
      makeFlatVector<int64_t>(
          {256, 258}, test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON()),
  });

  test::assertEqualVectors(
      evaluate<SimpleVector<bool>>("c0 = c1", input),
      makeFlatVector<bool>({true, false}));
  test::assertEqualVectors(
      evaluate<SimpleVector<bool>>("c0 <> c1", input),
      makeFlatVector<bool>({false, true}));
  test::assertEqualVectors(
      evaluate<SimpleVector<bool>>("c0 is distinct from c1", input),
      makeFlatVector<bool>({false, true}));
}

namespace {
template <typename Tp, typename Op, const char* fnName>
struct ComparisonTypeOp {
  typedef Tp type;
  typedef Op fn;
  std::string sqlFunction{fnName};

  static std::string toString() {
    return fmt::format("{}:{}", Tp().toString(), fnName);
  }
};

const char eq[] = "eq";
const char neq[] = "neq";
const char lt[] = "lt";
const char gt[] = "gt";
const char lte[] = "lte";
const char gte[] = "gte";

#define ComparisonTypes(Tp)                           \
  ComparisonTypeOp<Tp, std::equal_to<>, eq>,          \
      ComparisonTypeOp<Tp, std::not_equal_to<>, neq>, \
      ComparisonTypeOp<Tp, std::less<>, lt>,          \
      ComparisonTypeOp<Tp, std::greater<>, gt>,       \
      ComparisonTypeOp<Tp, std::less_equal<>, lte>,   \
      ComparisonTypeOp<Tp, std::greater_equal<>, gte>

typedef ::testing::Types<
    ComparisonTypes(TinyintType),
    ComparisonTypes(SmallintType),
    ComparisonTypes(IntegerType),
    ComparisonTypes(BigintType),
    ComparisonTypes(RealType),
    ComparisonTypes(DoubleType)>
    comparisonTypes;
} // namespace

template <typename ComparisonTypeOp>
class SimdComparisonsTest : public functions::test::FunctionBaseTest {
 public:
  using T = typename ComparisonTypeOp::type::NativeType;
  using ComparisonOp = typename ComparisonTypeOp::fn;
  const std::string sqlFn = ComparisonTypeOp().sqlFunction;

  auto checkConstantComparison(
      const std::vector<T>& data,
      T constVal,
      bool constantRhs = true) {
    auto vector1 = makeFlatVector<T>(data);
    auto vector2 = makeConstant<T>(constVal, vector1->size());
    auto rowVector = constantRhs ? makeRowVector({vector1, vector2})
                                 : makeRowVector({vector2, vector1});
    return evaluate<SimpleVector<bool>>(
        fmt::format("{}(c0, c1)", sqlFn), rowVector);
  }

  void vectorComparisonImpl(
      const std::vector<T>& lhs,
      const std::vector<T>& rhs) {
    auto rowVector =
        makeRowVector({makeFlatVector<T>(lhs), makeFlatVector<T>(rhs)});
    auto result = evaluate<SimpleVector<bool>>(
        fmt::format("{}(c0, c1)", sqlFn), rowVector);

    auto expectedResult = std::vector<bool>();
    expectedResult.reserve(lhs.size());
    for (auto i = 0; i < lhs.size(); i++) {
      expectedResult.push_back(ComparisonOp()(lhs[i], rhs[i]));
    }

    test::assertEqualVectors(result, makeFlatVector<bool>(expectedResult));
  }

  void testVectorComparison(
      const std::vector<T>& lhs,
      const std::vector<T>& rhs) {
    vectorComparisonImpl(lhs, rhs);
    vectorComparisonImpl(rhs, lhs);
  }

  void testConstant(uint32_t vectorLength, T constVal) {
    auto data = std::vector<T>(vectorLength);
    for (int i = 0; i < vectorLength; i++) {
      data[i] = i;
    }

    auto result = checkConstantComparison(data, constVal);
    ASSERT_EQ(result->size(), vectorLength);

    for (int i = 0; i < vectorLength; i++) {
      ASSERT_FALSE(result->isNullAt(i));
      ASSERT_EQ(result->valueAt(i), ComparisonOp()(((T)i), constVal))
          << "Values not matching at " << i;
    }

    // Now check converse
    result = checkConstantComparison(data, constVal, false);
    ASSERT_EQ(result->size(), vectorLength);

    for (int i = 0; i < vectorLength; i++) {
      ASSERT_FALSE(result->isNullAt(i));
      ASSERT_EQ(result->valueAt(i), ComparisonOp()(constVal, ((T)i)))
          << "Values not matching at " << i;
    }
  }

  void testFlat() {
    // Basic comparison test.
    auto lhsVector = std::vector<T>{1, 2, 3, 4, 5, 6, 7};
    auto rhsVector = std::vector<T>{1, 2, 3, 7, 8, 9, 0};

    testVectorComparison(lhsVector, rhsVector);

    // Test some larger vectors.
    lhsVector = std::vector<T>(2047);
    std::fill(
        lhsVector.begin(), lhsVector.end(), std::numeric_limits<T>::max());
    lhsVector[13] = std::numeric_limits<T>::min();
    lhsVector[257] = std::numeric_limits<T>::min();

    rhsVector = std::vector<T>(lhsVector.size());
    std::fill(
        rhsVector.begin(), rhsVector.end(), std::numeric_limits<T>::min());

    testVectorComparison(lhsVector, rhsVector);
  }

  void testDictionary() {
    // Identity mapping, however this will result in non-simd path.
    auto makeDictionary = [&](const std::vector<T>& data) {
      auto base = makeFlatVector(data);
      auto indices = makeIndices(data.size(), [](auto row) { return row; });
      return wrapInDictionary(indices, data.size(), base);
    };

    auto lhs = std::vector<T>{1, 2, 3, 4, 5};
    auto rhs = std::vector<T>{1, 0, 0, 0, 5};
    auto lhsVector = makeDictionary(lhs);
    auto rhsVector = makeDictionary(rhs);

    auto rowVector = makeRowVector({lhsVector, rhsVector});
    auto result = evaluate<SimpleVector<bool>>(
        fmt::format("{}(c0, c1)", sqlFn), rowVector);
    auto expectedResult = std::vector<bool>();
    expectedResult.reserve(lhsVector->size());
    for (auto i = 0; i < lhsVector->size(); i++) {
      expectedResult.push_back(ComparisonOp()(lhs[i], rhs[i]));
    }
    test::assertEqualVectors(result, makeFlatVector<bool>(expectedResult));
  }
};

TYPED_TEST_SUITE(SimdComparisonsTest, comparisonTypes);

TYPED_TEST(SimdComparisonsTest, constant) {
  this->testConstant(35, 17);
  this->testConstant(1024, 53);
  this->testConstant(99, 7);
  this->testConstant(1027, 13);
}

TYPED_TEST(SimdComparisonsTest, flat) {
  this->testFlat();
}

TYPED_TEST(SimdComparisonsTest, dictionary) {
  this->testDictionary();
}
