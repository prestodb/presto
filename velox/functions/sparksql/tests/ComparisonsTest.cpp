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
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include <velox/vector/SimpleVector.h>

namespace facebook::velox::functions::sparksql::test {
namespace {

static constexpr double kInf = std::numeric_limits<double>::infinity();
static constexpr float kInfF = std::numeric_limits<float>::infinity();
static constexpr auto kNaN = std::numeric_limits<double>::quiet_NaN();

class ComparisonsTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<bool> equaltonullsafe(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("equalnullsafe(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<bool> equalto(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("equalto(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<bool>
  between(std::optional<T> a, std::optional<T> b, std::optional<T> c) {
    return evaluateOnce<bool>("c0 between c1 and c2", a, b, c);
  }

  template <typename T>
  std::optional<bool> lessthan(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("lessthan(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<bool> lessthanorequal(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("lessthanorequal(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<bool> greaterthan(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("greaterthan(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<bool> greaterthanorequal(
      std::optional<T> a,
      std::optional<T> b) {
    return evaluateOnce<bool>("greaterthanorequal(c0, c1)", a, b);
  }

  void runAndCompare(
      const std::string& functionName,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expectedResult) {
    auto actual = evaluate<SimpleVector<bool>>(
        fmt::format("{}(c0, c1)", functionName), makeRowVector(input));
    facebook::velox::test::assertEqualVectors(expectedResult, actual);
  };
};

TEST_F(ComparisonsTest, equaltonullsafe) {
  EXPECT_EQ(equaltonullsafe<int64_t>(1, 1), true);
  EXPECT_EQ(equaltonullsafe<int32_t>(1, 2), false);
  EXPECT_EQ(equaltonullsafe<float>(std::nullopt, std::nullopt), true);
  EXPECT_EQ(equaltonullsafe<std::string>(std::nullopt, "abcs"), false);
  EXPECT_EQ(equaltonullsafe<std::string>(std::nullopt, std::nullopt), true);
  EXPECT_EQ(equaltonullsafe<double>(1, std::nullopt), false);
  EXPECT_EQ(equaltonullsafe<double>(kNaN, std::nullopt), false);
  EXPECT_EQ(equaltonullsafe<double>(kNaN, 1), false);
  EXPECT_EQ(equaltonullsafe<double>(kNaN, kNaN), true);
}

TEST_F(ComparisonsTest, equalto) {
  EXPECT_EQ(equalto<Timestamp>(Timestamp(2, 2), Timestamp(2, 2)), true);
  EXPECT_EQ(equalto<StringView>("test"_sv, "test"_sv), true);
  EXPECT_EQ(equalto<StringView>("test"_sv, std::nullopt), std::nullopt);
  EXPECT_EQ(equalto<int64_t>(1, 1), true);
  EXPECT_EQ(equalto<int32_t>(1, 2), false);
  EXPECT_EQ(equalto<float>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(equalto<std::string>(std::nullopt, "abcs"), std::nullopt);
  EXPECT_EQ(equalto<std::string>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(equalto<double>(1, std::nullopt), std::nullopt);
  EXPECT_EQ(equalto<double>(kNaN, std::nullopt), std::nullopt);
  EXPECT_EQ(equalto<double>(kNaN, 1), false);
  EXPECT_EQ(equalto<double>(0, kNaN), false);
  EXPECT_EQ(equalto<double>(kNaN, kNaN), true);
  EXPECT_EQ(equalto<double>(kInf, kInf), true);
  EXPECT_EQ(equalto<float>(kInfF, kInfF), true);
  EXPECT_EQ(equalto<double>(kInf, 2.0), false);
  EXPECT_EQ(equalto<double>(-kInf, 2.0), false);
  EXPECT_EQ(equalto<float>(kInfF, 1.0), false);
  EXPECT_EQ(equalto<float>(-kInfF, 1.0), false);
  EXPECT_EQ(equalto<float>(kInfF, -kInfF), false);
  EXPECT_EQ(equalto<double>(kInf, kNaN), false);
}

TEST_F(ComparisonsTest, between) {
  EXPECT_EQ(between<int64_t>(2, 1, 3), true);
  EXPECT_EQ(between<int32_t>(2, 1, 3), true);
  EXPECT_EQ(
      between<float>(std::nullopt, std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(between<double>(1, std::nullopt, 3), std::nullopt);
  EXPECT_EQ(between<double>(kNaN, std::nullopt, 4), std::nullopt);
  EXPECT_EQ(between<double>(kNaN, 1, 5), false);
  EXPECT_EQ(between<double>(0, -1, kNaN), false);
  EXPECT_EQ(between<double>(kNaN, 0, kNaN), false);
  EXPECT_EQ(between<double>(kInf, 0, kInf), true);
  EXPECT_EQ(between<float>(kInfF, 0, kInfF), true);
  EXPECT_EQ(between<double>(kInf, 2.0, 5.0), false);
  EXPECT_EQ(between<double>(-kInf, 2.0, 5.0), false);
  EXPECT_EQ(between<float>(kInfF, 1.0, 6.0), false);
  EXPECT_EQ(between<float>(-kInfF, 1.0, 6.0), false);
  EXPECT_EQ(between<float>(kInfF, -kInfF, kInfF), true);
  EXPECT_EQ(between<double>(kInf, -kInf, kNaN), false);
}

TEST_F(ComparisonsTest, decimal) {
  std::vector<VectorPtr> inputs = {
      makeNullableFlatVector<int64_t>(
          {1, std::nullopt, 3, -2, std::nullopt, 4}, DECIMAL(10, 5)),
      makeNullableFlatVector<int64_t>(
          {0, 2, 3, -3, std::nullopt, 5}, DECIMAL(10, 5))};
  auto expected = makeNullableFlatVector<bool>(
      {true, std::nullopt, false, true, std::nullopt, false});
  runAndCompare("greaterthan", inputs, expected);
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
  auto expectedLte = makeNullableFlatVector<bool>(
      {false, std::nullopt, true, false, std::nullopt, true});
  runAndCompare("lessthanorequal", longDecimalsInputs, expectedLte);
  auto expectedEqNullSafe =
      makeFlatVector<bool>({false, false, true, false, true, false});
  runAndCompare("equalnullsafe", longDecimalsInputs, expectedEqNullSafe);

  // Test with different data types.
  std::vector<VectorPtr> invalidInputs = {
      makeFlatVector(std::vector<int64_t>{1}, DECIMAL(10, 5)),
      makeFlatVector(std::vector<int64_t>{1}, DECIMAL(10, 4))};
  auto invalidResult = makeConstant<bool>(true, 1);
  VELOX_ASSERT_THROW(
      runAndCompare("equalto", invalidInputs, invalidResult),
      "Scalar function signature is not supported: "
      "equalto(DECIMAL(10, 5), DECIMAL(10, 4))");
}

TEST_F(ComparisonsTest, testdictionary) {
  // Identity mapping, however this will result in non-simd path.
  auto makeDictionary = [&](const VectorPtr& base) {
    auto indices = makeIndices(base->size(), [](auto row) { return row; });
    return wrapInDictionary(indices, base->size(), base);
  };

  auto makeConstantDic = [&](const VectorPtr& base) {
    return BaseVector::wrapInConstant(base->size(), 0, base);
  };
  // Lhs: 0, null, 2, null, 4.
  auto lhs =
      makeFlatVector<int16_t>(5, [](auto row) { return row; }, nullEvery(2, 1));
  auto rhs = makeFlatVector<int16_t>({1, 0, 3, 0, 5});
  auto lhsVector = makeDictionary(lhs);
  auto rhsVector = makeDictionary(rhs);

  auto rowVector = makeRowVector({lhsVector, rhsVector});
  auto result = evaluate<SimpleVector<bool>>(
      fmt::format("{}(c0, c1)", "greaterthan"), rowVector);
  // Result : false, null, false, null, false.
  facebook::velox::test::assertEqualVectors(
      result,
      makeFlatVector<bool>(5, [](auto row) { return false; }, nullEvery(2, 1)));
  auto constVector = makeConstant(100, 5);
  auto testConstVector =
      makeRowVector({makeDictionary(lhs), makeConstantDic(constVector)});
  // Lhs: 0, null, 2, null, 4.
  // Rhs: const 100.
  // Lessthanorequal result : true, null, true, null, true.
  auto constResult = evaluate<SimpleVector<bool>>(
      fmt::format("{}(c0, c1)", "lessthanorequal"), testConstVector);
  facebook::velox::test::assertEqualVectors(
      constResult,
      makeFlatVector<bool>(5, [](auto row) { return true; }, nullEvery(2, 1)));
  // Lhs: const 100.
  // Rhs: 0, null, 2, null, 4.
  // Greaterthanorequal result : true, null, true, null, true.
  auto testConstVector1 =
      makeRowVector({makeConstantDic(constVector), makeDictionary(lhs)});
  auto constResult1 = evaluate<SimpleVector<bool>>(
      fmt::format("{}(c0, c1)", "greaterthanorequal"), testConstVector1);
  facebook::velox::test::assertEqualVectors(
      constResult1,
      makeFlatVector<bool>(5, [](auto row) { return true; }, nullEvery(2, 1)));
}

TEST_F(ComparisonsTest, testflat) {
  auto vector0 = makeNullableFlatVector<int32_t>({0, 1, 2, 3});
  auto vector1 = makeFlatVector<int32_t>(
      4, [](auto row) { return row + 1; }, nullEvery(2));

  auto expectedResult =
      makeFlatVector<bool>(4, [](auto row) { return true; }, nullEvery(2));
  auto actualResult = evaluate<SimpleVector<bool>>(
      "lessthan(c0, c1)", makeRowVector({vector0, vector1}));
  facebook::velox::test::assertEqualVectors(expectedResult, actualResult);
  auto vectorBool0 = makeFlatVector<bool>({true, true, false, true});
  auto vectorBool1 = makeConstant(true, 4);
  auto actualBoolResult = evaluate<SimpleVector<bool>>(
      "equalto(c0, c1)", makeRowVector({vectorBool0, vectorBool1}));
  facebook::velox::test::assertEqualVectors(vectorBool0, actualBoolResult);
}

TEST_F(ComparisonsTest, lessthan) {
  EXPECT_EQ(lessthan<int64_t>(1, 1), false);
  EXPECT_EQ(lessthan<int32_t>(1, 2), true);
  EXPECT_EQ(lessthan<float>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthan<std::string>(std::nullopt, "abcs"), std::nullopt);
  EXPECT_EQ(lessthan<std::string>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthan<double>(1, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthan<double>(kNaN, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthan<double>(kNaN, 1), false);
  EXPECT_EQ(lessthan<double>(0, kNaN), true);
  EXPECT_EQ(lessthan<double>(kNaN, kNaN), false);
  EXPECT_EQ(lessthan<double>(kInf, kInf), false);
  EXPECT_EQ(lessthan<float>(kInfF, kInfF), false);
  EXPECT_EQ(lessthan<double>(kInf, 2.0), false);
  EXPECT_EQ(lessthan<double>(-kInf, 2.0), true);
  EXPECT_EQ(lessthan<float>(kInfF, 1.0), false);
  EXPECT_EQ(lessthan<float>(-kInfF, 1.0), true);
  EXPECT_EQ(lessthan<float>(kInfF, -kInfF), false);
  EXPECT_EQ(lessthan<double>(kInf, kNaN), true);
}

TEST_F(ComparisonsTest, lessthanorequal) {
  EXPECT_EQ(lessthanorequal<int64_t>(1, 1), true);
  EXPECT_EQ(lessthanorequal<int32_t>(1, 2), true);
  EXPECT_EQ(lessthanorequal<float>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthanorequal<std::string>(std::nullopt, "abcs"), std::nullopt);
  EXPECT_EQ(
      lessthanorequal<std::string>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthanorequal<double>(1, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthanorequal<double>(kNaN, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthanorequal<double>(kNaN, 1), false);
  EXPECT_EQ(lessthanorequal<double>(0, kNaN), true);
  EXPECT_EQ(lessthanorequal<double>(kNaN, kNaN), true);
  EXPECT_EQ(lessthanorequal<double>(kInf, kInf), true);
  EXPECT_EQ(lessthanorequal<float>(kInfF, kInfF), true);
  EXPECT_EQ(lessthanorequal<double>(kInf, 2.0), false);
  EXPECT_EQ(lessthanorequal<double>(-kInf, 2.0), true);
  EXPECT_EQ(lessthanorequal<float>(kInfF, 1.0), false);
  EXPECT_EQ(lessthanorequal<float>(-kInfF, 1.0), true);
  EXPECT_EQ(lessthanorequal<float>(kInfF, -kInfF), false);
  EXPECT_EQ(lessthanorequal<double>(kInf, kNaN), true);
}

TEST_F(ComparisonsTest, greaterthan) {
  EXPECT_EQ(greaterthan<int64_t>(1, 1), false);
  EXPECT_EQ(greaterthan<int32_t>(1, 2), false);
  EXPECT_EQ(greaterthan<float>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthan<std::string>(std::nullopt, "abcs"), std::nullopt);
  EXPECT_EQ(greaterthan<std::string>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthan<double>(1, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthan<double>(kNaN, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthan<double>(kNaN, 1), true);
  EXPECT_EQ(greaterthan<double>(0, kNaN), false);
  EXPECT_EQ(greaterthan<double>(kNaN, kNaN), false);
  EXPECT_EQ(greaterthan<double>(kInf, kInf), false);
  EXPECT_EQ(greaterthan<float>(kInfF, kInfF), false);
  EXPECT_EQ(greaterthan<double>(kInf, 2.0), true);
  EXPECT_EQ(greaterthan<double>(-kInf, 2.0), false);
  EXPECT_EQ(greaterthan<float>(kInfF, 1.0), true);
  EXPECT_EQ(greaterthan<float>(-kInfF, 1.0), false);
  EXPECT_EQ(greaterthan<float>(kInfF, -kInfF), true);
  EXPECT_EQ(greaterthan<float>(kInf, kNaN), false);
}

TEST_F(ComparisonsTest, greaterthanorequal) {
  EXPECT_EQ(greaterthanorequal<int64_t>(1, 1), true);
  EXPECT_EQ(greaterthanorequal<int32_t>(1, 2), false);
  EXPECT_EQ(
      greaterthanorequal<float>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(
      greaterthanorequal<std::string>(std::nullopt, "abcs"), std::nullopt);
  EXPECT_EQ(
      greaterthanorequal<std::string>(std::nullopt, std::nullopt),
      std::nullopt);
  EXPECT_EQ(greaterthanorequal<double>(1, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthanorequal<double>(kNaN, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthanorequal<double>(kNaN, 1), true);
  EXPECT_EQ(greaterthanorequal<double>(0, kNaN), false);
  EXPECT_EQ(greaterthanorequal<double>(kNaN, kNaN), true);
  EXPECT_EQ(greaterthanorequal<double>(kInf, kInf), true);
  EXPECT_EQ(greaterthanorequal<float>(kInfF, kInfF), true);
  EXPECT_EQ(greaterthanorequal<double>(kInf, 2.0), true);
  EXPECT_EQ(greaterthanorequal<double>(-kInf, 2.0), false);
  EXPECT_EQ(greaterthanorequal<float>(kInfF, 1.0), true);
  EXPECT_EQ(greaterthanorequal<float>(-kInfF, 1.0), false);
  EXPECT_EQ(greaterthanorequal<float>(kInfF, -kInfF), true);
  EXPECT_EQ(greaterthanorequal<float>(kInf, kNaN), false);
}

TEST_F(ComparisonsTest, boolean) {
  std::vector<VectorPtr> inputs = {
      makeNullableFlatVector<bool>({true, false, false, true, std::nullopt}),
      makeNullableFlatVector<bool>({false, true, false, true, std::nullopt})};

  runAndCompare(
      "equalnullsafe",
      inputs,
      makeFlatVector<bool>({false, false, true, true, true}));
  runAndCompare(
      "equalto",
      inputs,
      makeNullableFlatVector<bool>({false, false, true, true, std::nullopt}));
  runAndCompare(
      "lessthan",
      inputs,
      makeNullableFlatVector<bool>({false, true, false, false, std::nullopt}));
  runAndCompare(
      "lessthanorequal",
      inputs,
      makeNullableFlatVector<bool>({false, true, true, true, std::nullopt}));
  runAndCompare(
      "greaterthan",
      inputs,
      makeNullableFlatVector<bool>({true, false, false, false, std::nullopt}));
  runAndCompare(
      "greaterthanorequal",
      inputs,
      makeNullableFlatVector<bool>({true, false, true, true, std::nullopt}));
}

TEST_F(ComparisonsTest, dateTypes) {
  std::vector<VectorPtr> dateInputs = {
      makeNullableFlatVector<int32_t>({126, 128, std::nullopt}, DATE()),
      makeNullableFlatVector<int32_t>({126, 127, std::nullopt}, DATE())};
  std::vector<VectorPtr> intervalDayInputs = {
      makeNullableFlatVector<int64_t>(
          {126, 128, std::nullopt}, INTERVAL_DAY_TIME()),
      makeNullableFlatVector<int64_t>(
          {126, 127, std::nullopt}, INTERVAL_DAY_TIME())};
  std::vector<VectorPtr> intervalYearInputs = {
      makeNullableFlatVector<int32_t>(
          {1, 2, std::nullopt}, INTERVAL_YEAR_MONTH()),
      makeNullableFlatVector<int32_t>(
          {1, 1, std::nullopt}, INTERVAL_YEAR_MONTH())};

  auto test = [&](std::vector<VectorPtr>& inputs) {
    runAndCompare(
        "equalnullsafe", inputs, makeFlatVector<bool>({true, false, true}));
    runAndCompare(
        "equalto",
        inputs,
        makeNullableFlatVector<bool>({true, false, std::nullopt}));
    runAndCompare(
        "lessthan",
        inputs,
        makeNullableFlatVector<bool>({false, false, std::nullopt}));
    runAndCompare(
        "lessthanorequal",
        inputs,
        makeNullableFlatVector<bool>({true, false, std::nullopt}));
    runAndCompare(
        "greaterthan",
        inputs,
        makeNullableFlatVector<bool>({false, true, std::nullopt}));
    runAndCompare(
        "greaterthanorequal",
        inputs,
        makeNullableFlatVector<bool>({true, true, std::nullopt}));
  };

  test(dateInputs);

  test(intervalDayInputs);

  test(intervalYearInputs);
}

TEST_F(ComparisonsTest, notSupportedTypes) {
  const auto candidataFuncs = {
      "equalnullsafe",
      "equalto",
      "lessthan",
      "lessthanorequal",
      "greaterthan",
      "greaterthanorequal"};
  auto arrayData = makeArrayVectorFromJson<int64_t>({"[1, 2, 3]"});
  auto mapData = makeMapVectorFromJson<int64_t, int64_t>({"{1: 1}"});
  auto rowData = makeRowVector({arrayData});
  std::vector<VectorPtr> unsupportedTypeData = {arrayData, mapData, rowData};
  auto testFails = [&](const std::string& func, const VectorPtr& vector) {
    VELOX_ASSERT_USER_THROW(
        evaluate<SimpleVector<bool>>(
            fmt::format("{}(c1, c0)", func), makeRowVector({vector, vector})),
        fmt::format(
            "Scalar function signature is not supported: {}({}, {})",
            func,
            vector->type()->toString(),
            vector->type()->toString()));
  };

  for (const auto& func : candidataFuncs) {
    for (const auto& data : unsupportedTypeData) {
      testFails(func, data);
    }
  }
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
