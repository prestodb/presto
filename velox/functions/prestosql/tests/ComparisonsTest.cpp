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
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;

class ComparisonsTest : public functions::test::FunctionBaseTest {
 public:
  void SetUp() override {
    this->options_.parseDecimalAsDouble = false;
  }

 protected:
  template <typename T>
  void testDistinctFrom(
      const std::vector<std::optional<T>>& col1,
      const std::vector<std::optional<T>>& col2,
      const std::vector<bool>& expected) {
    auto data = vectorMaker_.rowVector(
        {vectorMaker_.flatVectorNullable<T>(col1),
         vectorMaker_.flatVectorNullable<T>(col2)});

    auto result = evaluate<SimpleVector<bool>>("c0 IS DISTINCT FROM c1", data);

    ASSERT_EQ((size_t)result->size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_TRUE(!result->isNullAt(i));
      ASSERT_EQ(expected[i], result->valueAt(i));
    }
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
  auto parseDate = [](const std::string& dateStr) {
    Date returnDate;
    parseTo(dateStr, returnDate);
    return returnDate;
  };
  std::vector<std::tuple<Date, bool>> testData = {
      {parseDate("2019-05-01"), false},
      {parseDate("2019-06-01"), true},
      {parseDate("2019-07-01"), true},
      {parseDate("2020-05-31"), true},
      {parseDate("2020-06-01"), true},
      {parseDate("2020-07-01"), false}};

  auto result = evaluate<SimpleVector<bool>>(
      "c0 between cast(\'2019-06-01\' as date) and cast(\'2020-06-01\' as date)",
      makeRowVector({makeFlatVector<Date, 0>(testData)}));

  for (int i = 0; i < testData.size(); ++i) {
    EXPECT_EQ(result->valueAt(i), std::get<1>(testData[i])) << "at " << i;
  }
}

TEST_F(ComparisonsTest, betweenDecimal) {
  auto runAndCompare = [&](const std::string& exprStr,
                           VectorPtr input,
                           VectorPtr expectedResult) {
    auto actual = evaluate<SimpleVector<bool>>(exprStr, makeRowVector({input}));
    test::assertEqualVectors(actual, expectedResult);
  };

  auto shortFlat = makeNullableShortDecimalFlatVector(
      {100, 250, 300, 500, std::nullopt}, DECIMAL(3, 2));
  auto expectedResult =
      makeNullableFlatVector<bool>({false, true, true, false, std::nullopt});

  runAndCompare("c0 between 2.00 and 3.00", shortFlat, expectedResult);

  auto longFlat = makeNullableLongDecimalFlatVector(
      {100, 250, 300, 500, std::nullopt}, DECIMAL(20, 2));

  // Comparing LONG_DECIMAL and SHORT_DECIMAL must throw error.
  VELOX_ASSERT_THROW(
      runAndCompare("c0 between 2.00 and 3.00", longFlat, expectedResult),
      "Scalar function signature is not supported: between(LONG_DECIMAL(20,2), SHORT_DECIMAL(3,2), SHORT_DECIMAL(3,2)).");
}

TEST_F(ComparisonsTest, eqDecimal) {
  auto runAndCompare = [&](std::vector<VectorPtr>& inputs,
                           VectorPtr expectedResult) {
    auto actual =
        evaluate<SimpleVector<bool>>("c0 == c1", makeRowVector(inputs));
    test::assertEqualVectors(actual, expectedResult);
  };

  std::vector<VectorPtr> inputs = {
      makeNullableShortDecimalFlatVector(
          {1, std::nullopt, 3, -3, std::nullopt, 4}, DECIMAL(10, 5)),
      makeNullableShortDecimalFlatVector(
          {1, 2, 3, -3, std::nullopt, 5}, DECIMAL(10, 5))};
  auto expected = makeNullableFlatVector<bool>(
      {true, std::nullopt, true, true, std::nullopt, false});
  runAndCompare(inputs, expected);

  // Test with different data types.
  inputs = {
      makeShortDecimalFlatVector({1}, DECIMAL(10, 5)),
      makeShortDecimalFlatVector({1}, DECIMAL(10, 4))};
  VELOX_ASSERT_THROW(
      runAndCompare(inputs, expected),
      "Scalar function signature is not supported: eq(SHORT_DECIMAL(10,5),"
      " SHORT_DECIMAL(10,4))");
}

TEST_F(ComparisonsTest, eqArray) {
  auto test =
      [&](const std::optional<std::vector<std::optional<int64_t>>>& array1,
          const std::optional<std::vector<std::optional<int64_t>>>& array2,
          std::optional<bool> expected) {
        auto vector1 = vectorMaker_.arrayVectorNullable<int64_t>({array1});
        auto vector2 = vectorMaker_.arrayVectorNullable<int64_t>({array2});
        auto result = evaluate<SimpleVector<bool>>(
            "c0 == c1", makeRowVector({vector1, vector2}));

        ASSERT_EQ(expected.has_value(), !result->isNullAt(0));
        if (expected.has_value()) {
          ASSERT_EQ(expected.value(), result->valueAt(0));
        }
      };

  test(std::nullopt, std::nullopt, std::nullopt);
  test(std::nullopt, {{1}}, std::nullopt);
  test({{1}}, std::nullopt, std::nullopt);

  test({{}}, {{}}, true);

  test({{1, 2, 3}}, {{1, 2, 3}}, true);
  test({{1, 2, 3}}, {{1, 2, 4}}, false);

  // Checking the first element is enough to determine the result of the
  // compare.
  test({{1, std::nullopt}}, {{6, 2}}, false);

  test({{1, std::nullopt}}, {{1, 2}}, std::nullopt);

  // Different size arrays.
  test({{}}, {{std::nullopt, std::nullopt}}, false);
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

TEST_F(ComparisonsTest, eqMap) {
  using map_t =
      std::optional<std::vector<std::pair<int64_t, std::optional<int64_t>>>>;
  auto test =
      [&](const map_t& map1, const map_t& map2, std::optional<bool> expected) {
        auto vector1 = makeNullableMapVector<int64_t, int64_t>({map1});
        auto vector2 = makeNullableMapVector<int64_t, int64_t>({map2});

        auto result = evaluate<SimpleVector<bool>>(
            "c0 == c1", makeRowVector({vector1, vector2}));

        ASSERT_EQ(expected.has_value(), !result->isNullAt(0));

        if (expected.has_value()) {
          ASSERT_EQ(expected.value(), result->valueAt(0));
        }
      };

  test({{{1, 2}, {3, 4}}}, {{{1, 2}, {3, 4}}}, true);

  // Elements checked in sorted order.
  test({{{3, 4}, {1, 2}}}, {{{1, 2}, {3, 4}}}, true);

  test({{}}, {{}}, true);

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

TEST_F(ComparisonsTest, distinctFrom) {
  testDistinctFrom(
      std::vector<std::optional<int64_t>>{3, 1, 1, std::nullopt, std::nullopt},
      std::vector<std::optional<int64_t>>{3, 2, std::nullopt, 2, std::nullopt},
      {false, true, true, true, false});

  const Timestamp ts1(998474645, 321000000);
  const Timestamp ts2(998423705, 321000000);
  const Timestamp ts3(400000000, 123000000);
  testDistinctFrom(
      std::vector<std::optional<Timestamp>>{
          ts3, ts1, ts1, std::nullopt, std::nullopt},
      std::vector<std::optional<Timestamp>>{
          ts3, ts2, std::nullopt, ts2, std::nullopt},
      {false, true, true, true, false});
}

TEST_F(ComparisonsTest, eqNestedComplex) {
  // Compare Row(Array<Array<int>>, int, Map<int, int>)
  using array_type = std::optional<std::vector<std::optional<int64_t>>>;
  array_type array1 = {{1, 2}};
  array_type array2 = {{}};
  array_type array3 = {{1, 100, 2}};

  auto vector1 = makeNestedArrayVector<int64_t>({{array1, array2, array3}});
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
  using T = typename ComparisonTypeOp::type::NativeType::NativeType;
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

    // Add tests against Nan and other edge cases.
    if constexpr (std::is_floating_point_v<T>) {
      lhsVector = std::vector<T>(47);
      rhsVector = std::vector<T>(47);

      std::fill(
          lhsVector.begin(),
          lhsVector.end(),
          std::numeric_limits<T>::signaling_NaN());
      std::fill(
          rhsVector.begin(),
          rhsVector.end(),
          std::numeric_limits<T>::signaling_NaN());
      testVectorComparison(lhsVector, rhsVector);

      std::fill(
          lhsVector.begin(),
          lhsVector.end(),
          std::numeric_limits<T>::signaling_NaN());
      std::fill(rhsVector.begin(), rhsVector.end(), 1);
      testVectorComparison(lhsVector, rhsVector);
    }
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

TYPED_TEST_SUITE(
    SimdComparisonsTest,
    comparisonTypes,
    functions::test::FunctionBaseTest::TypeNames);

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
