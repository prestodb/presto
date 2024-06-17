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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace facebook::velox {

class DecimalArithmeticTest : public FunctionBaseTest {
 public:
  DecimalArithmeticTest() {
    options_.parseDecimalAsDouble = false;
  }

 protected:
  template <TypeKind KIND>
  void testDecimalExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    using EvalType = typename velox::TypeTraits<KIND>::NativeType;
    auto result =
        evaluate<SimpleVector<EvalType>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
    testOpDictVectors<EvalType>(expression, expected, input);
  }

  template <typename T>
  void testOpDictVectors(
      const std::string& operation,
      const VectorPtr& expected,
      const std::vector<VectorPtr>& flatVector) {
    // Dictionary vectors as arguments.
    auto newSize = flatVector[0]->size() * 2;
    std::vector<VectorPtr> dictVectors;
    for (auto i = 0; i < flatVector.size(); ++i) {
      auto indices = makeIndices(newSize, [&](int row) { return row / 2; });
      dictVectors.push_back(
          VectorTestBase::wrapInDictionary(indices, newSize, flatVector[i]));
    }
    auto resultIndices = makeIndices(newSize, [&](int row) { return row / 2; });
    auto expectedResultDictionary =
        VectorTestBase::wrapInDictionary(resultIndices, newSize, expected);
    auto actual =
        evaluate<SimpleVector<T>>(operation, makeRowVector(dictVectors));
    assertEqualVectors(expectedResultDictionary, actual);
  }
};
} // namespace facebook::velox

TEST_F(DecimalArithmeticTest, add) {
  auto shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(18, 3));
  // Add short and short, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({2000, 4000}, DECIMAL(19, 3)),
      "plus(c0, c1)",
      {shortFlat, shortFlat});

  // Add short and long, returning long.
  auto longFlat = makeFlatVector<int128_t>({1000, 2000}, DECIMAL(19, 3));
  auto expectedLongFlat =
      makeFlatVector<int128_t>({2000, 4000}, DECIMAL(20, 3));
  testDecimalExpr<TypeKind::HUGEINT>(
      expectedLongFlat, "plus(c0, c1)", {shortFlat, longFlat});

  // Add short and long, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      expectedLongFlat, "plus(c0, c1)", {longFlat, shortFlat});

  // Add long and long, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      expectedLongFlat, "c0 + c1", {longFlat, longFlat});

  // Add short and short, returning short.
  shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(10, 3));
  auto expectedShortFlat =
      makeFlatVector<int64_t>({2000, 4000}, DECIMAL(11, 3));
  testDecimalExpr<TypeKind::BIGINT>(
      expectedShortFlat, "c0 + c1", {shortFlat, shortFlat});

  auto expectedConstantFlat =
      makeFlatVector<int64_t>({2000, 3000}, DECIMAL(11, 3));

  // Constant and Flat arguments.
  testDecimalExpr<TypeKind::BIGINT>(
      expectedConstantFlat, "plus(1.00, c0)", {shortFlat});

  // Flat and Constant arguments.
  testDecimalExpr<TypeKind::BIGINT>(
      expectedConstantFlat, "plus(c0,1.00)", {shortFlat});

  testDecimalExpr<TypeKind::BIGINT>(
      makeNullableFlatVector<int64_t>(
          {2, 4, std::nullopt, std::nullopt, std::nullopt}, DECIMAL(11, 3)),
      "plus(c0, c1)",
      {makeNullableFlatVector<int64_t>(
           {1, 2, std::nullopt, 6, std::nullopt}, DECIMAL(10, 3)),
       makeNullableFlatVector<int64_t>(
           {1, 2, 5, std::nullopt, std::nullopt}, DECIMAL(10, 3))});

  // Addition overflow.
  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 + cast(1.00 as decimal(2,0))",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMax},
              DECIMAL(38, 0))}),
      "Decimal overflow. Value '100000000000000000000000000000000000000' is not in the range of Decimal Type");

  // Rescaling LHS overflows.
  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 + 0.01",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMax},
              DECIMAL(38, 0))}),
      "Decimal overflow: 99999999999999999999999999999999999999 + 1");
  // Rescaling RHS overflows.
  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "0.01 + c0",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMax},
              DECIMAL(38, 0))}),
      "Decimal overflow: 1 + 99999999999999999999999999999999999999");
}

TEST_F(DecimalArithmeticTest, subtract) {
  auto shortFlatA = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(18, 3));
  // Subtract short and short, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({500, 1000}, DECIMAL(19, 3)),
      "minus(c0, c1)",
      {shortFlatA, makeFlatVector<int64_t>({500, 1000}, DECIMAL(18, 3))});

  // Subtract short and long, returning long.
  auto longFlatA = makeFlatVector<int128_t>({100, 200}, DECIMAL(19, 3));
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({900, 1800}, DECIMAL(20, 3)),
      "minus(c0, c1)",
      {shortFlatA, longFlatA});

  // Subtract long and short, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({-900, -1800}, DECIMAL(20, 3)),
      "minus(c0, c1)",
      {longFlatA, shortFlatA});

  // Subtract long and long, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({-900, -1800}, DECIMAL(21, 3)),
      "c0 - c1",
      {longFlatA, makeFlatVector<int128_t>({100, 200}, DECIMAL(19, 2))});

  // Subtract short and short, returning short.
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({500, 1000}, DECIMAL(11, 3)),
      "minus(c0, c1)",
      {makeFlatVector<int64_t>({1000, 2000}, DECIMAL(10, 3)),
       makeFlatVector<int64_t>({500, 1000}, DECIMAL(10, 3))});
  // Constant and Flat arguments.
  shortFlatA = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(10, 3));
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({0, -1000}, DECIMAL(11, 3)),
      "minus(1.00, c0)",
      {shortFlatA});

  // Flat and Constant arguments.
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({0, 1000}, DECIMAL(11, 3)),
      "minus(c0, 1.00)",
      {shortFlatA});

  // Input with NULLs.
  testDecimalExpr<TypeKind::BIGINT>(
      makeNullableFlatVector<int64_t>(
          {2, 4, std::nullopt, std::nullopt, std::nullopt}, DECIMAL(11, 3)),
      "minus(c0, c1)",
      {makeNullableFlatVector<int64_t>(
           {3, 6, std::nullopt, 6, std::nullopt}, DECIMAL(10, 3)),
       makeNullableFlatVector<int64_t>(
           {1, 2, 5, std::nullopt, std::nullopt}, DECIMAL(10, 3))});

  // Subtraction overflow.
  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 - cast(1.00 as decimal(2,0))",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMin},
              DECIMAL(38, 0))}),
      "Decimal overflow. Value '-100000000000000000000000000000000000000' is not in the range of Decimal Type");
  // Rescaling LHS overflows.
  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 - 0.01",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMin},
              DECIMAL(38, 0))}),
      "Decimal overflow: -99999999999999999999999999999999999999 - 1");
  // Rescaling RHS overflows.
  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "0.01 - c0",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMin},
              DECIMAL(38, 0))}),
      "Decimal overflow: 1 - -99999999999999999999999999999999999999");
}

TEST_F(DecimalArithmeticTest, multiply) {
  auto shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(17, 3));
  // Multiply short and short, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({1000000, 4000000}, DECIMAL(34, 6)),
      "multiply(c0, c1)",
      {shortFlat, shortFlat});
  // Multiply short and long, returning long.
  auto longFlat = makeFlatVector<int128_t>({1000, 2000}, DECIMAL(20, 3));
  auto expectedLongFlat =
      makeFlatVector<int128_t>({1000000, 4000000}, DECIMAL(37, 6));
  testDecimalExpr<TypeKind::HUGEINT>(
      expectedLongFlat, "multiply(c0, c1)", {shortFlat, longFlat});
  // Multiply long and short, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      expectedLongFlat, "multiply(c0, c1)", {longFlat, shortFlat});

  // Multiply long and long, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({1000000, 4000000}, DECIMAL(38, 6)),
      "multiply(c0, c1)",
      {longFlat, longFlat});

  // Multiply short and short, returning short.
  shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(6, 3));
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({1000000, 4000000}, DECIMAL(12, 6)),
      "c0 * c1",
      {shortFlat, shortFlat});
  auto expectedConstantFlat =
      makeFlatVector<int64_t>({100000, 200000}, DECIMAL(9, 5));

  // Constant and Flat arguments.
  testDecimalExpr<TypeKind::BIGINT>(
      expectedConstantFlat, "1.00 * c0", {shortFlat});

  // Flat and Constant arguments.
  testDecimalExpr<TypeKind::BIGINT>(
      expectedConstantFlat, "c0 * 1.00", {shortFlat});

  // Long decimal limits
  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 * cast(10.00 as decimal(2,0))",
          {makeFlatVector(
              std::vector<int128_t>{
                  HugeInt::build(0x08FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)},
              DECIMAL(38, 0))}),
      "Decimal overflow. Value '119630519620642428561342635425231011830' is not in the range of Decimal Type");

  // Rescaling the final result overflows.
  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 * cast(1.00 as decimal(2,1))",
          {makeFlatVector(
              std::vector<int128_t>{
                  HugeInt::build(0x08FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)},
              DECIMAL(38, 0))}),
      "Decimal overflow. Value '119630519620642428561342635425231011830' is not in the range of Decimal Type");

  // The sum of input scales exceeds 38.
  VELOX_ASSERT_THROW(
      evaluate(
          "c0 * c0",
          makeRowVector(
              {makeFlatVector<int128_t>({1000, 2000}, DECIMAL(38, 30))})),
      "");
}

TEST_F(DecimalArithmeticTest, decimalDivTest) {
  auto shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(17, 3));
  // Divide short and short, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({500, 2000}, DECIMAL(20, 3)),
      "divide(c0, c1)",
      {makeFlatVector<int64_t>({500, 4000}, DECIMAL(17, 3)), shortFlat});

  // Divide short and long, returning long.
  auto longFlat = makeFlatVector<int128_t>({500, 4000}, DECIMAL(20, 2));
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({5000, 20000}, DECIMAL(24, 3)),
      "divide(c0, c1)",
      {longFlat, shortFlat});

  // Divide long and short, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({200, 50}, DECIMAL(19, 3)),
      "divide(c0, c1)",
      {shortFlat, longFlat});

  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>(
          {HugeInt::parse("20000000000000000"),
           HugeInt::parse("50000000000000000")},
          DECIMAL(38, 19)),
      "divide(c0, c1)",
      {makeFlatVector<int64_t>({100, 200}, DECIMAL(17, 4)),
       makeFlatVector<int128_t>(
           {HugeInt::parse("50000000000000000000"),
            HugeInt::parse("40000000000000000000")},
           DECIMAL(21, 19))});

  // Divide long and long, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({500, 300}, DECIMAL(22, 2)),
      "divide(c0, c1)",
      {makeFlatVector<int128_t>({2500, 12000}, DECIMAL(20, 2)), longFlat});

  // Divide short and short, returning short.
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({500, 300}, DECIMAL(7, 5)),
      "divide(c0, c1)",
      {makeFlatVector<int64_t>({2500, 12000}, DECIMAL(5, 5)),
       makeFlatVector<int64_t>({500, 4000}, DECIMAL(5, 2))});

  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({1000, 500}, DECIMAL(7, 3)),
      "1.00 / c0",
      {shortFlat});

  // Flat and Constant arguments.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({500, 1000}, DECIMAL(19, 3)),
      "c0 / 2.00",
      {shortFlat});

  // Divide and round-up.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({6, -1, -11, -15, 0, 8}, DECIMAL(3, 1))},
      "c0 / -6.0",
      {makeFlatVector<int64_t>({-34, 5, 65, 90, 2, -49}, DECIMAL(2, 1))});

  // Divide by zero.
  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::BIGINT>({}, "c0 / 0.0", {shortFlat}),
      "Division by zero");

  // Long decimal limits.
  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 / 0.01",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMax},
              DECIMAL(38, 0))}),
      "Decimal overflow: 99999999999999999999999999999999999999 * 10000");

  // Rescale factor > max precision (38).
  VELOX_ASSERT_USER_THROW(
      evaluate(
          "divide(c0, c1)",
          makeRowVector(
              {makeFlatVector<int128_t>({5000, 20000}, DECIMAL(20, 1)),
               makeFlatVector<int128_t>({5000, 20000}, DECIMAL(33, 32))})),
      "Decimal overflow");
}

TEST_F(DecimalArithmeticTest, decimalDivDifferentTypes) {
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({1, 1, -1, 1}, DECIMAL(12, 2))},
      "cast(c0 as decimal(12,2)) / c1",
      {makeFlatVector<int64_t>({100, 200, -300, 400}, DECIMAL(12, 2)),
       makeFlatVector<int128_t>({100, 200, 300, 400}, DECIMAL(19, 0))});

  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({100, 100, -100, 100}, DECIMAL(14, 2))},
      "cast(c0 as decimal(12,2)) / c1",
      {makeFlatVector<int128_t>({1, 2, 3, 4}, DECIMAL(19, 0)),
       makeFlatVector<int64_t>({100, 200, -300, 400}, DECIMAL(12, 2))});
}

TEST_F(DecimalArithmeticTest, decimalMod) {
  // short % short -> short.
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({0, 0}, DECIMAL(2, 1)),
      "mod(c0, c1)",
      {makeFlatVector<int64_t>({0, 50}, DECIMAL(2, 1)),
       makeFlatVector<int64_t>({20, 25}, DECIMAL(2, 1))});
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({3, -3, 3, -3}, DECIMAL(2, 1)),
      "mod(c0, c1)",
      {makeFlatVector<int64_t>({13, -13, 13, -13}, DECIMAL(3, 1)),
       makeFlatVector<int64_t>({5, 5, -5, -5}, DECIMAL(2, 1))});
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({90, -245, 245, -90}, DECIMAL(3, 2)),
      "mod(c0, c1)",
      {makeFlatVector<int64_t>({50, -50, 50, -50}, DECIMAL(2, 1)),
       makeFlatVector<int64_t>({205, 255, -255, -205}, DECIMAL(3, 2))});
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({2500, -12000}, DECIMAL(5, 3)),
      "mod(c0, c1)",
      {makeFlatVector<int64_t>({2500, -12000}, DECIMAL(5, 3)),
       makeFlatVector<int64_t>({600, 5000}, DECIMAL(5, 2))});

  // short % long -> short.
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({1000, -600, 1000, -600}, DECIMAL(17, 15)),
      "mod(c0, c1)",
      {makeFlatVector<int64_t>({1000, -600, 1000, -600}, DECIMAL(17, 15)),
       makeFlatVector<int128_t>({13, 17, -13, -17}, DECIMAL(20, 10))});

  // long % short -> short.
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({8, -11, 8, -11}, DECIMAL(17, 15)),
      "mod(c0, c1)",
      {makeFlatVector<int128_t>({500, -4000, 500, -4000}, DECIMAL(20, 10)),
       makeFlatVector<int64_t>({17, 19, -17, -19}, DECIMAL(17, 15))});

  // short % long -> long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({0, -16, 0, -16}, DECIMAL(25, 10)),
      "mod(c0, c1)",
      {makeFlatVector<int64_t>({1000, -600, 1000, -600}, DECIMAL(17, 2)),
       makeFlatVector<int128_t>({400, 38, -400, -38}, DECIMAL(30, 10))});

  // long % short -> long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({500, -4000, 500, -4000}, DECIMAL(25, 10)),
      "mod(c0, c1)",
      {makeFlatVector<int128_t>({500, -4000, 500, -4000}, DECIMAL(30, 10)),
       makeFlatVector<int64_t>({1000, 2000, -1000, -2000}, DECIMAL(17, 2))});

  // long % long -> long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({2500, -12000, 2500, -12000}, DECIMAL(23, 5)),
      "mod(c0, c1)",
      {makeFlatVector<int128_t>({2500, -12000, 2500, -12000}, DECIMAL(25, 5)),
       makeFlatVector<int128_t>({500, 4000, -500, -4000}, DECIMAL(20, 2))});

  VELOX_ASSERT_USER_THROW(
      testDecimalExpr<TypeKind::BIGINT>(
          {},
          "c0 % 0.0",
          {makeFlatVector<int64_t>({1000, 2000}, DECIMAL(17, 3))}),
      "Modulus by zero");
}

TEST_F(DecimalArithmeticTest, round) {
  // Round short decimals.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({0, 1, -1, 0}, DECIMAL(1, 0))},
      "round(c0)",
      {makeFlatVector<int64_t>({123, 542, -999, 0}, DECIMAL(3, 3))});
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({1111, 1112, -9999, 10000}, DECIMAL(5, 0))},
      "round(c0)",
      {makeFlatVector<int64_t>({11112, 11115, -99989, 99999}, DECIMAL(5, 1))});
  // Round long decimals.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({0, 1, -1, 0}, DECIMAL(1, 0))},
      "round(c0)",
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5000000000000000000, -9000000000000000000, 0},
          DECIMAL(19, 19))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {DecimalUtil::kPowersOfTen[37], -DecimalUtil::kPowersOfTen[37]},
          DECIMAL(38, 0))},
      "round(c0)",
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 1))});

  // Min and max short decimals.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>(
          {DecimalUtil::kShortDecimalMax, DecimalUtil::kShortDecimalMin},
          DECIMAL(15, 0))},
      "round(c0)",
      {makeFlatVector<int64_t>(
          {DecimalUtil::kShortDecimalMax, DecimalUtil::kShortDecimalMin},
          DECIMAL(15, 0))});

  // Min and max long decimals.
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 0))},
      "round(c0)",
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 0))});
}

TEST_F(DecimalArithmeticTest, roundN) {
  // Round upto 'scale' decimal places.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(4, 3))},
      "round(c0, CAST(3 as integer))",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 3))});
  // Round upto scale-1 decimal places.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({120, 550, -1000, 0}, DECIMAL(4, 3))},
      "round(c0, CAST(2 as integer))",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 3))});
  // Round upto 0 decimal places.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({100, 600, -1000, 0}, DECIMAL(4, 2))},
      "round(c0, CAST(0 as integer))",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 2))});
  // Round upto -1 decimal places.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({100, 600, -1000, 0}, DECIMAL(4, 1))},
      "round(c0, CAST(-1 as integer))",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 1))});
  // Round upto -2 decimal places. Here precision == scale - decimals.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({0, 0, 0, 0}, DECIMAL(4, 1))},
      "round(c0, CAST(-2 as integer))",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 1))});

  // Round up long decimals scale - 5 decimal places.
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {1234567890123500000,
           5000000000000000000,
           -10'000'000'000'000'000'00,
           0},
          DECIMAL(20, 19))},
      "round(c0, CAST(14 as integer))",
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5000000000000000000, -999999999999999999, 0},
          DECIMAL(19, 19))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {1234600000000000000, 5555600000000000000, -1000000000000000000, 0},
          DECIMAL(20, 5))},
      "round(c0, CAST(-9 as integer))",
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5555555555555555555, -999999999999999999, 0},
          DECIMAL(19, 5))});
}

TEST_F(DecimalArithmeticTest, floor) {
  // short DECIMAL -> short DECIMAL.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({0, 0, -1, 0, -1, 0, -1, 0, -1}, DECIMAL(2, 0))},
      "floor(c0)",
      {makeFlatVector<int64_t>(
          {0, 1, -1, 49, -49, 50, -50, 99, -99}, DECIMAL(3, 2))});
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>(
          {123, -123, 123, -124, 123, -124, 123, -124, 123, -124, 123, -124},
          DECIMAL(4, 0))},
      "floor(c0)",
      {makeFlatVector<int64_t>(
          {12300,
           -12300,
           12301,
           -12301,
           12345,
           -12345,
           12349,
           -12349,
           12350,
           -12350,
           12399,
           -12399},
          DECIMAL(5, 2))});
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>(
          {DecimalUtil::kShortDecimalMax, DecimalUtil::kShortDecimalMin},
          DECIMAL(18, 0))},
      "floor(c0)",
      {makeFlatVector<int64_t>(
          {DecimalUtil::kShortDecimalMax, DecimalUtil::kShortDecimalMin},
          DECIMAL(18, 0))});

  // long DECIMAL -> long DECIMAL.
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {0, 0, -1, 0, -1, 0, -1, 0, -1}, DECIMAL(19, 0))},
      "floor(c0)",
      {makeFlatVector<int128_t>(
          {0, 1, -1, 49, -49, 50, -50, 99, -99}, DECIMAL(20, 2))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {DecimalUtil::kPowersOfTen[33] - 1, -DecimalUtil::kPowersOfTen[33]},
          DECIMAL(34, 0))},
      "floor(c0)",
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 5))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 0))},
      "floor(c0)",
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 0))});

  // long DECIMAL -> short DECIMAL.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({0, 0, -1, -1, 0}, DECIMAL(1, 0))},
      "floor(c0)",
      {makeFlatVector<int128_t>(
          {1234567890123456789,
           5000000000000000000,
           -9000000000000000000,
           -1000000000000000000,
           0},
          DECIMAL(19, 19))});
}

TEST_F(DecimalArithmeticTest, truncate) {
  // Truncate short decimals.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({0, 0, 0, 0}, DECIMAL(1, 0))},
      "truncate(c0)",
      {makeFlatVector<int64_t>({123, 542, -999, 0}, DECIMAL(3, 3))});
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({1111, 1111, -9998, 9999}, DECIMAL(4, 0))},
      "truncate(c0)",
      {makeFlatVector<int64_t>({11112, 11115, -99989, 99999}, DECIMAL(5, 1))});

  // Truncate long decimals.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({0, 0, 0, 0}, DECIMAL(1, 0))},
      "truncate(c0)",
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5000000000000000000, -9000000000000000000, 0},
          DECIMAL(19, 19))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {DecimalUtil::kPowersOfTen[37] - 1,
           -DecimalUtil::kPowersOfTen[37] + 1},
          DECIMAL(37, 0))},
      "truncate(c0)",
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 1))});

  // Min and max short decimals.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>(
          {DecimalUtil::kShortDecimalMax, DecimalUtil::kShortDecimalMin},
          DECIMAL(15, 0))},
      "truncate(c0)",
      {makeFlatVector<int64_t>(
          {DecimalUtil::kShortDecimalMax, DecimalUtil::kShortDecimalMin},
          DECIMAL(15, 0))});

  // Min and max long decimals.
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 0))},
      "truncate(c0)",
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 0))});
}

TEST_F(DecimalArithmeticTest, truncateN) {
  // Truncate to 'scale' decimal places.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 3))},
      "truncate(c0, 3::integer)",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 3))});

  // Truncate to 'scale' - 1 decimal places.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({120, 550, -990, 0}, DECIMAL(3, 3))},
      "truncate(c0, 2::integer)",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 3))});

  // Truncate to 0 decimal places.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({100, 500, -900, 0}, DECIMAL(3, 2))},
      "truncate(c0, 0::integer)",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 2))});

  // Truncate to -1 decimal places.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({100, 500, -900, 0}, DECIMAL(3, 1))},
      "truncate(c0, '-1'::integer)",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 1))});

  // Truncate to -2 decimal places.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({0, 0, 0, 0}, DECIMAL(3, 1))},
      "truncate(c0, '-2'::integer)",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 1))});

  // Truncate long decimals to 'scale' - 5 decimal places.
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {1234567890123400000, 5000000000000000000, -999999999999900000, 0},
          DECIMAL(19, 19))},
      "truncate(c0, 14::integer)",
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5000000000000000000, -999999999999999999, 0},
          DECIMAL(19, 19))});

  // Truncate long decimals to -9 decimal places.
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {1234500000000000000, 5555500000000000000, -999900000000000000, 0},
          DECIMAL(19, 5))},
      "truncate(c0, '-9'::integer)",
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5555555555555555555, -999999999999999999, 0},
          DECIMAL(19, 5))});
}

TEST_F(DecimalArithmeticTest, abs) {
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({1111, 1112, 9999, 0}, DECIMAL(5, 1))},
      "abs(c0)",
      {makeFlatVector<int64_t>({-1111, 1112, -9999, 0}, DECIMAL(5, 1))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {11111111, 11112112, 99999999, 0}, DECIMAL(19, 19))},
      "abs(c0)",
      {makeFlatVector<int128_t>(
          {-11111111, 11112112, -99999999, 0}, DECIMAL(19, 19))});

  // Min and max short decimals.
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector(
          std::vector<int64_t>{DecimalUtil::kShortDecimalMax}, DECIMAL(15, 0))},
      "abs(c0)",
      {makeFlatVector(
          std::vector<int64_t>{DecimalUtil::kShortDecimalMin},
          DECIMAL(15, 0))});
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector(
          std::vector<int64_t>{DecimalUtil::kShortDecimalMax},
          DECIMAL(15, 15))},
      "abs(c0)",
      {makeFlatVector(
          std::vector<int64_t>{DecimalUtil::kShortDecimalMin},
          DECIMAL(15, 15))});

  // Min and max long decimals.
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector(
          std::vector<int128_t>{DecimalUtil::kLongDecimalMax}, DECIMAL(38, 0))},
      "abs(c0)",
      {makeFlatVector(
          std::vector<int128_t>{DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 0))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector(
          std::vector<int128_t>{DecimalUtil::kLongDecimalMax},
          DECIMAL(38, 38))},
      "abs(c0)",
      {makeFlatVector(
          std::vector<int128_t>{DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 38))});
}

TEST_F(DecimalArithmeticTest, negate) {
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({1111, -1112, 9999, 0}, DECIMAL(5, 1))},
      "negate(c0)",
      {makeFlatVector<int64_t>({-1111, 1112, -9999, 0}, DECIMAL(5, 1))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {11111111,
           -11112112,
           99999999,
           -DecimalUtil::kLongDecimalMax,
           -DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 19))},
      "negate(c0)",
      {makeFlatVector<int128_t>(
          {-11111111,
           11112112,
           -99999999,
           DecimalUtil::kLongDecimalMax,
           DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 19))});
}
