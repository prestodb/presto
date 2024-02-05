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
  VELOX_ASSERT_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 + cast(1.00 as decimal(2,0))",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMax},
              DECIMAL(38, 0))}),
      "Decimal overflow. Value '100000000000000000000000000000000000000' is not in the range of Decimal Type");

  // Rescaling LHS overflows.
  VELOX_ASSERT_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 + 0.01",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMax},
              DECIMAL(38, 0))}),
      "Decimal overflow: 99999999999999999999999999999999999999 + 1");
  // Rescaling RHS overflows.
  VELOX_ASSERT_THROW(
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
  VELOX_ASSERT_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 - cast(1.00 as decimal(2,0))",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMin},
              DECIMAL(38, 0))}),
      "Decimal overflow. Value '-100000000000000000000000000000000000000' is not in the range of Decimal Type");
  // Rescaling LHS overflows.
  VELOX_ASSERT_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 - 0.01",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMin},
              DECIMAL(38, 0))}),
      "Decimal overflow: -99999999999999999999999999999999999999 - 1");
  // Rescaling RHS overflows.
  VELOX_ASSERT_THROW(
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
  VELOX_ASSERT_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 * cast(10.00 as decimal(2,0))",
          {makeFlatVector(
              std::vector<int128_t>{
                  HugeInt::build(0x08FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)},
              DECIMAL(38, 0))}),
      "Decimal overflow. Value '119630519620642428561342635425231011830' is not in the range of Decimal Type");

  // Rescaling the final result overflows.
  VELOX_ASSERT_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 * cast(1.00 as decimal(2,1))",
          {makeFlatVector(
              std::vector<int128_t>{
                  HugeInt::build(0x08FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)},
              DECIMAL(38, 0))}),
      "Decimal overflow. Value '119630519620642428561342635425231011830' is not in the range of Decimal Type");
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
  VELOX_ASSERT_THROW(
      testDecimalExpr<TypeKind::BIGINT>({}, "c0 / 0.0", {shortFlat}),
      "Division by zero");

  // Long decimal limits.
  VELOX_ASSERT_THROW(
      testDecimalExpr<TypeKind::HUGEINT>(
          {},
          "c0 / 0.01",
          {makeFlatVector(
              std::vector<int128_t>{DecimalUtil::kLongDecimalMax},
              DECIMAL(38, 0))}),
      "Decimal overflow: 99999999999999999999999999999999999999 * 10000");
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
