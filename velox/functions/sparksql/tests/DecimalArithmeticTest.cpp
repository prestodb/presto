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
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class DecimalArithmeticTest : public SparkFunctionBaseTest {
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
  }

  VectorPtr makeLongDecimalVector(
      const std::vector<std::string>& value,
      int8_t precision,
      int8_t scale) {
    if (value.size() == 1) {
      return makeConstant<int128_t>(
          HugeInt::parse(std::move(value[0])), 1, DECIMAL(precision, scale));
    }
    std::vector<int128_t> int128s;
    for (auto& v : value) {
      int128s.emplace_back(HugeInt::parse(std::move(v)));
    }
    return makeFlatVector<int128_t>(int128s, DECIMAL(precision, scale));
  }
} // namespace

TEST_F(DecimalArithmeticTest, multiply) {
  // The result can be obtained by Spark unit test
  //       test("multiply") {
  //     val l1 = Literal.create(
  //       Decimal(BigDecimal(1), 17, 3),
  //       DecimalType(17, 3))
  //     val l2 = Literal.create(
  //       Decimal(BigDecimal(1), 17, 3),
  //       DecimalType(17, 3))
  //     checkEvaluation(Divide(l1, l2), null)
  //   }
  auto shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(17, 3));
  // Multiply short and short, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({1000000, 4000000}, DECIMAL(35, 6)),
      "multiply(c0, c1)",
      {shortFlat, shortFlat});
  // Multiply short and long, returning long.
  auto longFlat = makeFlatVector<int128_t>({1000, 2000}, DECIMAL(20, 3));
  auto expectedLongFlat =
      makeFlatVector<int128_t>({1000000, 4000000}, DECIMAL(38, 6));
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

  auto leftFlat0 = makeFlatVector<int128_t>({0, 1, 0}, DECIMAL(20, 3));
  auto rightFlat0 = makeFlatVector<int128_t>({1, 0, 0}, DECIMAL(20, 2));
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>({0, 0, 0}, DECIMAL(38, 5)),
      "multiply(c0, c1)",
      {leftFlat0, rightFlat0});

  // Multiply short and short, returning short.
  shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(6, 3));
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({1000000, 4000000}, DECIMAL(13, 6)),
      "c0 * c1",
      {shortFlat, shortFlat});

  auto expectedConstantFlat =
      makeFlatVector<int64_t>({100000, 200000}, DECIMAL(10, 5));
  // Constant and Flat arguments.
  testDecimalExpr<TypeKind::BIGINT>(
      expectedConstantFlat, "1.00 * c0", {shortFlat});

  // Flat and Constant arguments.
  testDecimalExpr<TypeKind::BIGINT>(
      expectedConstantFlat, "c0 * 1.00", {shortFlat});

  // out_precision == 38, small input values, trimming of scale.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeConstant<int128_t>(61, 1, DECIMAL(38, 7)),
      "c0 * c1",
      {makeConstant<int128_t>(201, 1, DECIMAL(20, 5)),
       makeConstant<int128_t>(301, 1, DECIMAL(20, 5))});

  // out_precision == 38, large values, trimming of scale.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeConstant<int128_t>(
          HugeInt::parse("201" + std::string(31, '0')), 1, DECIMAL(38, 6)),
      "c0 * c1",
      {makeConstant<int128_t>(201, 1, DECIMAL(20, 5)),
       makeConstant<int128_t>(
           HugeInt::parse(std::string(35, '9')), 1, DECIMAL(35, 5))});

  // out_precision == 38, very large values, trimming of scale (requires convert
  // to 256).
  testDecimalExpr<TypeKind::HUGEINT>(
      makeConstant<int128_t>(
          HugeInt::parse("9999999999999999999999999999999999890"),
          1,
          DECIMAL(38, 6)),
      "c0 * c1",
      {makeConstant<int128_t>(
           HugeInt::parse(std::string(35, '9')), 1, DECIMAL(38, 20)),
       makeConstant<int128_t>(
           HugeInt::parse(std::string(36, '9')), 1, DECIMAL(38, 20))});

  // out_precision == 38, very large values, trimming of scale (requires convert
  // to 256). should cause overflow.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeConstant<int128_t>(std::nullopt, 1, DECIMAL(38, 6)),
      "c0 * c1",
      {makeConstant<int128_t>(
           HugeInt::parse(std::string(35, '9')), 1, DECIMAL(38, 4)),
       makeConstant<int128_t>(
           HugeInt::parse(std::string(36, '9')), 1, DECIMAL(38, 4))});

  // Big scale * big scale.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeConstant<int128_t>(0, 1, DECIMAL(38, 37)),
      "c0 * c1",
      {makeConstant<int128_t>(201, 1, DECIMAL(38, 38)),
       makeConstant<int128_t>(301, 1, DECIMAL(38, 38))});

  // Long decimal limits.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeConstant<int128_t>(std::nullopt, 1, DECIMAL(38, 0)),
      "c0 * cast(10.00 as decimal(2,0))",
      {makeConstant<int128_t>(
          HugeInt::build(0x08FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF),
          1,
          DECIMAL(38, 0))});

  // Rescaling the final result overflows.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeConstant<int128_t>(std::nullopt, 1, DECIMAL(38, 1)),
      "c0 * cast(1.00 as decimal(2,1))",
      {makeConstant<int128_t>(
          HugeInt::build(0x08FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF),
          1,
          DECIMAL(38, 0))});
}

TEST_F(DecimalArithmeticTest, decimalDivTest) {
  auto shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(17, 3));
  // Divide short and short, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeLongDecimalVector(
          {"500000000000000000000", "2000000000000000000000"}, 38, 21),
      "divide(c0, c1)",
      {makeFlatVector<int64_t>({500, 4000}, DECIMAL(17, 3)), shortFlat});

  // Divide short and long, returning long.
  auto longFlat = makeFlatVector<int128_t>({500, 4000}, DECIMAL(20, 2));
  testDecimalExpr<TypeKind::HUGEINT>(
      makeFlatVector<int128_t>(
          {500000000000000000, 2000000000000000000}, DECIMAL(38, 17)),
      "divide(c0, c1)",
      {longFlat, shortFlat});

  // Divide long and short, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeLongDecimalVector(
          {"20" + std::string(20, '0'), "5" + std::string(20, '0')}, 38, 22),
      "divide(c0, c1)",
      {shortFlat, longFlat});

  // Divide long and long, returning long.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeLongDecimalVector(
          {"5" + std::string(18, '0'), "3" + std::string(18, '0')}, 38, 18),
      "divide(c0, c1)",
      {makeFlatVector<int128_t>({2500, 12000}, DECIMAL(20, 2)), longFlat});

  // Divide short and short, returning short.
  testDecimalExpr<TypeKind::BIGINT>(
      makeFlatVector<int64_t>({500000000, 300000000}, DECIMAL(13, 11)),
      "divide(c0, c1)",
      {makeFlatVector<int64_t>({2500, 12000}, DECIMAL(5, 5)),
       makeFlatVector<int64_t>({500, 4000}, DECIMAL(5, 2))});
  // This result can be obtained by Spark unit test
  //   test("divide decimal big") {
  //     val s = Seq(35, 6, 20, 3)
  //     var builder = new StringBuffer()
  //     (0 until 29).foreach(_ => builder = builder.append("9"))
  //     builder.append(".")
  //     (0 until 6).foreach(_ => builder = builder.append("9"))
  //     val str1 = builder.toString

  //     val l1 = Literal.create(
  //       Decimal(BigDecimal(str1), s.head, s(1)),
  //       DecimalType(s.head, s(1)))
  //     val l2 = Literal.create(
  //       Decimal(BigDecimal(0.201), s(2), s(3)),
  //       DecimalType(s(2), s(3)))
  //     checkEvaluation(Divide(l1, l2), null)
  //   }
  testDecimalExpr<TypeKind::HUGEINT>(
      makeLongDecimalVector({"497512437810945273631840796019900493"}, 38, 6),
      "c0 / c1",
      {makeLongDecimalVector({std::string(35, '9')}, 35, 6),
       makeConstant<int128_t>(201, 1, DECIMAL(20, 3))});

  testDecimalExpr<TypeKind::HUGEINT>(
      makeLongDecimalVector(
          {"1000" + std::string(17, '0'), "500" + std::string(17, '0')},
          24,
          20),
      "1.00 / c0",
      {shortFlat});

  // Flat and Constant arguments.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeLongDecimalVector(
          {"500" + std::string(4, '0'), "1000" + std::string(4, '0')}, 23, 7),
      "c0 / 2.00",
      {shortFlat});

  // Divide and round-up.
  // The result can be obtained by Spark unit test
  //     test("divide test") {
  //     spark.sql("create table decimals_test(a decimal(2,1)) using parquet;")
  //     spark.sql("insert into decimals_test values(6)")
  //     val df = spark.sql("select a / -6.0 from decimals_test")
  //     df.printSchema()
  //     df.show(truncate = false)
  //     spark.sql("drop table decimals_test;")
  //   }
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>(
          {566667, -83333, -1083333, -1500000, -33333, 816667}, DECIMAL(8, 6))},
      "c0 / -6.0",
      {makeFlatVector<int64_t>({-34, 5, 65, 90, 2, -49}, DECIMAL(2, 1))});
  // Divide by zero.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeConstant<int128_t>(std::nullopt, 2, DECIMAL(21, 6)),
      "c0 / 0.0",
      {shortFlat});

  // Long decimal limits.
  testDecimalExpr<TypeKind::HUGEINT>(
      makeConstant<int128_t>(std::nullopt, 1, DECIMAL(38, 6)),
      "c0 / 0.01",
      {makeConstant<int128_t>(
          DecimalUtil::kLongDecimalMax, 1, DECIMAL(38, 0))});
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
