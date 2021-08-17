/*
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

#include "velox/experimental/codegen/code_generator/tests/CodegenExpressionsTestBase.h"

namespace facebook::velox {
namespace codegen::expressions::test {
class ArithmeticFunctionsTest : public ExpressionCodegenTestBase {
  void SetUp() override {
    ExpressionCodegenTestBase::SetUp();
    registerVeloxArithmeticUDFs(udfManager);
  }

 public:
  void runPlusTests();
  void runMinusTests();
  void runMultiplyTests();
};

void ArithmeticFunctionsTest::runPlusTests() {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "plus(C0, C0)", {{1}, {2}, {3}}, {{2}, {4}, {6}}, false);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "plus(C0, C0)", {{1}, {2}, {3}}, {{2}, {4}, {6}}, false);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "plus(C0, C0)", {{1}, {2}, {3}}, {{2}, {4}, {6}}, true);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>(
      "plus(C0, C0)", {{1}, {2}, {3}}, {{2}, {4}, {6}}, false);

  // Not nullable inputs.
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "plus(C0, 1)",
      {{1}, {2}, {3}},
      {{2}, {3}, {4}},
      false /*print code*/,
      {false} /*input rows nullability*/);
}

TEST_F(ArithmeticFunctionsTest, testPlusSymbols) {
  this->useBuiltInForArithmetic = true;
  runPlusTests();
}

TEST_F(ArithmeticFunctionsTest, testPlus) {
  this->useBuiltInForArithmetic = false;
  runPlusTests();
}

void ArithmeticFunctionsTest::runMinusTests() {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "minus(c0, c0)", {{1}, {-1}, {0}}, {{0}, {0}, {0}}, false);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "minus(c0, c0)", {{1}, {-1}, {0}}, {{0}, {0}, {0}}, false);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "minus(c0, c0)", {{1}, {-1}, {0}}, {{0}, {0}, {0}}, false);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>(
      "minus(c0, c0)", {{1}, {-1}, {0}}, {{0}, {0}, {0}}, false);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "minus(c0, 1)", {{0}, {1}, {2}}, {{-1}, {0}, {1}}, false);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "c0-10",
      {{1}, {2}, {3}},
      {{-9}, {-8}, {-7}},
      false /*print code*/,
      {false} /*input rows nullability*/);

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "1-10", {{}}, {{-9}}, false /*print code*/);
}

TEST_F(ArithmeticFunctionsTest, testMinusSymbols) {
  this->useBuiltInForArithmetic = true;
  runMinusTests();
}

TEST_F(ArithmeticFunctionsTest, testMinus) {
  this->useBuiltInForArithmetic = false;
  runMinusTests();
}

void ArithmeticFunctionsTest::runMultiplyTests() {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "multiply(C0, 1)", {{0}, {1}, {2}}, {{-0}, {1}, {2}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "multiply(C0, C0)", {{0}, {1}, {2}}, {{0}, {1}, {4}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "multiply(C0, C0)", {{0}, {1}, {2}}, {{0}, {1}, {4}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "multiply(C0, C0)", {{0}, {1}, {2}}, {{0}, {1}, {4}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>(
      "multiply(C0, C0)", {{0}, {1}, {2}}, {{0}, {1}, {4}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "C0*10",
      {{1}, {2}, {3}},
      {{10}, {20}, {30}},
      false /*print code*/,
      {false} /*input rows nullability*/);

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "1*10*10", {{}}, {{100}}, false /*print code*/);
}

TEST_F(ArithmeticFunctionsTest, testMultiplySymbols) {
  this->useBuiltInForArithmetic = true;
  runMultiplyTests();
}

TEST_F(ArithmeticFunctionsTest, testNullability) {
  auto runTests = [&]() {
    evaluateAndCompare<
        RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT, TypeKind::BIGINT>,
        RowTypeTrait<TypeKind::BIGINT>>(
        "plus(C0, plus(C1, C2))",
        {{1, 2, 3}, {2, 3, 4}, {3, std::nullopt, 5}},
        {{6}, {9}, {std::nullopt}});

    evaluateAndCompare<
        RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT, TypeKind::BIGINT>,
        RowTypeTrait<TypeKind::BIGINT>>(
        "plus(C0, plus(C1, C2))",
        {{1, 2, 3}, {2, 3, 4}, {3, std::nullopt, 5}},
        {{6}, {9}, {std::nullopt}},
        false /*print code*/,
        {false, true, false} /*input rows nullability*/);

    evaluateAndCompare<
        RowTypeTrait<TypeKind::DOUBLE, TypeKind::DOUBLE, TypeKind::DOUBLE>,
        RowTypeTrait<TypeKind::DOUBLE>>(
        "plus(C0 + 1.0, plus(C1, C2) - 2.0)",
        {{1.1, 2, 3}, {2.1, 3, 4}, {3, std::nullopt, 5}},
        {{5.1}, {8.1}, {std::nullopt}},
        false /*print code*/,
        {false, true, false} /*input rows nullability*/);
  };

  this->useBuiltInForArithmetic = true;
  runTests();

  this->useBuiltInForArithmetic = false;
  runTests();
}

TEST_F(ArithmeticFunctionsTest, complexExpression) {
  auto runTests = [&]() {
    evaluateAndCompare<
        RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT, TypeKind::BIGINT>,
        RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT, TypeKind::BIGINT>>(
        "concatRow(C0+C2, C0*C1, C2-C1+10)",
        {{1, 2, 3}, {2, 3, 4}, {3, 5, std::nullopt}},
        {{4, 2, 11}, {6, 6, 11}, {std::nullopt, 15, std::nullopt}});
  };

  this->useBuiltInForArithmetic = true;
  runTests();

  this->useBuiltInForArithmetic = false;
  runTests();
}

TEST_F(ArithmeticFunctionsTest, testMultiply) {
  this->useBuiltInForArithmetic = false;
  runMultiplyTests();
}

TEST_F(ArithmeticFunctionsTest, testDivideUDF) {
  this->useBuiltInForArithmetic = false;

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL, TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>(
      "divide(C0, C1)", {{0, 1}, {1, 2}, {2, 0.1}}, {{0}, {0.5}, {20}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE, TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "divide(C0, C1)", {{0, 1}, {1, 2}, {2, .1}}, {{0}, {0.5}, {20}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER, TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "divide(C0, C1)", {{0, 1}, {1, 2}, {2, 1}}, {{0}, {0}, {2}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "divide(C0, C1)", {{0, 1}, {1, 2}, {2, 1}}, {{0}, {0}, {2}});
}

TEST_F(ArithmeticFunctionsTest, testModuleUDF) {
  this->useBuiltInForArithmetic = false;

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER, TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "modulus(C0, C1)", {{0, 1}, {1, 2}, {2, 1}}, {{0}, {1}, {0}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT, TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "modulus(C0, C1)", {{0, 1}, {1, 2}, {2, 1}}, {{0}, {1}, {0}});

  auto runModuleReal = [&]() {
    evaluateAndCompare<
        RowTypeTrait<TypeKind::REAL, TypeKind::REAL>,
        RowTypeTrait<TypeKind::REAL>>(
        "modulus(C0, C1)", {{0, 1}, {1, 2}, {2, 1}}, {{0}, {1}, {0}});
  };

  // This wont even be typed by Velox
  ASSERT_THROW(runModuleReal(), std::invalid_argument);
}

TEST_F(ArithmeticFunctionsTest, testNegateUDF) {
  this->useBuiltInForArithmetic = false;

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>(
      "negate(C0)", {{0}, {1.1}, {-1.1}}, {{0}, {-1.1}, {1.1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "negate(C0)", {{0}, {1.1}, {-1.1}}, {{0}, {-1.1}, {1.1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "negate(C0)", {{0}, {1}, {-1}}, {{0}, {-1}, {1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "negate(C0)", {{0}, {1}, {-1}}, {{0}, {-1}, {1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "negate(C0)", {{0}, {1}, {-1}}, {{0}, {-1}, {1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "negate(C0)", {{0}, {1}, {-1}}, {{0}, {-1}, {1}});
}

TEST_F(ArithmeticFunctionsTest, testAbs) {
  this->useBuiltInForArithmetic = false;

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>(
      "abs(C0)", {{0}, {1.1}, {-1.1}}, {{0}, {1.1}, {1.1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "abs(C0)", {{0}, {1.1}, {-1.1}}, {{0}, {1.1}, {1.1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "abs(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "abs(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "abs(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "abs(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {1}});
}

TEST_F(ArithmeticFunctionsTest, testCeil) {
  this->useBuiltInForArithmetic = false;

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>(
      "ceil(C0)", {{0}, {1.1}, {-1.1}}, {{0}, {2}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "ceiling(C0)", {{0}, {1.1}, {-1.1}}, {{0}, {2}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "ceil(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "ceiling(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "ceil(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "ceiling(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {-1}});
}

TEST_F(ArithmeticFunctionsTest, testFloor) {
  this->useBuiltInForArithmetic = false;

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>(
      "floor(C0)", {{0}, {1.1}, {-1.1}}, {{0}, {1}, {-2}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "floor(C0)", {{0}, {1.1}, {-1.1}}, {{0}, {1}, {-2}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "floor(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "floor(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "floor(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "floor(C0)", {{0}, {1}, {-1}}, {{0}, {1}, {-1}});
}

TEST_F(ArithmeticFunctionsTest, DISABLED_testRound) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>(
      "round(C0,cast (1 as int))", {{0}, {1.1}, {-1.1}, {1.9}, {-1.9}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>(
      "round(C0)", {{0}, {1.1}, {-1.1}, {1.9}, {-1.9}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "round(C0,cast (1 as int))", {{0}, {1.1}, {-1.1}, {1.9}, {-1.9}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "round(C0)", {{0}, {1.1}, {-1.1}, {1.9}, {-1.9}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "round(C0,cast (-1 as int))", {{32}, {13}, {-13}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "round(C0)", {{32}, {13}, {-13}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "round(C0,cast (-1 as int))", {{32}, {13}, {-13}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "round(C0)", {{32}, {13}, {-13}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "round(C0,cast (-1 as int))", {{32}, {13}, {-13}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "round(C0)", {{32}, {13}, {-13}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "round(C0,cast (-1 as int))", {{32}, {13}, {-13}, {1}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "round(C0)", {{32}, {13}, {-13}, {1}, {-1}});
}

// FIXME: This test errors on macs:
// velox/velox/functions/common/HashImpl.h:22:31: error: implicit instantiation
// of undefined template 'folly::hasher<std::__1::basic_string_view<char, \
// std::__1::char_traits<char> >, void>'
TEST_F(ArithmeticFunctionsTest, DISABLED_testHash) {
  StringView input("hi welcome");
  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::BIGINT>>("hash(C0)", {input});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>("hash(C0)", {3});

  //   evaluateAndCompare<
  //       RowTypeTrait<TypeKind::INTEGER>,
  //       RowTypeTrait<TypeKind::BIGINT>>("hash(C0)", {3});

  //   evaluateAndCompare<
  //       RowTypeTrait<TypeKind::SMALLINT>,
  //       RowTypeTrait<TypeKind::BIGINT>>("hash(C0)", {3});

  //   evaluateAndCompare<
  //       RowTypeTrait<TypeKind::TINYINT>,
  //       RowTypeTrait<TypeKind::BIGINT>>("hash(C0)", {3});

  //   evaluateAndCompare<
  //       RowTypeTrait<TypeKind::DOUBLE>,
  //       RowTypeTrait<TypeKind::BIGINT>>("hash(C0)", {3.1312312321});

  //   evaluateAndCompare<
  //       RowTypeTrait<TypeKind::REAL>,
  //       RowTypeTrait<TypeKind::BIGINT>>("hash(C0)", {3.1312312321});
}

TEST_F(ArithmeticFunctionsTest, testRand) {
  // Rand is not deterministic, this assert the function is called with some
  // non-null results
  checkNullPropagation<RowTypeTrait<>>("rand()", {}, {false});
}

} // namespace codegen::expressions::test
} // namespace facebook::velox
