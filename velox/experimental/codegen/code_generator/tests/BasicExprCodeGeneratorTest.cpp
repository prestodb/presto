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

#include "velox/experimental/codegen/code_generator/tests/CodegenExpressionsTestBase.h"

namespace facebook::velox {
namespace codegen::expressions::test {

class CodegenBasicExprTest : public ExpressionCodegenTestBase {
  void SetUp() override {
    ExpressionCodegenTestBase::SetUp();
    registerVeloxArithmeticUDFs(udfManager);
  }
};

TEST_F(CodegenBasicExprTest, testConstant) {
  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "1", {{}, {}, {}}, {{1}, {1}, {1}});

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::DOUBLE>>(
      "1.0", {{}, {}, {}}, {{1.0}, {1.0}, {1.0}});
}

TEST_F(CodegenBasicExprTest, testInputRef) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>("C0", {{1}, {2}, {3}}, {{1}, {2}, {3}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT, TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::BIGINT, TypeKind::DOUBLE>>(
      "row_constructor(C0, C1)",
      {{1, 1.2}, {2, 1.3}, {3, 1.4}},
      {{1, 1.2}, {2, 1.3}, {3, 1.4}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT, TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::BIGINT, TypeKind::DOUBLE, TypeKind::BIGINT>>(
      "row_constructor(C0, C1, C0)",
      {{std::nullopt, 1.2}, {2, 1.3}, {3, 1.4}},
      {{std::nullopt, 1.2, std::nullopt}, {2, 1.3, 2}, {3, 1.4, 3}});
}

TEST_F(CodegenBasicExprTest, testIfExpressionNotEager) {
  // Constant inputs
  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "if((1=1), 1, 2)", {{}, {}, {}}, {{1}, {1}, {1}});

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "if((1!=1), 1, 2)", {{}, {}, {}}, {{2}, {2}, {2}});

  // C0 maybe null
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "if(C0, 1, 2)",
      {{true}, {false}, {true}},
      {{1}, {2}, {1}},
      false,
      {true});

  // C0 not nullable
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "if(C0, 1, 2)",
      {{true}, {false}, {true}},
      {{1}, {2}, {1}},
      false,
      {false});

  // Complex if expression
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN, TypeKind::BIGINT, TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "if(if(C0, (1!=1), (1=1)), C1+C2, C1-C2)",
      {{true, 1, 2}, {false, 10, 15}, {true, 11, 11}},
      {{-1}, {25}, {0}});

  // Not supported in Velox parser
  //  evaluateAndCompare<
  //      RowTypeTrait<TypeKind::BIGINT>,
  //      RowTypeTrait<TypeKind::BIGINT>>(
  //      "if(C0 = 1, 1)",
  //      {{1}, {2}, {3}},
  //      {{1}, {std::nullopt}, {std::nullopt}},
  //      false,
  //      {},
  //      false);

  // String type if
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::VARCHAR>>(
      "if(C0 = 1, 'is one','not one')",
      {{1}, {2}, {3}},
      {{StringView("is one")},
       {StringView("not one")},
       {StringView("not one")}});
}

TEST_F(CodegenBasicExprTest, testSwitchExpressionNotEager) {
  // Constant inputs form 1
  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "CASE 1 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END",
      {{}, {}, {}},
      {{1}, {1}, {1}},
      false,
      {},
      false);

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "CASE 2 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END",
      {{}, {}, {}},
      {{2}, {2}, {2}},
      false,
      {},
      false);

  // NULL Return
  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "CASE 3 WHEN 1 THEN 1 WHEN 2 THEN 2 END",
      {{}, {}, {}},
      {{}, {}, {}},
      false,
      {},
      false);

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "CASE 0 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END",
      {{}, {}, {}},
      {{3}, {3}, {3}},
      false,
      {},
      false);

  // Constant inputs form 2
  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "CASE WHEN 1=1 THEN 1 ELSE 2 END",
      {{}, {}, {}},
      {{1}, {1}, {1}},
      false,
      {},
      false);

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "CASE WHEN 1!=1 THEN 1 ELSE 2 END",
      {{}, {}, {}},
      {{2}, {2}, {2}},
      false,
      {},
      false);

  // C0 maybe null
  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "CASE C0 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END",
      {{1}, {2}, {}},
      {{1}, {2}, {3}},
      false,
      {true},
      false);

  // C0 not nullable
  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "CASE C0 WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END",
      {{1}, {2}, {0}},
      {{1}, {2}, {3}},
      false,
      {false},
      false);

  // nested case expression
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN, TypeKind::BIGINT, TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "CASE C0"
      "  WHEN TRUE THEN CASE "
      "                   WHEN C1 > 10 THEN C1 + C2 "
      "                   ELSE C1 - C2 "
      "                 END "
      "  ELSE 0 "
      "END ",
      {{true, 1, 2}, {true, 11, 12}, {false, 10, 15}},
      {{-1}, {23}, {0}},
      false,
      {false, false, false},
      false);

  // String type case
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::VARCHAR>>(
      "CASE C0 WHEN 1 THEN 'is one' WHEN 2 THEN 'is two' ELSE 'not one or two' END",
      {{1}, {2}, {3}},
      {{StringView("is one")},
       {StringView("is two")},
       {StringView("not one or two")}},
      false,
      {false},
      false);
}

TEST_F(CodegenBasicExprTest, testIsNull) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 IS NULL", {{}, {1}, {}}, {{true}, {false}, {true}}, false);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "CASE WHEN C0 IS NULL THEN 0 ELSE 1 END",
      {{}, {1}, {}},
      {{0}, {1}, {0}},
      false);

  // input not nullable, another one that will cause Velox Error
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "CASE WHEN C0 IS NULL THEN 0 ELSE 1 END",
      {{0}, {1}, {0}},
      {{1}, {1}, {1}},
      false,
      {false},
      false);
}

TEST_F(CodegenBasicExprTest, testNot) {
  // boolean input column
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "NOT C0", {{}, {true}, {false}}, {{}, {false}, {true}}, false);

  // non-boolean input column
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "NOT C0 < 1", {{}, {0}, {2}}, {{}, {false}, {true}}, false);
}

TEST_F(CodegenBasicExprTest, testCoalesce) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "\"coalesce\"(C0, cast (1 as INT))",
      {{std::nullopt}, {2}, {std::nullopt}},
      {{1}, {2}, {1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "\"coalesce\"(C0, cast (0 as INT))",
      {{1}, {2}, {3}},
      {{1}, {2}, {3}},
      false,
      {false});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "\"coalesce\"(C0, cast (0 as INT))",
      {{1}, {2}, {3}},
      {{1}, {2}, {3}},
      false,
      {true});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::VARCHAR>>(
      "\"coalesce\"(C0, 'no')",
      {StringView("yes"), StringView("maybe"), std::nullopt},
      {StringView("yes"), StringView("maybe"), StringView("no")});

  checkNullPropagation<RowTypeTrait<TypeKind::INTEGER>>(
      "\"coalesce\"(C0, cast (1 as INT))", {true}, {false});

  checkNullPropagation<RowTypeTrait<TypeKind::INTEGER>>(
      "\"coalesce\"(C0, cast (1 as INT))", {false}, {false});

  checkNullPropagation<RowTypeTrait<TypeKind::INTEGER>>(
      "\"coalesce\"(C0, C0)", {true}, {true});

  checkNullPropagation<RowTypeTrait<TypeKind::INTEGER>>(
      "\"coalesce\"(C0, C0)", {false}, {false});
}

} // namespace codegen::expressions::test
} // namespace facebook::velox
// ace facebook::velox
