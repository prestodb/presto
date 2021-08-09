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

class LogicalExprTest : public ExpressionCodegenTestBase {
  void SetUp() override {
    ExpressionCodegenTestBase::SetUp();
    registerVeloxArithmeticUDFs(udfManager);
  }
};

TEST_F(LogicalExprTest, testGt) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER, TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 > C1",
      {{0, 1}, {1, 0}, {1, std::nullopt}},
      {{false}, {true}, {std::nullopt}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL, TypeKind::REAL>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 > C1",
      {{0, 1}, {1, 0}, {1, std::nullopt}},
      {{false}, {true}, {std::nullopt}});
}

TEST_F(LogicalExprTest, testLt) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER, TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 < C1",
      {{0, 1}, {1, 0}, {1, std::nullopt}},
      {{true}, {false}, {std::nullopt}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL, TypeKind::REAL>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 < C1",
      {{0, 1}, {1, 0}, {1, std::nullopt}},
      {{true}, {false}, {std::nullopt}});
}

TEST_F(LogicalExprTest, testGte) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER, TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 >= C1",
      {{0, 1}, {0, 0}, {1, std::nullopt}},
      {{false}, {true}, {std::nullopt}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL, TypeKind::REAL>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 >= C1",
      {{0, 1}, {0, 0}, {1, std::nullopt}},
      {{false}, {true}, {std::nullopt}});
}

TEST_F(LogicalExprTest, testLte) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER, TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 <= C1",
      {{0, 1}, {0, 0}, {1, std::nullopt}},
      {{true}, {true}, {std::nullopt}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL, TypeKind::REAL>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 <= C1",
      {{0, 1}, {0, 0}, {1, std::nullopt}},
      {{true}, {true}, {std::nullopt}});
}

TEST_F(LogicalExprTest, testEq) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER, TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 = C1",
      {{0, 1}, {0, 0}, {1, std::nullopt}},
      {{false}, {true}, {std::nullopt}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL, TypeKind::REAL>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 = C1",
      {{0, 1}, {0, 0}, {1, std::nullopt}},
      {{false}, {true}, {std::nullopt}});
}

TEST_F(LogicalExprTest, testNeq) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER, TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 != C1",
      {{0, 1}, {0, 0}, {1, std::nullopt}},
      {{true}, {false}, {std::nullopt}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL, TypeKind::REAL>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 != C1",
      {{0, 1}, {0, 0}, {1, std::nullopt}},
      {{true}, {false}, {std::nullopt}});
}

TEST_F(LogicalExprTest, testAnd) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN, TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 and C1",
      {{true, true},
       {true, false},
       {true, std::nullopt},
       {false, true},
       {false, false},
       {false, std::nullopt},
       {std::nullopt, true},
       {std::nullopt, false},
       {std::nullopt, std::nullopt}},
      {{true},
       {false},
       {std::nullopt},
       {false},
       {false},
       {false},
       {std::nullopt},
       {false},
       {std::nullopt}},
      true);

  // Not nullable Inputs
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN, TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 and C1",
      {{true, true}, {true, false}, {false, true}, {false, false}},
      {{true}, {false}, {false}, {false}},
      false,
      {false, false});

  // Not nullable lhs
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN, TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 and C1",
      {{true, true},
       {true, false},
       {true, std::nullopt},
       {false, true},
       {false, false},
       {false, std::nullopt}},
      {{true}, {false}, {std::nullopt}, {false}, {false}, {false}},
      false,
      {false, true});

  // Not nullable rhs
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN, TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 and C1",
      {{true, true},
       {true, false},
       {false, true},
       {false, false},
       {std::nullopt, true},
       {std::nullopt, false}},
      {{true}, {false}, {false}, {false}, {std::nullopt}, {false}},
      false,
      {true, false});
}

TEST_F(LogicalExprTest, testOr) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN, TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 or C1",
      {{true, true},
       {true, false},
       {true, std::nullopt},
       {false, true},
       {false, false},
       {false, std::nullopt},
       {std::nullopt, true},
       {std::nullopt, false},
       {std::nullopt, std::nullopt}},
      {{true},
       {true},
       {true},
       {true},
       {false},
       {std::nullopt},
       {true},
       {std::nullopt},
       {std::nullopt}});

  // Not nullable Inputs
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN, TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 or C1",
      {{true, true}, {true, false}, {false, true}, {false, false}},
      {{true}, {true}, {true}, {false}});

  // Not nullable lhs
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN, TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 or C1",
      {{true, true},
       {true, false},
       {true, std::nullopt},
       {false, true},
       {false, false},
       {false, std::nullopt}},
      {{true}, {true}, {true}, {true}, {false}, std::nullopt},
      false,
      {false, true});

  // Not nullable rhs
  evaluateAndCompare<
      RowTypeTrait<TypeKind::BOOLEAN, TypeKind::BOOLEAN>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 or C1",
      {{true, true},
       {true, false},
       {false, true},
       {false, false},
       {std::nullopt, true},
       {std::nullopt, false}},
      {{true}, {true}, {true}, {false}, {true}, {std::nullopt}},
      false,
      {true, false});
}

TEST_F(LogicalExprTest, testBetween) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::BOOLEAN>>("C0 between 3 and 4", {{3}, {4}, {5}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BOOLEAN>>("C0 between 3 and 4", {{3}, {4}, {5}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::BOOLEAN>>("C0 between 3 and 4", {{3}, {4}, {5}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::BOOLEAN>>("C0 between 3 and 4", {{3}, {4}, {5}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 between 3.0 and 4.0", {{3}, {4}, {5}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::BOOLEAN>>(
      "C0 between 3.0 and 4.0", {{3}, {4}, {5}});
}

} // namespace codegen::expressions::test
} // namespace facebook::velox
