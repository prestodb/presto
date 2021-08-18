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

class CastExprTest : public ExpressionCodegenTestBase {
  void SetUp() override {
    ExpressionCodegenTestBase::SetUp();
  }
};

TEST_F(CastExprTest, castByTruncate) {
  getExprCodeGenerator().castIntByTruncate = true;
  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "cast (C0 as INT)",
      {{1.6}, {0.9}, {-1}},
      {1, 0, -1},
      false,
      {},
      false /*compare with velox*/);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "cast (C0 as INT)",
      {{1.1}, {0.9}, {-1}},
      {1, 0, -1},
      false,
      {},
      false /*compare with velox*/);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "cast (C0 as BIGINT)",
      {{1.6}, {0.9}, {-1}},
      {1, 0, -1},
      false,
      {},
      false /*compare with velox*/);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "cast (C0 as BIGINT)",
      {{1.1}, {0.9}, {-1}},
      {1, 0, -1},
      false,
      {},
      false /*compare with velox*/);
}

TEST_F(CastExprTest, testCastToInteger) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "cast (C0 as INT)", {{1.6}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "cast (C0 as INT)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "cast (C0 as INT)", {{2147483647}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::INTEGER>>("cast (C0 as INT)", {{23}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::INTEGER>>("cast (C0 as INT)", {{1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::INTEGER>>("cast (C0 as INT)", {{1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::INTEGER>>(
      "cast (C0 as INT)",
      {{StringView("100")}, {StringView("1")}, {StringView("-1")}});

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::INTEGER>>(
      "cast ('1' as INT)", {{}, {}});
}

TEST_F(CastExprTest, testCastToBigInt) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "cast (C0 as BIGINT)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "cast (C0 as BIGINT)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "cast (C0 as BIGINT)", {{2147483647}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "cast (C0 as BIGINT)", {{23}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "cast (C0 as BIGINT)", {{1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "cast (C0 as BIGINT)", {{1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "cast (C0 as BIGINT)",
      {{StringView("100")}, {StringView("1")}, {StringView("-1")}});

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::BIGINT>>(
      "cast ('2147483647 ' as BIGINT)", {{}, {}});
}

TEST_F(CastExprTest, testCastToSmallInt) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "cast (C0 as SMALLINT)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "cast (C0 as SMALLINT)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "cast (C0 as SMALLINT)", {{12}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "cast (C0 as SMALLINT)", {{23}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "cast (C0 as SMALLINT)", {{1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "cast (C0 as SMALLINT)", {{1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::SMALLINT>>(
      "cast (C0 as SMALLINT)",
      {{StringView("100")}, {StringView("1")}, {StringView("-1")}});

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::SMALLINT>>(
      "cast ('2 ' as SMALLINT)", {{}, {}});
}

TEST_F(CastExprTest, testCastToTinyInt) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "cast (C0 as TINYINT)", {{1.1}, {0.9}, {-1}}, {{1}, {1}, {-1}}, true);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "cast (C0 as TINYINT)", {{1.1}, {0.9}, {-1}}, {{1}, {1}, {-1}}, false);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "cast (C0 as TINYINT)", {{43}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "cast (C0 as TINYINT)", {{23}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "cast (C0 as TINYINT)", {{1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "cast (C0 as TINYINT)", {{1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::TINYINT>>(
      "cast (C0 as TINYINT)",
      {{StringView("100")}, {StringView("1")}, {StringView("-1")}});

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::TINYINT>>(
      "cast ('2 ' as TINYINT)", {{}, {}});
}

TEST_F(CastExprTest, testCastDouble) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "cast (C0 as DOUBLE)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "cast (C0 as DOUBLE)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "cast (C0 as DOUBLE)", {{43.}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "cast (C0 as DOUBLE)", {{23.12}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "cast (C0 as DOUBLE)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "cast (C0 as DOUBLE)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::DOUBLE>>(
      "cast (C0 as DOUBLE)",
      {{StringView("100.12")}, {StringView("0.1")}, {StringView("-1")}});

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::DOUBLE>>(
      "cast ('2.11' as DOUBLE)", {{}, {}});
}

TEST_F(CastExprTest, testCastReal) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::DOUBLE>,
      RowTypeTrait<TypeKind::REAL>>("cast (C0 as REAL)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::REAL>,
      RowTypeTrait<TypeKind::REAL>>("cast (C0 as REAL)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::BIGINT>,
      RowTypeTrait<TypeKind::REAL>>("cast (C0 as REAL)", {{43.}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::INTEGER>,
      RowTypeTrait<TypeKind::REAL>>(
      "cast (C0 as REAL)", {{23.12}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::TINYINT>,
      RowTypeTrait<TypeKind::REAL>>("cast (C0 as REAL)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::SMALLINT>,
      RowTypeTrait<TypeKind::REAL>>("cast (C0 as REAL)", {{1.1}, {0.9}, {-1}});

  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::REAL>>(
      "cast (C0 as REAL)",
      {{StringView("100.12")}, {StringView("0.1")}, {StringView("-1")}});

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::REAL>>(
      "cast ('2.11' as REAL)", {{}, {}});
}
} // namespace codegen::expressions::test
} // namespace facebook::velox
