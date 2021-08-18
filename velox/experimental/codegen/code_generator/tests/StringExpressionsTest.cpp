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

class StringExprTest : public ExpressionCodegenTestBase {
  void SetUp() override {
    ExpressionCodegenTestBase::SetUp();
    registerVeloxArithmeticUDFs(udfManager);
    registerVeloxStringFunctions(udfManager);
  }
};

TEST_F(StringExprTest, testStringConstant) {
  StringView str("1.0");

  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::VARCHAR>>(
      "'1.0'", {{}}, {{str}}, true);

  std::string longString =
      "*************************************not-inlined-string*********************************************";
  evaluateAndCompare<RowTypeTrait<>, RowTypeTrait<TypeKind::VARCHAR>>(
      fmt::format("'{}'", longString), {{}}, {{StringView(longString)}});
}

TEST_F(StringExprTest, ifString) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR, TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::VARCHAR, TypeKind::VARCHAR>>(
      "concatRow(C0,C1)",
      {{StringView("one"), StringView("two")},
       {StringView("one"), StringView("two")},
       {StringView("one"), StringView("two")}});
}

TEST_F(StringExprTest, coalesceString) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR, TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::VARCHAR>>(
      "\"coalesce\"(C0, C1, '3')",
      {{std::nullopt, StringView("2")},
       {StringView("1"), std::nullopt},
       {std::nullopt, std::nullopt}},
      {{StringView("2")}, {StringView("1")}, {StringView("3")}},
      true);
}

TEST_F(StringExprTest, upperString) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::VARCHAR>>(
      "upper(C0)",
      {std::nullopt,
       StringView("ascii"),
       StringView("àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ")},
      {std::nullopt,
       StringView("ASCII"),
       StringView("ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ")},
      true);
}

TEST_F(StringExprTest, lowerString) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::VARCHAR>>(
      "lower(C0)",
      {std::nullopt,
       StringView("ASCII"),
       StringView("ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ")},
      {std::nullopt,
       StringView("ascii"),
       StringView("àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ")},
      false);
}

TEST_F(StringExprTest, concatString) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::VARCHAR>>(
      "concat(C0, C0)",
      {std::nullopt,
       StringView("ASCII"),
       StringView("ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ")},
      {std::nullopt,
       StringView("ASCIIASCII"),
       StringView(
           "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ")},
      false);

  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR, TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::VARCHAR>>(
      "concat(C0, '+', C1)",
      {{std::nullopt, StringView("1")},
       {StringView("1"), StringView("2")},
       {StringView("1"), std::nullopt}});
}

TEST_F(StringExprTest, length) {
  evaluateAndCompare<
      RowTypeTrait<TypeKind::VARCHAR>,
      RowTypeTrait<TypeKind::BIGINT>>(
      "length(C0)",
      {std::nullopt,
       StringView("ASCII"),
       StringView("ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ")});
}

} // namespace codegen::expressions::test
} // namespace facebook::velox
