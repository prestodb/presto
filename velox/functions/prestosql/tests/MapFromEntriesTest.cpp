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
#include <cstdint>
#include <optional>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/lib/CheckDuplicateKeys.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/vector/tests/TestingDictionaryArrayElementsFunction.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {
std::optional<std::vector<std::pair<int32_t, std::optional<int32_t>>>> O(
    const std::vector<std::pair<int32_t, std::optional<int32_t>>>& vector) {
  return std::make_optional(vector);
}
} // namespace

namespace {
class MapFromEntriesTest : public FunctionBaseTest {
 protected:
  /// Create an MAP vector of size 1 using specified 'keys' and 'values' vector.
  VectorPtr makeSingleRowMapVector(
      const VectorPtr& keys,
      const VectorPtr& values) {
    BufferPtr offsets = allocateOffsets(1, pool());
    BufferPtr sizes = allocateSizes(1, pool());
    sizes->asMutable<vector_size_t>()[0] = keys->size();

    return std::make_shared<MapVector>(
        pool(),
        MAP(keys->type(), values->type()),
        nullptr,
        1,
        offsets,
        sizes,
        keys,
        values);
  }

  void verifyMapFromEntries(
      const std::vector<VectorPtr>& input,
      const VectorPtr& expected,
      const std::string& funcArg = "C0",
      bool wrappedWithTry = false) {
    const std::string expr = wrappedWithTry
        ? fmt::format("try(map_from_entries({}))", funcArg)
        : fmt::format("map_from_entries({})", funcArg);
    auto result = evaluate<MapVector>(expr, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  // Evaluate an expression only, usually expect error thrown.
  void evaluateExpr(
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    evaluate(expression, makeRowVector(input));
  }
};
} // namespace

TEST_F(MapFromEntriesTest, intKeyAndVarcharValue) {
  auto rowType = ROW({INTEGER(), VARCHAR()});
  std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
      data = {
          {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
      };
  auto input = makeArrayOfRowVector(data, rowType);
  auto expected = makeMapVector<int32_t, StringView>(
      {{{1, "red"_sv}, {2, "blue"_sv}, {3, "green"_sv}}});
  verifyMapFromEntries({input}, expected);
}

TEST_F(MapFromEntriesTest, nullMapEntries) {
  auto rowType = ROW({INTEGER(), INTEGER()});
  {
    std::vector<std::vector<std::optional<std::tuple<int32_t, int32_t>>>> data =
        {
            {std::nullopt},
            {{{1, 11}}},
        };
    auto input = makeArrayOfRowVector(data, rowType);
    VELOX_ASSERT_THROW(
        evaluateExpr("map_from_entries(C0)", {input}),
        "map entry cannot be null");
    auto expected =
        makeNullableMapVector<int32_t, int32_t>({std::nullopt, O({{1, 11}})});
    verifyMapFromEntries({input}, expected, "C0", true);
  }
  {
    // Create array(row(a,b)) where a, b sizes are 0 because all row(a, b)
    // values are null.
    std::vector<std::vector<std::optional<std::tuple<int32_t, int32_t>>>> data =
        {
            {std::nullopt, std::nullopt, std::nullopt},
            {std::nullopt},
        };
    auto input = makeArrayOfRowVector(data, rowType);
    auto rowInput = input->as<ArrayVector>();
    rowInput->elements()->as<RowVector>()->childAt(0)->resize(0);
    rowInput->elements()->as<RowVector>()->childAt(1)->resize(0);

    VELOX_ASSERT_THROW(
        evaluateExpr("map_from_entries(C0)", {input}),
        "map entry cannot be null");
    auto expected =
        makeNullableMapVector<int32_t, int32_t>({std::nullopt, std::nullopt});
    verifyMapFromEntries({input}, expected, "C0", true);
  }
}

TEST_F(MapFromEntriesTest, nullKeys) {
  auto rowType = ROW({INTEGER(), INTEGER()});
  std::vector<std::vector<variant>> data = {
      {variant::row({variant::null(TypeKind::INTEGER), 0})},
      {variant::row({1, 11})}};
  auto input = makeArrayOfRowVector(rowType, data);
  VELOX_ASSERT_THROW(
      evaluateExpr("map_from_entries(C0)", {input}), "map key cannot be null");
  auto expected =
      makeNullableMapVector<int32_t, int32_t>({std::nullopt, O({{1, 11}})});
  verifyMapFromEntries({input}, expected, "C0", true);
}

TEST_F(MapFromEntriesTest, duplicateKeys) {
  auto rowType = ROW({INTEGER(), INTEGER()});
  std::vector<std::vector<std::optional<std::tuple<int32_t, int32_t>>>> data = {
      {{{1, 10}}, {{1, 11}}},
      {{{2, 22}}},
  };
  auto input = makeArrayOfRowVector(data, rowType);
  VELOX_ASSERT_THROW(
      evaluateExpr("map_from_entries(C0)", {input}),
      "Duplicate map keys (1) are not allowed");

  auto expected = makeMapVectorFromJson<int32_t, int32_t>({
      "null",
      "{2: 22}",
  });
  verifyMapFromEntries({input}, expected, "C0", true);
}

TEST_F(MapFromEntriesTest, nullValues) {
  auto rowType = ROW({INTEGER(), INTEGER()});
  std::vector<std::vector<variant>> data = {
      {variant::row({1, variant::null(TypeKind::INTEGER)}),
       variant::row({2, 22}),
       variant::row({3, 33})}};
  auto input = makeArrayOfRowVector(rowType, data);
  auto expected =
      makeMapVector<int32_t, int32_t>({{{1, std::nullopt}, {2, 22}, {3, 33}}});
  verifyMapFromEntries({input}, expected);
}

TEST_F(MapFromEntriesTest, constant) {
  const vector_size_t kConstantSize = 1'000;
  auto rowType = ROW({VARCHAR(), INTEGER()});
  std::vector<std::vector<std::optional<std::tuple<std::string, int32_t>>>>
      data = {
          {{{"red", 1}}, {{"blue", 2}}, {{"green", 3}}},
          {{{"red shiny car ahead", 4}}, {{"blue clear sky above", 5}}},
          {{{"r", 11}}, {{"g", 22}}, {{"b", 33}}},
      };
  auto input = makeArrayOfRowVector(data, rowType);

  auto evaluateConstant = [&](vector_size_t row, const VectorPtr& vector) {
    return evaluate(
        "map_from_entries(C0)",
        makeRowVector(
            {BaseVector::wrapInConstant(kConstantSize, row, vector)}));
  };

  auto result = evaluateConstant(0, input);
  auto expected = BaseVector::wrapInConstant(
      kConstantSize,
      0,
      makeSingleRowMapVector(
          makeFlatVector<StringView>({"red"_sv, "blue"_sv, "green"_sv}),
          makeFlatVector<int32_t>({1, 2, 3})));
  test::assertEqualVectors(expected, result);

  result = evaluateConstant(1, input);
  expected = BaseVector::wrapInConstant(
      kConstantSize,
      0,
      makeSingleRowMapVector(
          makeFlatVector<StringView>(
              {"red shiny car ahead"_sv, "blue clear sky above"_sv}),
          makeFlatVector<int32_t>({4, 5})));
  test::assertEqualVectors(expected, result);

  result = evaluateConstant(2, input);
  expected = BaseVector::wrapInConstant(
      kConstantSize,
      0,
      makeSingleRowMapVector(
          makeFlatVector<StringView>({"r"_sv, "g"_sv, "b"_sv}),
          makeFlatVector<int32_t>({11, 22, 33})));
  test::assertEqualVectors(expected, result);
}

TEST_F(MapFromEntriesTest, dictionaryEncodedElementsInFlat) {
  exec::registerVectorFunction(
      "testing_dictionary_array_elements",
      test::TestingDictionaryArrayElementsFunction::signatures(),
      std::make_unique<test::TestingDictionaryArrayElementsFunction>());

  auto rowType = ROW({INTEGER(), VARCHAR()});
  std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
      data = {
          {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
      };
  auto input = makeArrayOfRowVector(data, rowType);
  auto expected = makeMapVector<int32_t, StringView>(
      {{{1, "red"_sv}, {2, "blue"_sv}, {3, "green"_sv}}});
  verifyMapFromEntries(
      {input}, expected, "testing_dictionary_array_elements(C0)");
}

TEST_F(MapFromEntriesTest, outputSizeIsBoundBySelectedRows) {
  // This test makes sure that map_from_entries output vector size is
  // `rows.end()` instead of `rows.size()`.

  auto rowType = ROW({INTEGER(), INTEGER()});
  core::QueryConfig config({});
  auto function =
      exec::getVectorFunction("map_from_entries", {ARRAY(rowType)}, {}, config);

  std::vector<std::vector<std::optional<std::tuple<int32_t, int32_t>>>> data = {
      {{{1, 11}}, {{2, 22}}, {{3, 33}}},
      {{{4, 44}}, {{5, 55}}},
      {{{6, 66}}},
  };
  auto array = makeArrayOfRowVector(data, rowType);

  auto rowVector = makeRowVector({array});

  // Only the first 2 rows selected.
  SelectivityVector rows(2);
  // This is larger than input array size but rows beyond the input vector size
  // are not selected.
  rows.resize(1000, false);

  ASSERT_EQ(rows.size(), 1000);
  ASSERT_EQ(rows.end(), 2);
  ASSERT_EQ(array->size(), 3);

  auto typedExpr =
      makeTypedExpr("map_from_entries(c0)", asRowType(rowVector->type()));
  std::vector<VectorPtr> results(1);

  exec::ExprSet exprSet({typedExpr}, &execCtx_);
  exec::EvalCtx evalCtx(&execCtx_, &exprSet, rowVector.get());
  exprSet.eval(rows, evalCtx, results);

  ASSERT_EQ(results[0]->size(), 2);
}

TEST_F(MapFromEntriesTest, rowsWithNullsNotPassedToCheckDuplicateKey) {
  auto innerRowVector = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 2, 3, 4}),
       makeNullableFlatVector<int32_t>({1, 2, 3, 4})});

  auto offsets = makeIndices({0, 2});
  auto sizes = makeIndices({2, 2});

  auto arrayVector = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(ROW({INTEGER(), INTEGER()})),
      nullptr,
      2,
      offsets,
      sizes,
      innerRowVector);
  ASSERT_NO_THROW(
      evaluate("try(map_from_entries(c0))", makeRowVector({arrayVector})));
}

TEST_F(MapFromEntriesTest, arrayOfDictionaryRowOfNulls) {
  RowVectorPtr rowVector =
      makeRowVector({makeFlatVector<int32_t>(0), makeFlatVector<int32_t>(0)});
  rowVector->resize(4);
  rowVector->childAt(0)->resize(0);
  rowVector->childAt(1)->resize(0);
  for (int i = 0; i < rowVector->size(); i++) {
    rowVector->setNull(i, true);
  }

  EXPECT_EQ(rowVector->childAt(0)->size(), 0);
  EXPECT_EQ(rowVector->childAt(1)->size(), 0);

  auto indices = makeIndices({0, 1, 2, 3});

  auto dictionary =
      BaseVector::wrapInDictionary(nullptr, indices, 4, rowVector);

  auto offsets = makeIndices({0, 2});
  auto sizes = makeIndices({2, 2});

  auto arrayVector = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(ROW({INTEGER(), INTEGER()})),
      nullptr,
      2,
      offsets,
      sizes,
      dictionary);
  VectorPtr result =
      evaluate("try(map_from_entries(c0))", makeRowVector({arrayVector}));
  for (int i = 0; i < result->size(); i++) {
    EXPECT_TRUE(result->isNullAt(i));
  }
}

TEST_F(MapFromEntriesTest, arrayOfConstantRowOfNulls) {
  RowVectorPtr rowVector =
      makeRowVector({makeFlatVector<int32_t>(0), makeFlatVector<int32_t>(0)});
  rowVector->resize(1);
  rowVector->setNull(0, true);
  rowVector->childAt(0)->resize(0);
  rowVector->childAt(1)->resize(0);
  EXPECT_EQ(rowVector->childAt(0)->size(), 0);
  EXPECT_EQ(rowVector->childAt(1)->size(), 0);

  VectorPtr rowVectorConstant = BaseVector::wrapInConstant(4, 0, rowVector);

  auto offsets = makeIndices({0, 2});
  auto sizes = makeIndices({2, 2});

  auto arrayVector = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(ROW({INTEGER(), INTEGER()})),
      nullptr,
      2,
      offsets,
      sizes,
      rowVectorConstant);
  VectorPtr result =
      evaluate("try(map_from_entries(c0))", makeRowVector({arrayVector}));
  for (int i = 0; i < result->size(); i++) {
    EXPECT_TRUE(result->isNullAt(i));
  }
}

TEST_F(MapFromEntriesTest, arrayOfConstantNotNulls) {
  RowVectorPtr rowVector = makeRowVector(
      {makeFlatVector<int32_t>({1, 2}), makeFlatVector<int32_t>({3, 4})});
  rowVector->resize(1);
  rowVector->setNull(0, false);

  VectorPtr rowVectorConstant = BaseVector::wrapInConstant(4, 0, rowVector);
  {
    auto offsets = makeIndices({0, 2});
    auto sizes = makeIndices({2, 2});

    auto arrayVector = std::make_shared<ArrayVector>(
        pool(),
        ARRAY(ROW({INTEGER(), INTEGER()})),
        nullptr,
        2,
        offsets,
        sizes,
        rowVectorConstant);

    // will fail due to duplicate key.
    VectorPtr result =
        evaluate("try(map_from_entries(c0))", makeRowVector({arrayVector}));
    for (int i = 0; i < result->size(); i++) {
      EXPECT_TRUE(result->isNullAt(i));
    }
  }

  {
    auto offsets = makeIndices({0, 1});
    auto sizes = makeIndices({1, 1});

    auto arrayVector = std::make_shared<ArrayVector>(
        pool(),
        ARRAY(ROW({INTEGER(), INTEGER()})),
        nullptr,
        2,
        offsets,
        sizes,
        rowVectorConstant);

    // will fail due to duplicate key.
    VectorPtr result =
        evaluate("map_from_entries(c0)", makeRowVector({arrayVector}));
    auto expected = makeMapVector<int32_t, int32_t>(
        {{{1, 2}}, {{1, 2}}, {{1, 2}}, {{1, 2}}});
  }
}

TEST_F(MapFromEntriesTest, nestedNullInKeysSuccess) {
  auto arrayVector =
      makeNullableArrayVector<int32_t>({{2, std::nullopt}, {4, 5}});
  auto flatVector = makeFlatVector<int32_t>({1, 2});
  auto input = std::make_shared<ArrayVector>(
      pool(),
      /*type=*/ARRAY(ROW({ARRAY(INTEGER()), INTEGER()})),
      /*nulls=*/nullptr,
      /*length=*/1,
      /*offsets=*/makeIndices({0}),
      /*lengths=*/makeIndices({2}),
      /*elements=*/makeRowVector({arrayVector, flatVector}));

  VectorPtr result = evaluate("map_from_entries(c0)", makeRowVector({input}));

  assertEqualVectors(
      result,
      makeMapVector(
          /*offsets=*/{0},
          /*keyVector=*/arrayVector,
          /*valueVector=*/flatVector));
}

TEST_F(MapFromEntriesTest, unknownInputs) {
  auto expectedType = MAP(UNKNOWN(), UNKNOWN());
  auto test = [&](const std::string& query) {
    auto result = evaluate(query, makeRowVector({makeFlatVector<int32_t>(2)}));
    ASSERT_TRUE(result->type()->equivalent(*expectedType));
  };

  test("try(map_from_entries(array_constructor(row_constructor(null, null))))");
  test("try(map_from_entries(array_constructor(null)))");
  test("try(map_from_entries(null))");
}

TEST_F(MapFromEntriesTest, nullRowEntriesWithSmallerChildren) {
  // Row vector is of size 3, childrens are of size 2 since row 2 is null.
  auto rowVector = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 2}),
       makeFlatVector<int32_t>({1, 2})});
  rowVector->appendNulls(1);
  rowVector->setNull(2, true);

  // Array [(null,1), (2,2), null]
  auto arrayVector = makeArrayVector({0}, rowVector);
  auto result =
      evaluate("try(map_from_entries(c0))", makeRowVector({arrayVector}));
  result->validate();
  assertEqualVectors(
      BaseVector::createNullConstant(MAP(INTEGER(), INTEGER()), 1, pool()),
      result);
}

TEST_F(MapFromEntriesTest, allTopLevelNullsCornerCase) {
  // Test a corner case where the input of mapFromEntries is an array with
  // array at row 0 : [(null), (null)]
  // array at row 1 : []
  // And the function is evaluated only at row 1.

  auto keys = makeNullableFlatVector<int32_t>({});
  auto values = makeNullableFlatVector<int32_t>({});
  auto rowVector = makeRowVector({keys, values});

  EXPECT_EQ(rowVector->size(), 0);
  rowVector->appendNulls(2);

  EXPECT_EQ(rowVector->size(), 2);
  EXPECT_EQ(rowVector->childAt(0)->size(), 0);
  EXPECT_EQ(rowVector->childAt(1)->size(), 0);

  // Array at row 0 is [(null), (null)]
  // Array at row 1 is []
  auto arrayVector = makeArrayVector({0, 2}, rowVector);

  SelectivityVector rows(2);
  rows.setValid(0, false);

  auto result =
      evaluate("map_from_entries(c0)", makeRowVector({arrayVector}), rows);
  result->validate();
  auto expected = makeMapVectorFromJson<int32_t, int32_t>({"{}", "{}"});
  assertEqualVectors(expected, result);
}
