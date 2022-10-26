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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;

namespace {

class ArrayConstructorTest : public functions::test::FunctionBaseTest {};

TEST_F(ArrayConstructorTest, basic) {
  vector_size_t size = 1'000;

  // no nulls
  auto a = makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; });
  auto b =
      makeFlatVector<int32_t>(size, [](vector_size_t row) { return row + 1; });
  {
    auto result = evaluate<ArrayVector>(
        "array_constructor(c0, c1)", makeRowVector({a, b}));
    auto resultElements = result->elements()->asFlatVector<int32_t>();
    for (vector_size_t row = 0; row < result->size(); row++) {
      ASSERT_FALSE(result->isNullAt(row)) << "at " << row;
      ASSERT_EQ(2, result->sizeAt(row)) << "at " << row;
      ASSERT_EQ(2 * row, result->offsetAt(row)) << "at " << row;

      auto offset = 2 * row;
      ASSERT_TRUE(a->equalValueAt(resultElements, row, offset));
      ASSERT_TRUE(b->equalValueAt(resultElements, row, offset + 1));
    }
  }

  // some nulls
  a = makeFlatVector<int32_t>(
      size, [](vector_size_t row) { return row; }, nullEvery(5));
  b = makeFlatVector<int32_t>(
      size, [](vector_size_t row) { return row + 1; }, nullEvery(7));
  {
    auto result = evaluate<ArrayVector>(
        "array_constructor(c0, c1)", makeRowVector({a, b}));
    auto resultElements = result->elements()->asFlatVector<int32_t>();
    for (vector_size_t row = 0; row < result->size(); row++) {
      ASSERT_FALSE(result->isNullAt(row)) << "at " << row;
      ASSERT_EQ(2, result->sizeAt(row)) << "at " << row;
      ASSERT_EQ(2 * row, result->offsetAt(row)) << "at " << row;

      auto offset = 2 * row;
      ASSERT_TRUE(a->equalValueAt(resultElements, row, offset));
      ASSERT_TRUE(b->equalValueAt(resultElements, row, offset + 1));
    }
  }

  // partially populated result
  {
    auto result = evaluate<ArrayVector>(
        "if(c0 % 2 = 0, array_constructor(c1, c0), array_constructor(c0, c1))",
        makeRowVector({a, b}));
    auto resultElements = result->elements()->asFlatVector<int32_t>();
    for (vector_size_t row = 0; row < result->size(); row++) {
      ASSERT_FALSE(result->isNullAt(row)) << "at " << row;
      ASSERT_EQ(2, result->sizeAt(row)) << "at " << row;

      auto offset = result->offsetAt(row);

      bool swap = !a->isNullAt(row) && a->valueAt(row) % 2 == 0;
      auto expectedA = swap ? b : a;
      auto expectedB = swap ? a : b;
      ASSERT_TRUE(expectedA->equalValueAt(resultElements, row, offset));
      ASSERT_TRUE(expectedB->equalValueAt(resultElements, row, offset + 1));
    }
  }
}

TEST_F(ArrayConstructorTest, encodings) {
  vector_size_t size = 1'000;

  // mix flat and constant encodings
  auto a = makeFlatVector<int64_t>(size, [](vector_size_t row) { return row; });
  auto result =
      evaluate<ArrayVector>("array_constructor(c0, 123)", makeRowVector({a}));
  auto resultElements = result->elements()->asFlatVector<int64_t>();
  for (vector_size_t row = 0; row < result->size(); row++) {
    ASSERT_FALSE(result->isNullAt(row)) << "at " << row;
    ASSERT_EQ(2, result->sizeAt(row)) << "at " << row;
    ASSERT_EQ(2 * row, result->offsetAt(row)) << "at " << row;

    auto offset = 2 * row;
    ASSERT_TRUE(a->equalValueAt(resultElements, row, offset));
    ASSERT_FALSE(resultElements->isNullAt(offset + 1));
    ASSERT_EQ(123, resultElements->valueAt(offset + 1));
  }
}

TEST_F(ArrayConstructorTest, empty) {
  vector_size_t size = 1'000;
  auto constantResult = evaluate<ConstantVector<ComplexType>>(
      "array_constructor()", makeRowVector(ROW({}, {}), size));
  ASSERT_EQ(size, constantResult->size());
  auto result =
      std::dynamic_pointer_cast<ArrayVector>(constantResult->valueVector());
  for (vector_size_t row = 0; row < result->size(); row++) {
    ASSERT_FALSE(constantResult->isNullAt(row)) << "at " << row;
    ASSERT_EQ(0, result->sizeAt(row)) << "at " << row;
    ASSERT_EQ(0, result->offsetAt(row)) << "at " << row;
  }

  auto cardinality = evaluate<ConstantVector<int64_t>>(
      "cardinality(array_constructor())", makeRowVector(ROW({}, {}), size));
  ASSERT_EQ(size, cardinality->size());
  for (vector_size_t row = 0; row < size; row++) {
    ASSERT_FALSE(cardinality->isNullAt(row)) << "at " << row;
    ASSERT_EQ(0, cardinality->valueAt(row)) << "at " << row;
  }

  auto a = makeFlatVector<int64_t>(size, [](vector_size_t row) { return row; });
  result = evaluate<ArrayVector>(
      "if(C0 % 2 = 0, array_constructor(C0), array_constructor())",
      makeRowVector({a}));
  auto resultElements = result->elements()->asFlatVector<int64_t>();
  ASSERT_EQ(size, result->size());
  for (vector_size_t row = 0; row < size; row++) {
    ASSERT_FALSE(result->isNullAt(row)) << "at " << row;
    if (row % 2 == 0) {
      ASSERT_EQ(0, result->sizeAt(1)) << "at " << row;
      ASSERT_EQ(row, resultElements->valueAt(result->offsetAt(row)));
    } else {
      ASSERT_EQ(0, result->sizeAt(row)) << "at " << row;
    }
  }
  // We produce the same result, this time a constant vector of
  // unknown type array gets morphed into an array vector of int64.
  auto sameResult = evaluate<ArrayVector>(
      "if(C0 % 2 = 1, array_constructor(), array_constructor(C0))",
      makeRowVector({a}));
  auto other = asArray(sameResult);
  EXPECT_TRUE(asArray(result)->equalValueAt(other.get(), 0, 0));
}

TEST_F(ArrayConstructorTest, reuseResultWithNulls) {
  vector_size_t size = 1'000;

  auto a = makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; });
  auto b =
      makeFlatVector<int32_t>(size, [](vector_size_t row) { return row + 1; });
  std::shared_ptr<BaseVector> reusedResult = makeArrayVector<int32_t>(
      size,
      [](vector_size_t /* row */) { return 1; },
      [](vector_size_t row) { return row; },
      [](vector_size_t /* row */) { return true; });
  SelectivityVector selected(size);

  auto result = evaluate<ArrayVector>(
      "array_constructor(c0, c1)",
      makeRowVector({a, b}),
      selected,
      reusedResult);
  auto resultElements = result->elements()->asFlatVector<int32_t>();
  for (vector_size_t row = 0; row < result->size(); row++) {
    ASSERT_FALSE(result->isNullAt(row)) << "at " << row;
    ASSERT_EQ(2, result->sizeAt(row)) << "at " << row;
    ASSERT_EQ(2 * row, result->offsetAt(row)) << "at " << row;

    auto offset = 2 * row;
    ASSERT_TRUE(a->equalValueAt(resultElements, row, offset));
    ASSERT_TRUE(b->equalValueAt(resultElements, row, offset + 1));
  }
}

TEST_F(ArrayConstructorTest, literals) {
  // Simple bigint literals.
  auto result =
      evaluate("array_constructor(1, 2, 3, 4, 5)", makeRowVector(ROW({}), 1));
  auto expected = makeArrayVector<int64_t>({{1, 2, 3, 4, 5}});
  test::assertEqualVectors(expected, result);

  // Add null literals.
  result = evaluate(
      "array_constructor(1, 2, null, 4, null)", makeRowVector(ROW({}), 1));
  expected =
      makeNullableArrayVector<int64_t>({{1, 2, std::nullopt, 4, std::nullopt}});
  test::assertEqualVectors(expected, result);

  // Double literals.
  result =
      evaluate("array_constructor(1.9, 2.4, 3.2)", makeRowVector(ROW({}), 1));
  expected = makeArrayVector<double>({{1.9, 2.4, 3.2}});
  test::assertEqualVectors(expected, result);

  // String literals.
  result = evaluate(
      "array_constructor('asd', '', 'def')", makeRowVector(ROW({}), 1));
  expected = makeArrayVector<StringView>({{"asd", "", "def"}});
  test::assertEqualVectors(expected, result);

  // Mixing literals is not allowed.
  EXPECT_THROW(
      evaluate("array_constructor('asd', 10, 99.9)", makeRowVector(ROW({}), 1)),
      VeloxUserError);
}

} // namespace
