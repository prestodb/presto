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
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

class VectorToStringTest : public testing::Test, public VectorTestBase {};

TEST_F(VectorToStringTest, flatIntegers) {
  // No nulls.
  auto flat = makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
  ASSERT_EQ(flat->toString(), "[FLAT INTEGER: 10 elements, no nulls]");
  ASSERT_EQ(flat->toString(true), "[FLAT INTEGER: 10 elements, no nulls]");
  ASSERT_EQ(flat->toString(1), "2");
  ASSERT_EQ(flat->toString(3, 8, ", ", false), "4, 5, 6, 7, 8");

  // With nulls.
  flat = makeFlatVector<int32_t>(
      100, [](auto row) { return row; }, nullEvery(3));
  ASSERT_EQ(flat->toString(), "[FLAT INTEGER: 100 elements, 34 nulls]");
  ASSERT_EQ(flat->toString(true), "[FLAT INTEGER: 100 elements, 34 nulls]");
  ASSERT_EQ(flat->toString(1), "1");
  ASSERT_EQ(flat->toString(33), "null");
  ASSERT_EQ(flat->toString(0, 7, ", ", false), "null, 1, 2, null, 4, 5, null");
}

TEST_F(VectorToStringTest, arrayOfIntegers) {
  // No nulls.
  auto arr = makeArrayVector<int64_t>(
      {{0}, {1, 2}, {3, 4, 5}, {6}, {}, {8, 9, 10, 11}});
  ASSERT_EQ(arr->toString(), "[ARRAY ARRAY<BIGINT>: 6 elements, no nulls]");
  ASSERT_EQ(arr->toString(3), "1 elements starting at 6 {6}");
  ASSERT_EQ(
      arr->toString(2, 6),
      "2: 3 elements starting at 3 {3, 4, 5}\n"
      "3: 1 elements starting at 6 {6}\n"
      "4: <empty>\n"
      "5: 4 elements starting at 7 {8, 9, 10, 11}");

  // With nulls.
  arr = makeNullableArrayVector<int64_t>(
      {{0}, {1, 2}, {3, 4, 5}, {6}, {}, {std::nullopt, 5}, {8, 9, 10, 11}});
  ASSERT_EQ(arr->toString(), "[ARRAY ARRAY<BIGINT>: 7 elements, 0 nulls]");
  ASSERT_EQ(arr->toString(4), "<empty>");
  ASSERT_EQ(
      arr->toString(2, 6),
      "2: 3 elements starting at 3 {3, 4, 5}\n"
      "3: 1 elements starting at 6 {6}\n"
      "4: <empty>\n"
      "5: 2 elements starting at 7 {null, 5}");
}

TEST_F(VectorToStringTest, mapOfIntegerToDouble) {
  auto map = makeMapVector<int32_t, double>(
      {{{1, 0.1}, {2, 0.2}, {3, 0.3}}, {}, {{4, 0.4}, {5, 0.5}}});
  ASSERT_EQ(map->toString(), "[MAP MAP<INTEGER,DOUBLE>: 3 elements, no nulls]");
  ASSERT_EQ(
      map->toString(0),
      "3 elements starting at 0 {1 => 0.1, 2 => 0.2, 3 => 0.3}");
  ASSERT_EQ(
      map->toString(2, 6), "2: 2 elements starting at 3 {4 => 0.4, 5 => 0.5}");
}

TEST_F(VectorToStringTest, row) {
  auto row = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<float>({10.1, 2.3, 444.56}),
      makeConstant(true, 3),
  });
  ASSERT_EQ(
      row->toString(),
      "[ROW ROW<c0:INTEGER,c1:REAL,c2:BOOLEAN>: 3 elements, no nulls]");
  ASSERT_EQ(row->toString(2), "{3, 444.55999755859375, 1}");
  ASSERT_EQ(
      row->toString(0, 10),
      "0: {1, 10.100000381469727, 1}\n"
      "1: {2, 2.299999952316284, 1}\n"
      "2: {3, 444.55999755859375, 1}");
}

TEST_F(VectorToStringTest, opaque) {
  auto opaque = BaseVector::create(OPAQUE<int>(), 10, pool_.get());

  ASSERT_EQ(opaque->toString(), "[FLAT OPAQUE<int>: 10 elements, no nulls]");
  ASSERT_EQ(
      opaque->toString(0, 3),
      "0: <opaque>\n"
      "1: <opaque>\n"
      "2: <opaque>");
}

TEST_F(VectorToStringTest, decimals) {
  auto shortDecimalFlatVector = makeShortDecimalFlatVector(
      {1000265, 35610, -314159, 7, 0}, DECIMAL(10, 3));
  ASSERT_EQ(
      shortDecimalFlatVector->toString(),
      "[FLAT SHORT_DECIMAL(10,3): 5 elements, no nulls]");
  ASSERT_EQ(
      shortDecimalFlatVector->toString(0, 5),
      "0: 1000.265\n"
      "1: 35.610\n"
      "2: -314.159\n"
      "3: 0.007\n"
      "4: 0");

  auto longDecimalFlatVector = makeLongDecimalFlatVector(
      {1000265, 35610, -314159, 7, 0}, DECIMAL(20, 4));
  ASSERT_EQ(
      longDecimalFlatVector->toString(),
      "[FLAT LONG_DECIMAL(20,4): 5 elements, no nulls]");
  ASSERT_EQ(
      longDecimalFlatVector->toString(0, 5),
      "0: 100.0265\n"
      "1: 3.5610\n"
      "2: -31.4159\n"
      "3: 0.0007\n"
      "4: 0");
}

TEST_F(VectorToStringTest, nullableDecimals) {
  auto shortDecimalFlatVector = makeNullableShortDecimalFlatVector(
      {1000265, 35610, -314159, 7, std::nullopt}, DECIMAL(10, 3));
  ASSERT_EQ(
      shortDecimalFlatVector->toString(),
      "[FLAT SHORT_DECIMAL(10,3): 5 elements, 1 nulls]");
  ASSERT_EQ(
      shortDecimalFlatVector->toString(0, 5),
      "0: 1000.265\n"
      "1: 35.610\n"
      "2: -314.159\n"
      "3: 0.007\n"
      "4: null");

  auto longDecimalFlatVector = makeNullableLongDecimalFlatVector(
      {1000265, 35610, -314159, 7, std::nullopt}, DECIMAL(20, 4));
  ASSERT_EQ(
      longDecimalFlatVector->toString(),
      "[FLAT LONG_DECIMAL(20,4): 5 elements, 1 nulls]");
  ASSERT_EQ(
      longDecimalFlatVector->toString(0, 5),
      "0: 100.0265\n"
      "1: 3.5610\n"
      "2: -31.4159\n"
      "3: 0.0007\n"
      "4: null");
}

TEST_F(VectorToStringTest, constant) {
  // Null constant.
  auto nullConstant = makeConstant<int32_t>(std::nullopt, 100);
  ASSERT_EQ(
      nullConstant->toString(true), "[CONSTANT INTEGER: 100 elements, null]");
  ASSERT_EQ(nullConstant->toString(17), "null");

  // Null constant of UNKNOWN type.
  auto unknownNullConstant = makeConstant<UnknownValue>(std::nullopt, 123);
  ASSERT_EQ(
      unknownNullConstant->toString(true),
      "[CONSTANT UNKNOWN: 123 elements, null]");
  ASSERT_EQ(unknownNullConstant->toString(21), "null");

  // Non-null constant.
  auto nonNullConstant = makeConstant<int32_t>(75, 100);
  ASSERT_EQ(
      nonNullConstant->toString(true), "[CONSTANT INTEGER: 100 elements, 75]");
  ASSERT_EQ(nonNullConstant->toString(44), "75");

  // Non-null complex-type constant.
  auto arrayVector = makeArrayVector<int32_t>({
      {1, 2, 3},
      {4, 5},
      {6, 7, 8, 9},
  });

  auto constant = BaseVector::wrapInConstant(100, 1, arrayVector);
  ASSERT_EQ(
      constant->toString(true),
      "[CONSTANT ARRAY<INTEGER>: 100 elements, 2 elements starting at 3 {4, 5}], "
      "[ARRAY ARRAY<INTEGER>: 3 elements, no nulls]");
  ASSERT_EQ(constant->toString(3), "2 elements starting at 3 {4, 5}");
}

TEST_F(VectorToStringTest, dictionary) {
  auto flat = makeFlatVector<int32_t>({1, 2, 3});
  auto flatWithNulls =
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 4, 5});

  // Dictionary over flat. No nulls.
  auto dict = wrapInDictionary(makeIndicesInReverse(3), 3, flat);
  ASSERT_EQ(dict->toString(), "[DICTIONARY INTEGER: 3 elements, no nulls]");
  ASSERT_EQ(
      dict->toString(true),
      "[DICTIONARY INTEGER: 3 elements, no nulls], "
      "[FLAT INTEGER: 3 elements, no nulls]");

  // 2 layers of dictionary over flat with nulls.
  auto doubleDict = BaseVector::wrapInDictionary(
      makeNulls(4, nullEvery(2)),
      makeIndices({0, 0, 2, 2}),
      4,
      wrapInDictionary(makeIndicesInReverse(3), 3, flatWithNulls));
  ASSERT_EQ(
      doubleDict->toString(), "[DICTIONARY INTEGER: 4 elements, 2 nulls]");
  ASSERT_EQ(
      doubleDict->toString(true),
      "[DICTIONARY INTEGER: 4 elements, 2 nulls], "
      "[DICTIONARY INTEGER: 3 elements, no nulls], "
      "[FLAT INTEGER: 5 elements, 1 nulls]");

  // Dictionary over constant.
  auto dictOverConst = BaseVector::wrapInDictionary(
      makeNulls(4, nullEvery(2)),
      makeIndices({0, 0, 0, 0}),
      4,
      makeConstant<int32_t>(75, 100));
  ASSERT_EQ(
      dictOverConst->toString(), "[DICTIONARY INTEGER: 4 elements, 2 nulls]");
  ASSERT_EQ(
      dictOverConst->toString(true),
      "[DICTIONARY INTEGER: 4 elements, 2 nulls], "
      "[CONSTANT INTEGER: 100 elements, 75]");
}

TEST_F(VectorToStringTest, printNulls) {
  // No nulls.
  BufferPtr nulls = allocateNulls(1024, pool());
  EXPECT_EQ(printNulls(nulls), "0 out of 1024 rows are null");

  // Some nulls.
  for (auto i = 3; i < 1024; i += 7) {
    bits::setNull(nulls->asMutable<uint64_t>(), i);
  }
  EXPECT_EQ(
      printNulls(nulls),
      "146 out of 1024 rows are null: ...n......n......n......n.....");

  EXPECT_EQ(
      printNulls(nulls, 15), "146 out of 1024 rows are null: ...n......n....");

  EXPECT_EQ(
      printNulls(nulls, 50),
      "146 out of 1024 rows are null: "
      "...n......n......n......n......n......n......n....");

  // All nulls.
  for (auto i = 0; i < 1024; ++i) {
    bits::setNull(nulls->asMutable<uint64_t>(), i);
  }
  EXPECT_EQ(
      printNulls(nulls),
      "1024 out of 1024 rows are null: nnnnnnnnnnnnnnnnnnnnnnnnnnnnnn");

  // Short buffer.
  nulls = allocateNulls(5, pool());
  bits::setNull(nulls->asMutable<uint64_t>(), 1);
  bits::setNull(nulls->asMutable<uint64_t>(), 2);
  bits::setNull(nulls->asMutable<uint64_t>(), 4);
  EXPECT_EQ(printNulls(nulls), "3 out of 8 rows are null: .nn.n...");
}

TEST_F(VectorToStringTest, printIndices) {
  BufferPtr indices = allocateIndices(1024, pool());
  EXPECT_EQ(
      printIndices(indices),
      "1 unique indices out of 1024: 0, 0, 0, 0, 0, 0, 0, 0, 0, 0");

  indices = makeIndices(1024, [](auto row) { return row / 3; });
  EXPECT_EQ(
      printIndices(indices),
      "342 unique indices out of 1024: 0, 0, 0, 1, 1, 1, 2, 2, 2, 3");

  indices = makeIndices(1024, [](auto row) { return row % 7; });
  EXPECT_EQ(
      printIndices(indices),
      "7 unique indices out of 1024: 0, 1, 2, 3, 4, 5, 6, 0, 1, 2");

  indices = makeIndices(1024, [](auto row) { return row; });
  EXPECT_EQ(
      printIndices(indices),
      "1024 unique indices out of 1024: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9");

  indices = makeIndices(1024, [](auto row) { return row; });
  EXPECT_EQ(
      printIndices(indices, 15),
      "1024 unique indices out of 1024: "
      "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14");

  indices = makeIndices({34, 79, 11, 0, 0, 33});
  EXPECT_EQ(
      printIndices(indices), "5 unique indices out of 6: 34, 79, 11, 0, 0, 33");
}
} // namespace facebook::velox::test
