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
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::test {

class VectorToStringTest : public testing::Test, public VectorTestBase {};

TEST_F(VectorToStringTest, flatIntegers) {
  // No nulls.
  auto flat = makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
  ASSERT_EQ(flat->toString(), "[FLAT INTEGER: 10 elements, no nulls]");
  ASSERT_EQ(flat->toString(1), "2");
  ASSERT_EQ(flat->toString(3, 8, ", ", false), "4, 5, 6, 7, 8");

  // With nulls.
  flat = makeFlatVector<int32_t>(
      100, [](auto row) { return row; }, nullEvery(3));
  ASSERT_EQ(flat->toString(), "[FLAT INTEGER: 100 elements, 34 nulls]");
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
} // namespace facebook::velox::test
