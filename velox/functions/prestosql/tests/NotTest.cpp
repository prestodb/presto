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
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class NotTest : public functions::test::FunctionBaseTest {};

TEST_F(NotTest, noNulls) {
  constexpr vector_size_t size = 1'000;

  // All true.
  auto allTrue = makeFlatVector<bool>(size, [](auto /*row*/) { return true; });
  auto result =
      evaluate<SimpleVector<bool>>("not(c0)", makeRowVector({allTrue}));
  for (int i = 0; i < size; ++i) {
    EXPECT_FALSE(result->valueAt(i)) << "at " << i;
  }

  // All false.
  auto allFalse =
      makeFlatVector<bool>(size, [](auto /*row*/) { return false; });
  result = evaluate<SimpleVector<bool>>("not(c0)", makeRowVector({allFalse}));
  for (int i = 0; i < size; ++i) {
    EXPECT_TRUE(result->valueAt(i)) << "at " << i;
  }

  // False in odd positions: True, False, True, False,... .
  auto oddFalse =
      makeFlatVector<bool>(size, [](auto row) { return row % 2 == 0; });
  result = evaluate<SimpleVector<bool>>("not(c0)", makeRowVector({oddFalse}));
  for (int i = 0; i < size; ++i) {
    EXPECT_EQ(result->valueAt(i), i % 2 == 1) << "at " << i;
  }
}

TEST_F(NotTest, someNulls) {
  constexpr vector_size_t size = 1'000;

  // False in even positions: False, True, False, True, .....; with some nulls.
  auto evenFalse = makeFlatVector<bool>(
      size, [](auto row) { return row % 2 == 1; }, nullEvery(7));
  auto result =
      evaluate<SimpleVector<bool>>("not(c0)", makeRowVector({evenFalse}));

  auto expectedResult = makeFlatVector<bool>(
      size, [](auto row) { return row % 2 == 0; }, nullEvery(7));
  assertEqualVectors(expectedResult, result);
}
