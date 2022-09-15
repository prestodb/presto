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
using namespace facebook::velox::test;

class IsNullTest : public functions::test::FunctionBaseTest {};

TEST_F(IsNullTest, basic) {
  vector_size_t size = 20;

  // All nulls.
  auto allNulls = makeFlatVector<int32_t>(
      size, [](vector_size_t /*row*/) { return 0; }, nullEvery(1));
  auto result =
      evaluate<SimpleVector<bool>>("is_null(c0)", makeRowVector({allNulls}));
  for (int i = 0; i < size; ++i) {
    EXPECT_TRUE(result->valueAt(i)) << "at " << i;
  }

  // Nulls in odd positions: 0, null, 2, null,..
  auto oddNulls = makeFlatVector<int32_t>(
      size, [](vector_size_t row) { return row; }, nullEvery(2, 1));
  result =
      evaluate<SimpleVector<bool>>("is_null(c0)", makeRowVector({oddNulls}));
  for (int i = 0; i < size; ++i) {
    EXPECT_EQ(result->valueAt(i), i % 2 == 1) << "at " << i;
  }

  // No nulls.
  auto noNulls =
      makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; });
  result =
      evaluate<SimpleVector<bool>>("is_null(c0)", makeRowVector({noNulls}));
  for (int i = 0; i < size; ++i) {
    EXPECT_FALSE(result->valueAt(i)) << "at " << i;
  }
}

TEST_F(IsNullTest, constant) {
  // Non-null constant.
  auto data = makeConstant<int32_t>(75, 1'000);
  auto result =
      evaluate<SimpleVector<bool>>("is_null(c0)", makeRowVector({data}));

  assertEqualVectors(makeConstant<bool>(false, 1'000), result);

  // Null constant.
  data = makeConstant<int32_t>(std::nullopt, 1'000);
  result = evaluate<SimpleVector<bool>>("is_null(c0)", makeRowVector({data}));

  assertEqualVectors(makeConstant<bool>(true, 1'000), result);
}

TEST_F(IsNullTest, dictionary) {
  vector_size_t size = 1'000;

  // Dictionary over flat, no nulls.
  auto flatNoNulls = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto dict = wrapInDictionary(
      makeIndices(size, [](auto row) { return row % 4; }), size, flatNoNulls);

  auto result =
      evaluate<SimpleVector<bool>>("is_null(c0)", makeRowVector({dict}));

  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(result->valueAt(i), dict->isNullAt(i)) << "at " << i;
  }

  // Dictionary with nulls over no-nulls flat vector.
  dict = BaseVector::wrapInDictionary(
      makeNulls(size, nullEvery(5)),
      makeIndices(size, [](auto row) { return row % 4; }),
      size,
      flatNoNulls);

  result = evaluate<SimpleVector<bool>>("is_null(c0)", makeRowVector({dict}));

  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(result->valueAt(i), dict->isNullAt(i)) << "at " << i;
  }

  // Dictionary over flat vector with nulls.
  auto flatWithNulls = makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 4});
  dict = wrapInDictionary(
      makeIndices(size, [](auto row) { return row % 4; }), size, flatWithNulls);

  result = evaluate<SimpleVector<bool>>("is_null(c0)", makeRowVector({dict}));

  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(result->valueAt(i), dict->isNullAt(i)) << "at " << i;
  }

  // Dictionary with nulls over flat vector with nulls.
  dict = BaseVector::wrapInDictionary(
      makeNulls(size, nullEvery(5)),
      makeIndices(size, [](auto row) { return row % 4; }),
      size,
      flatWithNulls);

  result = evaluate<SimpleVector<bool>>("is_null(c0)", makeRowVector({dict}));

  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(result->valueAt(i), dict->isNullAt(i)) << "at " << i;
  }

  // Dictionary with nulls over constant.
  dict = BaseVector::wrapInDictionary(
      makeNulls(size, nullEvery(5)),
      makeIndices(size, [](auto row) { return 2; }),
      size,
      makeConstant<int32_t>(75, 10));

  result = evaluate<SimpleVector<bool>>("is_null(c0)", makeRowVector({dict}));

  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(result->valueAt(i), dict->isNullAt(i)) << "at " << i;
  }
}
