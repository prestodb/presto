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

#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"

namespace facebook::velox::functions {

void registerIsNotNull() {
  parse::registerTypeResolver();
  VELOX_REGISTER_VECTOR_FUNCTION(udf_is_not_null, "isnotnull");
}

}; // namespace facebook::velox::functions

using namespace facebook::velox;
using namespace facebook::velox::test;

class IsNotNullTest : public functions::test::FunctionBaseTest {
 public:
  static void SetUpTestCase() {
    functions::registerIsNotNull();
  }

  template <typename T>
  std::optional<bool> isnotnull(std::optional<T> arg) {
    return evaluateOnce<bool>("isnotnull(c0)", arg);
  }

  SimpleVectorPtr<bool> isnotnull(const VectorPtr& arg) {
    return evaluate<SimpleVector<bool>>("isnotnull(c0)", makeRowVector({arg}));
  }
};

TEST_F(IsNotNullTest, singleValues) {
  EXPECT_FALSE(isnotnull<int32_t>(std::nullopt).value());
  EXPECT_FALSE(isnotnull<double>(std::nullopt).value());
  EXPECT_TRUE(isnotnull<int64_t>(1).value());
  EXPECT_TRUE(isnotnull<float>(1.5).value());
  EXPECT_TRUE(isnotnull<std::string>("1").value());
  EXPECT_TRUE(isnotnull<double>(std::numeric_limits<double>::max()).value());
}

// The rest of the file is copied and modified from IsNullTest.cpp

TEST_F(IsNotNullTest, flatInput) {
  vector_size_t size = 20;

  // All nulls.
  auto allNulls = makeFlatVector<int32_t>(
      size, [](auto /*row*/) { return 0; }, nullEvery(1));
  auto result = isnotnull(allNulls);
  assertEqualVectors(makeConstant(false, size), result);

  // Nulls in odd positions: 0, null, 2, null,..
  auto oddNulls = makeFlatVector<int32_t>(
      size, [](auto row) { return row; }, nullEvery(2, 1));
  result = isnotnull(oddNulls);
  for (int i = 0; i < size; ++i) {
    EXPECT_EQ(result->valueAt(i), i % 2 == 0) << "at " << i;
  }

  // No nulls.
  auto noNulls = makeFlatVector<int32_t>(size, [](auto row) { return row; });
  result = isnotnull(noNulls);
  assertEqualVectors(makeConstant(true, size), result);
}

TEST_F(IsNotNullTest, constantInput) {
  vector_size_t size = 1'000;

  // Non-null constant.
  auto data = makeConstant<int32_t>(75, size);
  auto result = isnotnull(data);

  assertEqualVectors(makeConstant<bool>(true, size), result);

  // Null constant.
  data = makeConstant<int32_t>(std::nullopt, size);
  result = isnotnull(data);

  assertEqualVectors(makeConstant<bool>(false, 1'000), result);
}

TEST_F(IsNotNullTest, dictionary) {
  vector_size_t size = 1'000;

  // Dictionary over flat, no nulls.
  auto flatNoNulls = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto dict = wrapInDictionary(
      makeIndices(size, [](auto row) { return row % 4; }), size, flatNoNulls);

  auto result = isnotnull(dict);

  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(result->valueAt(i), !dict->isNullAt(i)) << "at " << i;
  }

  // Dictionary with nulls over no-nulls flat vector.
  dict = BaseVector::wrapInDictionary(
      makeNulls(size, nullEvery(5)),
      makeIndices(size, [](auto row) { return row % 4; }),
      size,
      flatNoNulls);

  result = isnotnull(dict);

  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(result->valueAt(i), !dict->isNullAt(i)) << "at " << i;
  }

  // Dictionary over flat vector with nulls.
  auto flatWithNulls = makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 4});
  dict = wrapInDictionary(
      makeIndices(size, [](auto row) { return row % 4; }), size, flatWithNulls);

  result = isnotnull(dict);

  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(result->valueAt(i), !dict->isNullAt(i)) << "at " << i;
  }

  // Dictionary with nulls over flat vector with nulls.
  dict = BaseVector::wrapInDictionary(
      makeNulls(size, nullEvery(5)),
      makeIndices(size, [](auto row) { return row % 4; }),
      size,
      flatWithNulls);

  result = isnotnull(dict);

  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(result->valueAt(i), !dict->isNullAt(i)) << "at " << i;
  }

  // Dictionary with nulls over constant.
  dict = BaseVector::wrapInDictionary(
      makeNulls(size, nullEvery(5)),
      makeIndices(size, [](auto row) { return 2; }),
      size,
      makeConstant<int32_t>(75, 10));

  result = isnotnull(dict);

  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(result->valueAt(i), !dict->isNullAt(i)) << "at " << i;
  }
}
