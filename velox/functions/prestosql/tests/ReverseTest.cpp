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
using namespace facebook::velox::functions::test;

namespace {
class ReverseTest : public FunctionBaseTest {
 protected:
  template <typename T>
  void testExpr(const VectorPtr& expected, const VectorPtr& input) {
    auto result = evaluate<T>("reverse(C0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }
};

TEST_F(ReverseTest, intsArrays) {
  auto array1 = makeNullableArrayVector<int64_t>(
      {{1, -2, 3, std::nullopt, 4, 5, 6, std::nullopt},
       {1, 2, -2, 1},
       {3, 8, std::nullopt},
       {1, 8},
       {}});

  auto expected = makeNullableArrayVector<int64_t>(
      {{std::nullopt, 6, 5, 4, std::nullopt, 3, -2, 1},
       {1, -2, 2, 1},
       {std::nullopt, 8, 3},
       {8, 1},
       {}});
  testExpr<ArrayVector>(expected, array1);
}

TEST_F(ReverseTest, doublesArrays) {
  auto input = makeNullableArrayVector<double>({
      {1, 2, -2, 1},
      {3, 8, std::nullopt},
      {1, 8},
  });

  auto expected = makeNullableArrayVector<double>({
      {1, -2, 2, 1},
      {std::nullopt, 8, 3},
      {8, 1},
  });
  testExpr<ArrayVector>(expected, input);
}

TEST_F(ReverseTest, stringsArrays) {
  using S = StringView;
  auto input = makeNullableArrayVector<StringView>(
      {{S("abcdefghijklmopqrstuv"), S("b"), S("c")},
       {S("mnoasda asasd aqqerewqe"), S("xyz"), std::nullopt},
       {}});

  auto expected = makeNullableArrayVector<StringView>(
      {{S("c"), S("b"), S("abcdefghijklmopqrstuv")},
       {std::nullopt, S("xyz"), S("mnoasda asasd aqqerewqe")},
       {}});

  testExpr<ArrayVector>(expected, input);
}

TEST_F(ReverseTest, constant) {
  auto input = makeNullableArrayVector<int32_t>({
      {1, 2, 3},
  });

  auto constant = BaseVector::wrapInConstant(2, 0, input);

  auto expected = makeNullableArrayVector<int32_t>({{3, 2, 1}, {3, 2, 1}});

  testExpr<SimpleVector<ComplexType>>(expected, constant);
}

TEST_F(ReverseTest, nestedArray) {
  auto createArrayOfArrays =
      [&](std::vector<std::optional<std::vector<std::optional<int32_t>>>>
              data) {
        auto baseArray = makeVectorWithNullArrays<int32_t>(data);

        vector_size_t size = data.size() / 2;
        BufferPtr offsets =
            AlignedBuffer::allocate<vector_size_t>(size, pool());
        BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(size, pool());
        BufferPtr nulls = AlignedBuffer::allocate<uint64_t>(size, pool());

        auto rawOffsets = offsets->asMutable<vector_size_t>();
        auto rawSizes = sizes->asMutable<vector_size_t>();
        auto rawNulls = nulls->asMutable<uint64_t>();
        bits::fillBits(rawNulls, 0, size, pool());

        for (int i = 0; i < size; i++) {
          rawOffsets[i] = 2 * i;
          rawSizes[i] = 2;
        }

        return std::make_shared<ArrayVector>(
            pool(),
            ARRAY(ARRAY(INTEGER())),
            BufferPtr(nullptr),
            size,
            offsets,
            sizes,
            baseArray,
            0);
      };

  auto O = [](std::vector<std::optional<int32_t>> data) {
    return std::make_optional(data);
  };
  auto arrayOfArrays = createArrayOfArrays(
      {O({1, 2, 3}),
       O({4, 5, 6}),
       O({7, 8, 9}),
       O({10, 11, 12}),
       O({1, 2}),
       O({4, 3})});
  auto expected = createArrayOfArrays(
      {O({4, 5, 6}),
       O({1, 2, 3}),
       O({10, 11, 12}),
       O({7, 8, 9}),
       O({4, 3}),
       O({1, 2})});

  testExpr<ArrayVector>(expected, arrayOfArrays);

  arrayOfArrays =
      createArrayOfArrays({O({1, 2, 3, 4}), O({4, 5, std::nullopt})});
  expected = createArrayOfArrays({O({4, 5, std::nullopt}), O({1, 2, 3, 4})});

  testExpr<ArrayVector>(expected, arrayOfArrays);

  arrayOfArrays = createArrayOfArrays({std::nullopt, O({4, 5, std::nullopt})});
  expected = createArrayOfArrays({O({4, 5, std::nullopt}), std::nullopt});

  testExpr<ArrayVector>(expected, arrayOfArrays);
}

TEST_F(ReverseTest, nullArray) {
  auto vecWithNull = std::make_optional<std::vector<std::optional<int32_t>>>(
      {1, 2, std::nullopt});
  auto reverseNullVec = std::make_optional<std::vector<std::optional<int32_t>>>(
      {std::nullopt, 2, 1});
  auto nullArray =
      makeVectorWithNullArrays<int32_t>({vecWithNull, std::nullopt});
  auto reverseNullArray =
      makeVectorWithNullArrays<int32_t>({reverseNullVec, std::nullopt});

  testExpr<ArrayVector>(reverseNullArray, nullArray);
}
} // namespace
