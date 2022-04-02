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
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class ZipTest : public FunctionBaseTest {};

/// Test if we can zip two integer arrays.
TEST_F(ZipTest, simpleInt) {
  auto firstVector = makeNullableArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {std::nullopt}});
  auto secondVector =
      makeArrayVector<int64_t>({{2, 2, 2}, {1, 1, 1}, {3, 3, 3}});

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          firstVector,
          secondVector,
      }));

  auto firstResult = makeNullableFlatVector<int64_t>(
      {1, 2, 3, 4, 3, 4, 5, std::nullopt, std::nullopt, std::nullopt});
  auto secondResult = makeNullableFlatVector<int64_t>(
      {2, 2, 2, std::nullopt, 1, 1, 1, 3, 3, 3});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 4, 7}, rowVector);

  assertEqualVectors(expected, result);
}

/// Test if we can zip across different data types.
TEST_F(ZipTest, combineInt) {
  using S = StringView;
  auto firstVector = makeArrayVector<int64_t>({{1, 1, 1, 1}, {2, 2, 2}, {}});
  auto secondVector = makeArrayVector<StringView>(
      {{S("a"), S("a"), S("a")}, {S("b"), S("b"), S("b")}, {S("c"), S("c")}});

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          firstVector,
          secondVector,
      }));

  auto firstResult = makeNullableFlatVector<int64_t>(
      {1, 1, 1, 1, 2, 2, 2, std::nullopt, std::nullopt});
  auto secondResult = makeNullableFlatVector<std::string>(
      {S("a"),
       S("a"),
       S("a"),
       std::nullopt,
       S("b"),
       S("b"),
       S("b"),
       S("c"),
       S("c")});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 4, 7}, rowVector);

  assertEqualVectors(expected, result);
}

/// Test if we can zip with vectors containing null and empty Arrays
TEST_F(ZipTest, nullEmptyArray) {
  auto O = [](std::vector<std::optional<int32_t>> data) {
    return std::make_optional(data);
  };

  auto firstVector =
      makeVectorWithNullArrays<int32_t>({O({1, 1, 1, 1}), O({}), std::nullopt});

  auto secondVector = makeArrayVector<int32_t>({{0, 0, 0}, {4, 4}, {5, 5}});

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          firstVector,
          secondVector,
      }));

  auto firstResult =
      makeNullableFlatVector<int32_t>({1, 1, 1, 1, std::nullopt, std::nullopt});
  auto secondResult =
      makeNullableFlatVector<int32_t>({0, 0, 0, std::nullopt, 4, 4});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 4, 6}, rowVector, {2});

  assertEqualVectors(expected, result);
}

/// Test if we can pass multiple different array types in one function.
TEST_F(ZipTest, arity) {
  using S = StringView;
  auto firstVector =
      makeArrayVector<int16_t>({{1, 1, 1, 1}, {2, 2, 2}, {3, 3}});

  auto secondVector =
      makeArrayVector<int32_t>({{0, 0, 0, 0}, {4, 4, 4}, {5, 5}});

  auto thirdVector =
      makeArrayVector<int64_t>({{1, 1, 1, 1}, {2, 2, 2}, {3, 3}});

  auto fourthVector = makeArrayVector<StringView>(
      {{S("a"), S("a"), S("a"), S("a")},
       {S("b"), S("b"), S("b")},
       {S("c"), S("c")}});

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1, c2, c3)",
      makeRowVector({firstVector, secondVector, thirdVector, fourthVector}));

  auto firstResult =
      makeNullableFlatVector<int16_t>({1, 1, 1, 1, 2, 2, 2, 3, 3});
  auto secondResult =
      makeNullableFlatVector<int32_t>({0, 0, 0, 0, 4, 4, 4, 5, 5});
  auto thirdResult =
      makeNullableFlatVector<int64_t>({1, 1, 1, 1, 2, 2, 2, 3, 3});
  auto fourthResult = makeNullableFlatVector<std::string>(
      {S("a"), S("a"), S("a"), S("a"), S("b"), S("b"), S("b"), S("c"), S("c")});
  auto rowVector =
      makeRowVector({firstResult, secondResult, thirdResult, fourthResult});
  auto expected = makeArrayVector({0, 4, 7}, rowVector);
  assertEqualVectors(expected, result);
}

/// Test if we can zip on complex types
TEST_F(ZipTest, complexTypes) {
  auto baseVector = makeArrayVector<int64_t>(
      {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}});

  // Create an array of array vector using above base vector
  auto arrayOfArrays = makeArrayVector({0, 2, 4}, baseVector);

  auto secondVector = makeArrayVector<int32_t>({{0, 0}, {4, 4}, {5, 5}});

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          arrayOfArrays,
          secondVector,
      }));

  auto secondResult = makeNullableFlatVector<int32_t>({0, 0, 4, 4, 5, 5});
  auto rowVector = makeRowVector({baseVector, secondResult});
  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 2, 4}, rowVector);

  assertEqualVectors(expected, result);
}

} // namespace
