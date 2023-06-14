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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions::test {
namespace {
class ArrayTrimTest : public FunctionBaseTest {};

TEST_F(ArrayTrimTest, bigintArrays) {
  auto test = [this](
                  const std::vector<int64_t>& inputArray,
                  int size,
                  const std::vector<int64_t>& expectedOutput) {
    auto input = makeArrayVector<int64_t>({inputArray});
    auto result = evaluate(
        fmt::format("trim_array(c0, {})", size), makeRowVector({input}));
    auto expected = makeArrayVector<int64_t>({expectedOutput});
    ::facebook::velox::test::assertEqualVectors(expected, result);
  };

  test({1, 2, 3, 4}, 0, {1, 2, 3, 4});
  test({1, 2, 3, 4}, 1, {1, 2, 3});
  test({1, 2, 3, 4}, 2, {1, 2});
  test({1, 2, 3, 4}, 3, {1});
  test({1, 2, 3, 4}, 4, {});

  auto input = makeNullableArrayVector<int64_t>({{1, 2, 3, 4}});
  VELOX_ASSERT_THROW(
      evaluate("trim_array(c0, 5)", makeRowVector({input})),
      "size must not exceed array cardinality. arraySize: 4, size: 5");

  VELOX_ASSERT_THROW(
      evaluate("trim_array(c0, -1)", makeRowVector({input})),
      "size must not be negative: -1");
}

TEST_F(ArrayTrimTest, varcharArraysWithNull) {
  auto input = makeNullableArrayVector<std::string>(
      {{"aa", "bb", "dd"}, {"ad", std::nullopt, std::nullopt, "de"}});
  auto result = evaluate("trim_array(c0, 2)", makeRowVector({input}));
  auto expected =
      makeNullableArrayVector<std::string>({{"aa"}, {"ad", std::nullopt}});
  ::facebook::velox::test::assertEqualVectors(expected, result);
}
} // namespace
} // namespace facebook::velox::functions::test
