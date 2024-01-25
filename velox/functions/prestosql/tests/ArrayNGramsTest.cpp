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
#include <optional>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions {
namespace {
class ArrayNGramsTest : public test::FunctionBaseTest {
 protected:
  template <typename T>
  void testNgram(
      const std::vector<std::optional<T>>& inputArray,
      int32_t n,
      const std::vector<std::optional<std::vector<std::optional<T>>>>&
          expectedOutput) {
    std::vector<std::optional<std::vector<std::optional<T>>>> inputVec(
        {inputArray});
    auto input = makeNullableArrayVector<T>(inputVec);
    auto result = evaluate(
        fmt::format("ngrams(c0, {}::INTEGER)", n), makeRowVector({input}));

    auto expected = makeNullableNestedArrayVector<T>({expectedOutput});
    assertEqualVectors(expected, result);
  }
};

TEST_F(ArrayNGramsTest, integers) {
  testNgram<int64_t>({1, 2, 3, 4}, 1, {{{1}}, {{2}}, {{3}}, {{4}}});
  testNgram<int64_t>({1, 2, 3, 4}, 2, {{{1, 2}}, {{2, 3}}, {{3, 4}}});
  testNgram<int64_t>({1, 2, 3, 4}, 3, {{{1, 2, 3}}, {{2, 3, 4}}});
  testNgram<int64_t>({1, 2, 3, 4}, 4, {{{1, 2, 3, 4}}});
  testNgram<int64_t>({1, 2, 3, 4}, 5, {{{1, 2, 3, 4}}});
  testNgram<int64_t>(
      {1, 2, 3, 4}, std::numeric_limits<int32_t>::max(), {{{1, 2, 3, 4}}});
  testNgram<int64_t>({}, 1, {{{}}});
  testNgram<int64_t>({}, 10, {{{}}});
}

TEST_F(ArrayNGramsTest, invalidN) {
  auto input = makeArrayVector<int64_t>({{1, 2, 3, 4}});
  VELOX_ASSERT_THROW(
      evaluate("ngrams(c0, 0::INTEGER)", makeRowVector({input})),
      "(0 vs. 0) N must be greater than zero");
  VELOX_ASSERT_THROW(
      evaluate("ngrams(c0, -5::INTEGER)", makeRowVector({input})),
      "Scalar function signature is not supported");
  input = makeArrayVector<int64_t>({{}});
  VELOX_ASSERT_THROW(
      evaluate("ngrams(c0, 0::INTEGER)", makeRowVector({input})),
      "(0 vs. 0) N must be greater than zero");
}

TEST_F(ArrayNGramsTest, strings) {
  testNgram<std::string>(
      {"foo", "bar", "baz", "this is a very long sentence"},
      1,
      {{{"foo"}}, {{"bar"}}, {{"baz"}}, {{"this is a very long sentence"}}});
  testNgram<std::string>(
      {"foo", "bar", "baz", "this is a very long sentence"},
      2,
      {{{"foo", "bar"}},
       {{"bar", "baz"}},
       {{"baz", "this is a very long sentence"}}});
  testNgram<std::string>(
      {"foo", "bar", "baz", "this is a very long sentence"},
      3,
      {{{"foo", "bar", "baz"}},
       {{"bar", "baz", "this is a very long sentence"}}});
  testNgram<std::string>(
      {"foo", "bar", "baz", "this is a very long sentence"},
      4,
      {{{"foo", "bar", "baz", "this is a very long sentence"}}});
  testNgram<std::string>(
      {"foo", "bar", "baz", "this is a very long sentence"},
      5,
      {{{"foo", "bar", "baz", "this is a very long sentence"}}});
}

TEST_F(ArrayNGramsTest, nulls) {
  testNgram<std::string>(
      {"foo", std::nullopt, "bar"},
      2,
      {{{"foo", std::nullopt}}, {{std::nullopt, "bar"}}});
  testNgram<std::string>(
      {std::nullopt, std::nullopt, std::nullopt},
      2,
      {{{std::nullopt, std::nullopt}}, {{std::nullopt, std::nullopt}}});
}
} // namespace

} // namespace facebook::velox::functions
