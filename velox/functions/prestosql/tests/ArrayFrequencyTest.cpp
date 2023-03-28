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

using facebook::velox::test::assertEqualVectors;

namespace facebook::velox::functions::test {

namespace {

class ArrayFrequencyTest : public functions::test::FunctionBaseTest {
 protected:
  void testArrayFrequency(const VectorPtr& expected, const VectorPtr& input) {
    auto result =
        evaluate<BaseVector>("array_frequency(C0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }
};

} // namespace

TEST_F(ArrayFrequencyTest, integerArray) {
  auto array = makeNullableArrayVector<int64_t>(
      {{2, 1, 1, -2},
       {},
       {1, 2, 1, 1, 1, 1},
       {-1, std::nullopt, -1, -1},
       {std::numeric_limits<int64_t>::max(),
        std::numeric_limits<int64_t>::max(),
        1,
        std::nullopt,
        0,
        1,
        std::nullopt,
        0}});

  auto expected = makeMapVector<int64_t, int>(
      {{{1, 2}, {2, 1}, {-2, 1}},
       {},
       {{1, 5}, {2, 1}},
       {{-1, 3}},
       {{std::numeric_limits<int64_t>::max(), 2}, {1, 2}, {0, 2}}});

  testArrayFrequency(expected, array);
}

TEST_F(ArrayFrequencyTest, integerArrayWithoutNull) {
  auto array =
      makeArrayVector<int64_t>({{2, 1, 1, -2}, {}, {1, 2, 1, 1, 1, 1}});

  auto expected = makeMapVector<int64_t, int>(
      {{{1, 2}, {2, 1}, {-2, 1}}, {}, {{1, 5}, {2, 1}}});

  testArrayFrequency(expected, array);
}

TEST_F(ArrayFrequencyTest, varcharArray) {
  auto array = makeNullableArrayVector<StringView>({
      {"hello"_sv, "world"_sv, "!"_sv, "!"_sv, "!"_sv},
      {},
      {"hello"_sv, "world"_sv, std::nullopt, "!"_sv, "!"_sv},
      {"helloworldhelloworld"_sv,
       "helloworldhelloworld"_sv,
       std::nullopt,
       "!"_sv,
       "!"_sv},
  });

  auto expected = makeMapVector<StringView, int>(
      {{{"hello"_sv, 1}, {"world"_sv, 1}, {"!"_sv, 3}},
       {},
       {{"hello"_sv, 1}, {"world"_sv, 1}, {"!"_sv, 2}},
       {{"helloworldhelloworld"_sv, 2}, {"!"_sv, 2}}});

  testArrayFrequency(expected, array);
}

TEST_F(ArrayFrequencyTest, varcharArrayWithoutNull) {
  auto array = makeNullableArrayVector<StringView>({
      {"hello"_sv, "world"_sv, "!"_sv, "!"_sv, "!"_sv},
      {},
      {"helloworldhelloworld"_sv, "helloworldhelloworld"_sv, "!"_sv, "!"_sv},
  });
  auto expected = makeMapVector<StringView, int>(
      {{{"hello"_sv, 1}, {"world"_sv, 1}, {"!"_sv, 3}},
       {},
       {{"helloworldhelloworld"_sv, 2}, {"!"_sv, 2}}});

  testArrayFrequency(expected, array);
}

} // namespace facebook::velox::functions::test
