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

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {
class ArrayRemoveNullsTest : public FunctionBaseTest {
 protected:
  void testArrayRemoveNull(const VectorPtr& expected, const VectorPtr& input) {
    auto result = evaluate("remove_nulls(c0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }
};

TEST_F(ArrayRemoveNullsTest, simpleString) {
  auto input = makeNullableArrayVector<std::string>(
      {{"foo", std::nullopt, "bar"}, {"foo", "bar"}});
  auto expected =
      makeNullableArrayVector<std::string>({{"foo", "bar"}, {"foo", "bar"}});
  testArrayRemoveNull(expected, input);
}

TEST_F(ArrayRemoveNullsTest, simpleInt) {
  auto input = makeNullableArrayVector<int>(
      {{1, std::nullopt, std::nullopt, 3}, {1, 3}});
  auto expected = makeNullableArrayVector<int>({{1, 3}, {1, 3}});
  testArrayRemoveNull(expected, input);
}

TEST_F(ArrayRemoveNullsTest, simpleBool) {
  auto input = makeNullableArrayVector<bool>(
      {{true, false, true}, {true, false, std::nullopt}});
  auto expected =
      makeNullableArrayVector<bool>({{true, false, true}, {true, false}});
  testArrayRemoveNull(expected, input);
}

TEST_F(ArrayRemoveNullsTest, complexType) {
  using array_type = std::optional<std::vector<std::optional<int32_t>>>;
  array_type array1 = {{1, 2}};
  array_type array2 = std::nullopt;
  array_type array3 = {{1, std::nullopt, 2}};

  auto input =
      makeNullableNestedArrayVector<int32_t>({{{array1, array2, array3}}});

  array_type earray1 = {{1, 2}};
  array_type earray2 = {{1, std::nullopt, 2}};
  auto expected =
      makeNullableNestedArrayVector<int32_t>({{{earray1, earray2}}});
  testArrayRemoveNull(expected, input);
}

} // namespace
