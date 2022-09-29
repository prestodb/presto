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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {

class ArrayJoinTest : public FunctionBaseTest {
 protected:
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result =
        evaluate<SimpleVector<StringView>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  template <typename T>
  void testArrayJoinNoReplacement(
      std::vector<std::optional<T>> array,
      const StringView& delimiter,
      const StringView& expected) {
    auto arrayVector = makeNullableArrayVector(
        std::vector<std::vector<std::optional<T>>>{array});
    auto delimiterVector = makeFlatVector<StringView>({delimiter});
    auto expectedVector = makeFlatVector<StringView>({expected});
    testExpr(
        expectedVector, "array_join(c0, c1)", {arrayVector, delimiterVector});
  }

  template <typename T>
  void testArrayJoinReplacement(
      std::vector<std::optional<T>> array,
      const StringView& delimiter,
      const StringView& replacement,
      const StringView& expected) {
    auto arrayVector = makeNullableArrayVector(
        std::vector<std::vector<std::optional<T>>>{array});
    auto delimiterVector = makeFlatVector<StringView>({delimiter});
    auto replacementVector = makeFlatVector<StringView>({replacement});
    auto expectedVector = makeFlatVector<StringView>({expected});
    testExpr(
        expectedVector,
        "array_join(c0, c1, c2)",
        {arrayVector, delimiterVector, replacementVector});
  }
};

TEST_F(ArrayJoinTest, intTest) {
  testArrayJoinNoReplacement<int64_t>(
      {1, 2, std::nullopt, 3}, ","_sv, "1,2,3"_sv);
  testArrayJoinNoReplacement<int32_t>(
      {1, 2, std::nullopt, 3}, ","_sv, "1,2,3"_sv);
  testArrayJoinNoReplacement<int16_t>(
      {1, 2, std::nullopt, 3}, ","_sv, "1,2,3"_sv);
  testArrayJoinNoReplacement<int8_t>(
      {1, 2, std::nullopt, 3}, ","_sv, "1,2,3"_sv);
  // Test single element.
  testArrayJoinNoReplacement<int8_t>({1}, ","_sv, "1"_sv);
  // Test empty array.
  testArrayJoinNoReplacement<int8_t>({}, ","_sv, ""_sv);
  testArrayJoinNoReplacement<int8_t>({std::nullopt}, ","_sv, ""_sv);

  testArrayJoinReplacement<int64_t>(
      {1, 2, std::nullopt, 3}, ","_sv, "0"_sv, "1,2,0,3"_sv);
}

TEST_F(ArrayJoinTest, varcharTest) {
  testArrayJoinNoReplacement<StringView>(
      {"a"_sv, "b"_sv, std::nullopt, "c"_sv}, "-"_sv, "a-b-c"_sv);
  testArrayJoinNoReplacement<StringView>({}, "-"_sv, ""_sv);

  testArrayJoinReplacement<StringView>(
      {"a"_sv, "b"_sv, std::nullopt, "c"_sv}, "-"_sv, "z"_sv, "a-b-z-c"_sv);
}

TEST_F(ArrayJoinTest, boolTest) {
  testArrayJoinNoReplacement<bool>(
      {true, std::nullopt, false}, ","_sv, "true,false"_sv);

  testArrayJoinReplacement<bool>(
      {true, std::nullopt, false}, ","_sv, "apple"_sv, "true,apple,false"_sv);
}

} // namespace
