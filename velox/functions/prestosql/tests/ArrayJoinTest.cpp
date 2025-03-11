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
#include "velox/functions/prestosql/types/JsonType.h"

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
      const StringView& expected,
      bool isDate = false,
      bool isJson = false) {
    auto arrayVector = makeNullableArrayVector(
        std::vector<std::vector<std::optional<T>>>{array});
    if (isDate) {
      arrayVector = makeNullableArrayVector(
          std::vector<std::vector<std::optional<T>>>{array}, ARRAY(DATE()));
    } else if (isJson) {
      arrayVector = makeNullableArrayVector(
          std::vector<std::vector<std::optional<T>>>{array}, ARRAY(JSON()));
    }
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
      const StringView& expected,
      bool isDate = false,
      bool isJson = false) {
    auto arrayVector = makeNullableArrayVector(
        std::vector<std::vector<std::optional<T>>>{array});
    if (isDate) {
      arrayVector = makeNullableArrayVector(
          std::vector<std::vector<std::optional<T>>>{array}, ARRAY(DATE()));
    } else if (isJson) {
      arrayVector = makeNullableArrayVector(
          std::vector<std::vector<std::optional<T>>>{array}, ARRAY(JSON()));
    }
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

TEST_F(ArrayJoinTest, timestampTest) {
  setLegacyCast(false);
  testArrayJoinNoReplacement<Timestamp>(
      {Timestamp{333183, 0}, std::nullopt, Timestamp{2925183, 0}},
      "~"_sv,
      "1970-01-04 20:33:03.000~1970-02-03 20:33:03.000"_sv);
  testArrayJoinReplacement<Timestamp>(
      {Timestamp{333183, 0}, std::nullopt, Timestamp{2925183, 0}},
      "~"_sv,
      "<n/a>"_sv,
      "1970-01-04 20:33:03.000~<n/a>~1970-02-03 20:33:03.000"_sv);

  setLegacyCast(true);
  testArrayJoinNoReplacement<Timestamp>(
      {Timestamp{333183, 0}, std::nullopt, Timestamp{2925183, 0}},
      "~"_sv,
      "1970-01-04T20:33:03.000~1970-02-03T20:33:03.000"_sv);
  testArrayJoinReplacement<Timestamp>(
      {Timestamp{333183, 0}, std::nullopt, Timestamp{2925183, 0}},
      "~"_sv,
      "<missing>"_sv,
      "1970-01-04T20:33:03.000~<missing>~1970-02-03T20:33:03.000"_sv);

  setLegacyCast(false);
  setTimezone("America/Los_Angeles");
  testArrayJoinNoReplacement<Timestamp>(
      {Timestamp{333183, 0}, std::nullopt, Timestamp{2925183, 0}},
      "~"_sv,
      "1970-01-04 12:33:03.000~1970-02-03 12:33:03.000"_sv);
  testArrayJoinReplacement<Timestamp>(
      {Timestamp{333183, 0}, std::nullopt, Timestamp{2925183, 0}},
      "~"_sv,
      "<absent>"_sv,
      "1970-01-04 12:33:03.000~<absent>~1970-02-03 12:33:03.000"_sv);
}

TEST_F(ArrayJoinTest, dateTest) {
  std::cout << std::nextafter(0.67777777, INFINITY);
  setLegacyCast(false);
  testArrayJoinNoReplacement<int32_t>(
      {-7204, std::nullopt, -7203}, ","_sv, "1950-04-12,1950-04-13"_sv, true);
  testArrayJoinReplacement<int32_t>(
      {-7204, std::nullopt, -7203},
      ","_sv,
      "."_sv,
      "1950-04-12,.,1950-04-13"_sv,
      true);
}

TEST_F(ArrayJoinTest, jsonTest) {
  setLegacyCast(false);
  std::vector<std::optional<StringView>> input{
      R"({"one":1,"two":2})",
      std::nullopt,
      R"(secondElement)",
  };
  testArrayJoinNoReplacement<StringView>(
      input, ", ", R"({"one":1,"two":2}, secondElement)", false, true);
  testArrayJoinReplacement<StringView>(
      input, ", ", "0", R"({"one":1,"two":2}, 0, secondElement)", false, true);

  input = {R"("one element")"};
  testArrayJoinNoReplacement<StringView>(
      input, ", ", "one element", false, true);
  testArrayJoinReplacement<StringView>(
      input, ", ", "0", "one element", false, true);

  input = {std::nullopt, std::nullopt};
  testArrayJoinNoReplacement<StringView>(input, ", ", "", false, true);
  testArrayJoinReplacement<StringView>(input, ", ", "0", "0, 0", false, true);

  // JSON strings with special characters
  input = {
      R"({"key": "value\with\backslash"})",
      std::nullopt,
      R"('value\with\backslash')",
  };
  testArrayJoinNoReplacement<StringView>(
      input,
      ", ",
      R"({"key": "value\with\backslash"}, 'value\with\backslash')",
      false,
      true);
  testArrayJoinReplacement<StringView>(
      input,
      ", ",
      "0",
      R"({"key": "value\with\backslash"}, 0, 'value\with\backslash')",
      false,
      true);

  input = {
      R"({"key": "value\nwith\nnewline"})",
      std::nullopt,
      R"("value\nwith\nnewline")",
  };
  testArrayJoinNoReplacement<StringView>(
      input,
      ", ",
      R"({"key": "value\nwith\nnewline"}, value\nwith\nnewline)",
      false,
      true);
  testArrayJoinReplacement<StringView>(
      input,
      ", ",
      "0",
      R"({"key": "value\nwith\nnewline"}, 0, value\nwith\nnewline)",
      false,
      true);

  input = {
      R"({"key": "value with \u00A9 and \u20AC"})",
      std::nullopt,
      R"("value with \u00A9 and \u20AC")",
  };
  testArrayJoinNoReplacement<StringView>(
      input,
      ", ",
      R"({"key": "value with \u00A9 and \u20AC"}, value with \u00A9 and \u20AC)",
      false,
      true);
  testArrayJoinReplacement<StringView>(
      input,
      ", ",
      "0",
      R"({"key": "value with \u00A9 and \u20AC"}, 0, value with \u00A9 and \u20AC)",
      false,
      true);

  input = {
      R"({"key": "!@#$%^&*()_+-={}:<>?,./~`"})",
      std::nullopt,
      R"("!@#$%^&*()_+-={}:<>?,./~`")",
  };
  testArrayJoinNoReplacement<StringView>(
      input,
      ", ",
      R"({"key": "!@#$%^&*()_+-={}:<>?,./~`"}, !@#$%^&*()_+-={}:<>?,./~`)",
      false,
      true);
  testArrayJoinReplacement<StringView>(
      input,
      ", ",
      "0",
      R"({"key": "!@#$%^&*()_+-={}:<>?,./~`"}, 0, !@#$%^&*()_+-={}:<>?,./~`)",
      false,
      true);
}

TEST_F(ArrayJoinTest, unknownTest) {
  testArrayJoinNoReplacement<UnknownValue>(
      {std::nullopt, std::nullopt, std::nullopt}, ","_sv, ""_sv);

  testArrayJoinReplacement<UnknownValue>(
      {std::nullopt, std::nullopt, std::nullopt},
      ","_sv,
      "null"_sv,
      "null,null,null"_sv);
}

} // namespace
