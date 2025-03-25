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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions {
namespace {

class SplitToMultiMapTest : public functions::test::FunctionBaseTest {
 protected:
  void testValidInput(
      const std::string& input,
      const std::string& entryDelimiter,
      const std::string& keyValueDelimiter,
      const std::unordered_map<std::string, std::vector<std::string>>&
          expected) {
    auto data = makeRowVector({
        makeFlatVector<std::string>({input}),
    });

    auto result = evaluate(
        fmt::format(
            "split_to_multimap(c0, '{}', '{}')",
            entryDelimiter,
            keyValueDelimiter),
        data);

    std::vector<std::string> keys;
    std::vector<std::vector<std::string>> values;
    for (const auto& [key, valueList] : expected) {
      keys.push_back(key);
      values.push_back(valueList);
    }

    auto arrayVector = makeFlatVector<std::string>(keys);
    auto innerArrayVector = makeArrayVector<std::string>(values);
    auto mapVector = makeMapVector({0}, arrayVector, innerArrayVector);

    velox::test::assertEqualVectors(mapVector, result);
  }

  void testInvalidInput(
      const std::string& input,
      const std::string& entryDelimiter,
      const std::string& keyValueDelimiter,
      const std::string& expectedErrorMessage) {
    auto data = makeRowVector({
        makeFlatVector<std::string>({input}),
    });

    auto splitToMultiMap = [&](const std::string& entryDelimiter,
                               const std::string& keyValueDelimiter) {
      evaluate(
          fmt::format(
              "split_to_multimap(c0, '{}', '{}')",
              entryDelimiter,
              keyValueDelimiter),
          data);
    };

    VELOX_ASSERT_THROW(
        splitToMultiMap(entryDelimiter, keyValueDelimiter),
        expectedErrorMessage);
  }
};

TEST_F(SplitToMultiMapTest, basic) {
  testValidInput(
      "1:10,2:20,1:30",
      ",",
      ":",
      {
          {"1", {"10", "30"}},
          {"2", {"20"}},
      });

  testValidInput("", ",", ":", {});

  testValidInput(
      "a=123",
      ",",
      "=",
      {
          {"a", {"123"}},
      });

  testValidInput(
      "a=123,b=.4,c=,=d",
      ",",
      "=",
      {
          {"a", {"123"}},
          {"b", {".4"}},
          {"c", {""}},
          {"", {"d"}},
      });

  testValidInput(
      "a=123,a=.4,a=5.67",
      ",",
      "=",
      {
          {"a", {"123", ".4", "5.67"}},
      });

  testValidInput(
      "k=>v,k=>v",
      ",",
      "=>",
      {
          {"k", {"v", "v"}},
      });

  testValidInput(
      "k => v, k => v",
      ",",
      "=>",
      {
          {"k ", {" v"}},
          {" k ", {" v"}},
      });

  testValidInput(
      "key => value, key => value",
      ", ",
      "=>",
      {
          {"key ", {" value", " value"}},
      });

  testValidInput(
      "=",
      ",",
      "=",
      {
          {"", {""}},
      });
}

TEST_F(SplitToMultiMapTest, nonAsciiCharacters) {
  testValidInput(
      "\u4EA0\u4EFF\u4EA1",
      "\u4E00",
      "\u4EFF",
      {
          {"\u4EA0", {"\u4EA1"}},
      });

  testValidInput(
      "\u4EA0\u4EFF\u4EA1\u4E00\u4EA0\u4EFF\u4EB1",
      "\u4E00",
      "\u4EFF",
      {
          {"\u4EA0", {"\u4EA1", "\u4EB1"}},
      });
}

TEST_F(SplitToMultiMapTest, invalidInput) {
  // Test empty key-value delimiter
  testInvalidInput("1:10,2:20,1:30", ",", "", "keyValueDelimiter is empty");

  // Test empty entry delimiter
  testInvalidInput("1:10,2:20,1:30", "", ":", "entryDelimiter is empty");

  // Test entry delimiter and key-value delimiter are the same
  testInvalidInput(
      "1:10,2:20,1:30",
      ":",
      ":",
      "entryDelimiter and keyValueDelimiter must not be the same");

  // Test missing key-value delimiter in an entry
  testInvalidInput(
      "1:10,220,1:30",
      ",",
      ":",
      "Key-value delimiter must appear exactly once in each entry. Bad input: \'220");

  testInvalidInput(
      ",",
      ",",
      "=",
      "Key-value delimiter must appear exactly once in each entry. Bad input: \''");

  // Test multiple key-value delimiters in an entry
  testInvalidInput(
      "1:10:20,2:20,1:30",
      ",",
      ":",
      "Key-value delimiter must appear exactly once in each entry. Bad input: \'1:10:20");
}

TEST_F(SplitToMultiMapTest, testNullInArgsShouldReturnNull) {
  const auto nullMapVector = makeMapVector(
      {0}, makeFlatVector<std::string>({}), makeArrayVector<std::string>({}));
  nullMapVector->setNull(0, true);

  // Test1:  null input string
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({std::nullopt}),
  });

  auto result = evaluate(
      fmt::format("split_to_multimap(c0, '{}', '{}')", ',', ':'), data);

  velox::test::assertEqualVectors(nullMapVector, result);

  // Test2:  null entry delimiter
  data = makeRowVector({
      makeNullableFlatVector<std::string>({"1:10,2:20,1:30"}),
  });

  result =
      evaluate(fmt::format("split_to_multimap(c0, null, '{}')", ':'), data);
  velox::test::assertEqualVectors(nullMapVector, result);

  // Test3:  null key-value delimiter
  data = makeRowVector({
      makeNullableFlatVector<std::string>({"1:10,2:20,1:30"}),
  });

  result =
      evaluate(fmt::format("split_to_multimap(c0, '{}', null)", ':'), data);
  velox::test::assertEqualVectors(nullMapVector, result);

  // Test4 :  null entry delimiter and key-value delimiter
  data = makeRowVector({
      makeNullableFlatVector<std::string>({"1:10,2:20,1:30"}),
  });

  result = evaluate(fmt::format("split_to_multimap(c0, null, null)"), data);
  velox::test::assertEqualVectors(nullMapVector, result);
}

} // namespace
} // namespace facebook::velox::functions
