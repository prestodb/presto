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

class SplitToMapTest : public test::FunctionBaseTest {};

TEST_F(SplitToMapTest, basic) {
  auto callFunc = [&](const RowVectorPtr& input) {
    return evaluate("split_to_map(c0, ',', ':')", input);
  };

  // Valid string cases.
  auto data = makeRowVector({
      makeFlatVector<std::string>({
          "1:10,2:20,3:30,4:",
          "4:40",
          "",
          ":00,1:11,3:33,5:55,7:77",
          ":",
          "11:17,",
          ":,",
      }),
  });

  auto result = callFunc(data);

  auto expected = makeMapVector<std::string, std::string>({
      {{"1", "10"}, {"2", "20"}, {"3", "30"}, {"4", ""}},
      {{"4", "40"}},
      {},
      {{"", "00"}, {"1", "11"}, {"3", "33"}, {"5", "55"}, {"7", "77"}},
      {{"", ""}},
      {{"11", "17"}},
      {{"", ""}},
  });

  velox::test::assertEqualVectors(expected, result);

  // Invalid string cases.
  const std::string errorMessage =
      "Key-value delimiter must appear exactly once in each entry. Bad input:";

  data = makeRowVector({makeFlatVector<std::string>({","})});
  VELOX_ASSERT_THROW(callFunc(data), errorMessage);

  data = makeRowVector({makeFlatVector<std::string>({",11:17"})});
  VELOX_ASSERT_THROW(callFunc(data), errorMessage);

  data = makeRowVector({makeFlatVector<std::string>({"11:17,,"})});
  VELOX_ASSERT_THROW(callFunc(data), errorMessage);

  data = makeRowVector({makeFlatVector<std::string>({"11"})});
  VELOX_ASSERT_THROW(callFunc(data), errorMessage);
}

TEST_F(SplitToMapTest, invalidInput) {
  auto data = makeRowVector({
      makeFlatVector<std::string>({
          "1:10,2:20,1:30",
      }),
  });

  auto splitToMap = [&](const std::string& entryDelimiter,
                        const std::string& keyValueDelimiter) {
    evaluate(
        fmt::format(
            "split_to_map(c0, '{}', '{}')", entryDelimiter, keyValueDelimiter),
        data);
  };

  VELOX_ASSERT_THROW(
      splitToMap(".", "."),
      "entryDelimiter and keyValueDelimiter must not be the same");
  VELOX_ASSERT_THROW(splitToMap(".", ""), "keyValueDelimiter is empty");
  VELOX_ASSERT_THROW(splitToMap("", "."), "entryDelimiter is empty");
  VELOX_ASSERT_THROW(
      splitToMap(":", ","),
      "Key-value delimiter must appear exactly once in each entry. Bad input: '1'");
  VELOX_ASSERT_THROW(
      splitToMap(",", ":"), "Duplicate keys (1) are not allowed.");
}

} // namespace
} // namespace facebook::velox::functions
