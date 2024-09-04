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

  auto tryCallFunc = [&](const RowVectorPtr& input) {
    auto result = evaluate("try(split_to_map(c0, ',', ':'))", input);
    auto expected = makeAllNullMapVector(input->size(), VARCHAR(), VARCHAR());
    velox::test::assertEqualVectors(expected, result);
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
  tryCallFunc(data);

  data = makeRowVector({makeFlatVector<std::string>({",11:17"})});
  VELOX_ASSERT_THROW(callFunc(data), errorMessage);
  tryCallFunc(data);

  data = makeRowVector({makeFlatVector<std::string>({"11:17,,"})});
  VELOX_ASSERT_THROW(callFunc(data), errorMessage);
  tryCallFunc(data);

  data = makeRowVector({makeFlatVector<std::string>({"11"})});
  VELOX_ASSERT_THROW(callFunc(data), errorMessage);
  tryCallFunc(data);
}

TEST_F(SplitToMapTest, MultiCharKeyValueDelimiter) {
  const auto callFunc = [&](const RowVectorPtr& input) {
    return evaluate("split_to_map(c0, ',', ';=')", input);
  };

  auto tryCallFunc = [&](const RowVectorPtr& input) {
    auto result = evaluate("try(split_to_map(c0, ',', ':='))", input);
    auto expected = makeAllNullMapVector(input->size(), VARCHAR(), VARCHAR());
    velox::test::assertEqualVectors(expected, result);
  };

  // Valid string cases.
  auto data = makeRowVector({
      makeFlatVector<std::string>(
          {"1;=10,2;=20,3;=30,4;=",
           "4;=40,",
           "",
           ";=00,1;=11,3;=33,5;=55,7;=77",
           ";=",
           "4;=40\",2;=20\""}),
  });

  auto result = callFunc(data);

  auto expected = makeMapVector<std::string, std::string>(
      {{{"1", "10"}, {"2", "20"}, {"3", "30"}, {"4", ""}},
       {{"4", "40"}},
       {},
       {{"", "00"}, {"1", "11"}, {"3", "33"}, {"5", "55"}, {"7", "77"}},
       {{"", ""}},
       {{"4", "40\""}, {"2", "20\""}}});

  velox::test::assertEqualVectors(expected, result);

  // Invalid string cases.
  const std::string errorMessage =
      "Key-value delimiter must appear exactly once in each entry. Bad input:";

  // repeated key value delimiter
  data = makeRowVector({makeFlatVector<std::string>({"1;=;=10,2;=;=20"})});
  VELOX_ASSERT_THROW(callFunc(data), errorMessage);
  tryCallFunc(data);

  // No Delimiter found
  data = makeRowVector({makeFlatVector<std::string>({"1;=10,,"})});
  VELOX_ASSERT_THROW(callFunc(data), errorMessage);
  tryCallFunc(data);
}

TEST_F(SplitToMapTest, MultiCharEntryDelimiter) {
  const auto callFunc = [&](const RowVectorPtr& input) {
    return evaluate("split_to_map(c0, ';;', ';')", input);
  };

  auto tryCallFunc = [&](const RowVectorPtr& input) {
    auto result = evaluate("try(split_to_map(c0, ';;', ';'))", input);
    auto expected = makeAllNullMapVector(input->size(), VARCHAR(), VARCHAR());
    velox::test::assertEqualVectors(expected, result);
  };

  // Valid string cases.
  auto data = makeRowVector({
      makeFlatVector<std::string>({
          "1;10;;2;20;;3;30;;4;",
          "4;40;;",
          "",
          ";00;;1;11;;3;33;;5;55;;7;77",
          ";",
      }),
  });

  auto result = callFunc(data);

  auto expected = makeMapVector<std::string, std::string>({
      {{"1", "10"}, {"2", "20"}, {"3", "30"}, {"4", ""}},
      {{"4", "40"}},
      {},
      {{"", "00"}, {"1", "11"}, {"3", "33"}, {"5", "55"}, {"7", "77"}},
      {{"", ""}},
  });

  velox::test::assertEqualVectors(expected, result);

  const std::string errorMessage =
      "Key-value delimiter must appear exactly once in each entry. Bad input:";
  // No Delimiter found
  data = makeRowVector({makeFlatVector<std::string>({"a;b;;;;c;d"})});
  VELOX_ASSERT_THROW(callFunc(data), errorMessage);
  tryCallFunc(data);
}

TEST_F(SplitToMapTest, invalidInput) {
  auto data = makeRowVector({
      makeFlatVector<std::string>({
          "1:10,2:20,1:30",
      }),
  });

  auto splitToMap = [&](const std::string& entryDelimiter,
                        const std::string& keyValueDelimiter,
                        RowVectorPtr data) {
    evaluate(
        fmt::format(
            "split_to_map(c0, '{}', '{}')", entryDelimiter, keyValueDelimiter),
        data);
  };

  auto trySplitToMap = [&](const std::string& entryDelimiter,
                           const std::string& keyValueDelimiter,
                           const RowVectorPtr& data) {
    SCOPED_TRACE(fmt::format("{} {}", entryDelimiter, keyValueDelimiter));
    auto result = evaluate(
        fmt::format(
            "try(split_to_map(c0, '{}', '{}'))",
            entryDelimiter,
            keyValueDelimiter),
        data);
    auto expected = makeAllNullMapVector(data->size(), VARCHAR(), VARCHAR());
    velox::test::assertEqualVectors(expected, result);
  };

  VELOX_ASSERT_THROW(
      splitToMap(".", ".", data),
      "entryDelimiter and keyValueDelimiter must not be the same");
  trySplitToMap(".", ".", data);

  VELOX_ASSERT_THROW(splitToMap(".", "", data), "keyValueDelimiter is empty");
  trySplitToMap(".", "", data);

  VELOX_ASSERT_THROW(splitToMap("", ".", data), "entryDelimiter is empty");
  trySplitToMap("", ".", data);

  VELOX_ASSERT_THROW(
      splitToMap(":", ",", data),
      "No delimiter found. Key-value delimiter must appear exactly once in each entry. Bad input: '1'");
  trySplitToMap(":", ",", data);

  VELOX_ASSERT_THROW(
      splitToMap(",", ":", data), "Duplicate keys (1) are not allowed.");
  trySplitToMap(",", ":", data);

  data = makeRowVector({
      makeFlatVector<std::string>({
          "1::10,2:20,1:30",
      }),
  });
  VELOX_ASSERT_THROW(
      splitToMap(",", ":", data),
      "More than one delimiter found. Key-value delimiter must appear exactly once in each entry");
  trySplitToMap(",", ":", data);
}

TEST_F(SplitToMapTest, lambda) {
  auto splitToMap = [&](const std::string& input, const std::string& lambda) {
    auto rowVector = makeRowVector({
        makeFlatVector<std::string>({input}),
    });

    return evaluate(
        fmt::format("split_to_map(c0, ',', ':', {})", lambda), rowVector);
  };

  // No duplicate keys.
  auto result = splitToMap("1:a,2:b,3:c", "(k, v1, v2) -> v1");
  auto expected = makeMapVector<std::string, std::string>({
      {{"1", "a"}, {"2", "b"}, {"3", "c"}},
  });
  velox::test::assertEqualVectors(expected, result);

  // Duplicate keys. Keep first.
  result = splitToMap("1:a,2:b,1:c,2:d,3:e", "(k, v1, v2) -> v1");
  expected = makeMapVector<std::string, std::string>({
      {{"1", "a"}, {"2", "b"}, {"3", "e"}},
  });
  velox::test::assertEqualVectors(expected, result);

  // Duplicate keys. Keep last.
  result = splitToMap("1:a,2:b,1:c,2:d,3:e", "(k, v1, v2) -> v2");
  expected = makeMapVector<std::string, std::string>({
      {{"1", "c"}, {"2", "d"}, {"3", "e"}},
  });
  velox::test::assertEqualVectors(expected, result);

  VELOX_ASSERT_USER_THROW(
      splitToMap("1:a,2:b,1:c,2:d,3:e", "(k, v1, v2) -> concat(v1, v2)"),
      "split_to_map with arbitrary lambda is not supported");
}

} // namespace
} // namespace facebook::velox::functions
