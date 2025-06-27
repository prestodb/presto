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

namespace facebook::velox {

namespace {

class RegexpSplitTest : public functions::test::FunctionBaseTest {};

TEST_F(RegexpSplitTest, split) {
  auto input = makeRowVector({
      makeFlatVector<std::string>({
          "1a 2b 14m",
          "1a 2b 14",
          "",
          "a123b",
      }),
  });
  auto result = evaluate("regexp_split(c0, '\\s*[a-z]+\\s*')", input);

  auto expected = makeArrayVector<std::string>({
      {"1", "2", "14", ""},
      {"1", "2", "14"},
      {""},
      {"", "123", ""},
  });
  test::assertEqualVectors(expected, result);

  result = evaluate("regexp_split(c0, '\\s*\\d+\\s*')", input);
  expected = makeArrayVector<std::string>({
      {"", "a", "b", "m"},
      {"", "a", "b", ""},
      {""},
      {"a", "b"},
  });
  test::assertEqualVectors(expected, result);

  // Test for empty matches
  result = evaluate("regexp_split(c0, '')", input);
  expected = makeArrayVector<std::string>({
      {"", "1", "a", " ", "2", "b", " ", "1", "4", "m", ""},
      {"", "1", "a", " ", "2", "b", " ", "1", "4", ""},
      {"", ""},
      {"", "a", "1", "2", "3", "b", ""},
  });
  test::assertEqualVectors(expected, result);

  // Test for another case of empty matches
  result = evaluate("regexp_split(c0, '\\s*[a-z]*\\s*')", input);
  expected = makeArrayVector<std::string>({
      {"", "1", "", "2", "", "1", "4", "", ""},
      {"", "1", "", "2", "", "1", "4", ""},
      {"", ""},
      {"", "", "1", "2", "3", "", ""},
  });
  test::assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox
