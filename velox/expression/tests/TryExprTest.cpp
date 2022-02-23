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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace facebook::velox {
class TryExprTest : public functions::test::FunctionBaseTest {};

TEST_F(TryExprTest, tryExpr) {
  auto a = makeFlatVector<int32_t>({10, 20, 30, 20, 50, 30});
  auto b = makeFlatVector<int32_t>({1, 0, 3, 4, 0, 6});
  {
    auto result = evaluate("try(c0 / c1)", makeRowVector({a, b}));

    auto expectedResult = makeNullableFlatVector<int32_t>(
        {10, std::nullopt, 10, 5, std::nullopt, 5});
    assertEqualVectors(expectedResult, result);
  }

  auto c = makeNullableFlatVector<StringView>({"1", "2x", "3", "4", "5y"});
  {
    auto result = evaluate("try(cast(c0 as integer))", makeRowVector({c}));
    auto expectedResult =
        makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4, std::nullopt});
    assertEqualVectors(expectedResult, result);
  }
}

// Returns the number of times this function has been called so far.
template <typename T>
struct CountCallsFunction {
  int64_t numCalls = 0;

  bool callNullable(int64_t& out, const int64_t*) {
    out = numCalls++;

    return true;
  }
};

TEST_F(TryExprTest, skipExecution) {
  registerFunction<CountCallsFunction, int64_t, int64_t>(
      {"count_calls"}, BIGINT());

  std::vector<std::optional<int64_t>> expected{
      0, std::nullopt, 1, std::nullopt, 2};
  auto flatVector = makeFlatVector<StringView>(expected.size(), [&](auto row) {
    return expected[row].has_value() ? "1" : "a";
  });
  auto result = evaluate<FlatVector<int64_t>>(
      "try(count_calls(cast(c0 as integer)))", makeRowVector({flatVector}));

  assertEqualVectors(makeNullableFlatVector(expected), result);
}
} // namespace facebook::velox
