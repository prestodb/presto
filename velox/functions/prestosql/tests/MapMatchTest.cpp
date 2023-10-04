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

class MapMatchTest : public functions::test::FunctionBaseTest {
 protected:
  void match(
      const std::string& functionName,
      const VectorPtr& input,
      const std::string& lambda,
      const std::vector<std::optional<bool>>& expected) {
    const std::string expr =
        fmt::format("{}(c0, x -> ({}))", functionName, lambda);

    SCOPED_TRACE(expr);
    auto result = evaluate(expr, makeRowVector({input}));
    velox::test::assertEqualVectors(
        makeNullableFlatVector<bool>(expected), result);
  }

  void anyKeysMatch(
      const VectorPtr& input,
      const std::string& lambda,
      const std::vector<std::optional<bool>>& expected) {
    match("any_keys_match", input, lambda, expected);
  }

  void allKeysMatch(
      const VectorPtr& input,
      const std::string& lambda,
      const std::vector<std::optional<bool>>& expected) {
    match("all_keys_match", input, lambda, expected);
  }

  void noKeysMatch(
      const VectorPtr& input,
      const std::string& lambda,
      const std::vector<std::optional<bool>>& expected) {
    match("no_keys_match", input, lambda, expected);
  }

  void anyValuesMatch(
      const VectorPtr& input,
      const std::string& lambda,
      const std::vector<std::optional<bool>>& expected) {
    match("any_values_match", input, lambda, expected);
  }

  void noValuesMatch(
      const VectorPtr& input,
      const std::string& lambda,
      const std::vector<std::optional<bool>>& expected) {
    match("no_values_match", input, lambda, expected);
  }
};

TEST_F(MapMatchTest, anyKeysMatch) {
  auto data = makeMapVectorFromJson<int32_t, int64_t>({
      "{1: 10, 2: 20, 3: 30}",
      "{-1: 10, -2: 20}",
  });

  anyKeysMatch(data, "x > 0", {true, false});
  anyKeysMatch(data, "x > 2", {true, false});
  anyKeysMatch(data, "x > 3", {false, false});
  anyKeysMatch(data, "x <= 0", {false, true});
  anyKeysMatch(data, "x <= -2", {false, true});
  anyKeysMatch(data, "x <= -5", {false, false});
}

TEST_F(MapMatchTest, anyKeysMatchNull) {
  auto data = makeMapVectorFromJson<int32_t, int64_t>({
      "{1: 10, 2: 20, 3: 30}",
      "{-1: 10, -2: 20}",
  });

  anyKeysMatch(data, "if(x = 2, null::boolean, false)", {std::nullopt, false});
  anyKeysMatch(data, "if(x = 2, null::boolean, true)", {true, true});
}

TEST_F(MapMatchTest, allKeysMatch) {
  auto data = makeMapVectorFromJson<int32_t, int64_t>({
      "{1: 10, 2: 20, 3: 30}",
      "{-1: 10, -2: 20}",
  });

  allKeysMatch(data, "x > 0", {true, false});
  allKeysMatch(data, "x > 1", {false, false});
  allKeysMatch(data, "x > 10", {false, false});
  allKeysMatch(data, "x <= 0", {false, true});
  allKeysMatch(data, "x <= 3", {true, true});
  allKeysMatch(data, "x <= 100", {true, true});
}

TEST_F(MapMatchTest, allKeysMatchNull) {
  auto data = makeMapVectorFromJson<int32_t, int64_t>({
      "{1: 10, 2: 20, 3: 30}",
      "{-1: 10, -2: 20}",
  });

  allKeysMatch(data, "if(x = 2, null::boolean, false)", {false, false});
  allKeysMatch(data, "if(x = 2, null::boolean, true)", {std::nullopt, true});
}

TEST_F(MapMatchTest, noKeysMatch) {
  auto data = makeMapVectorFromJson<int32_t, int64_t>({
      "{1: 10, 2: 20, 3: 30}",
      "{-1: 10, -2: 20}",
  });

  noKeysMatch(data, "x > 0", {false, true});
  noKeysMatch(data, "x = 2", {false, true});
  noKeysMatch(data, "x = 22", {true, true});
  noKeysMatch(data, "x = -2", {true, false});
  noKeysMatch(data, "x > 2 OR x < -1", {false, false});
}

TEST_F(MapMatchTest, noKeysMatchNull) {
  auto data = makeMapVectorFromJson<int32_t, int64_t>({
      "{1: 10, 2: 20, 3: 30}",
      "{-1: 10, -2: 20}",
  });

  noKeysMatch(data, "if(x = 2, null::boolean, false)", {std::nullopt, true});
  noKeysMatch(data, "if(x = 2, null::boolean, true)", {false, false});
}

TEST_F(MapMatchTest, anyValuesMatch) {
  auto data = makeMapVectorFromJson<int32_t, int64_t>({
      "{1: 10, 2: 20, 3: 30}",
      "{-1: 11, -2: 22}",
  });

  anyValuesMatch(data, "x = 10", {true, false});
  anyValuesMatch(data, "x = 22", {false, true});
  anyValuesMatch(data, "x < 15", {true, true});
  anyValuesMatch(data, "x < 0", {false, false});
  anyValuesMatch(data, "x IN (20, 11)", {true, true});
}

TEST_F(MapMatchTest, anyValuesMatchNull) {
  auto data = makeMapVectorFromJson<int32_t, int64_t>({
      "{1: 10, 2: 20, 3: 30}",
      "{-1: 11, -2: 22}",
  });

  anyValuesMatch(
      data, "if(x = 20, null::boolean, false)", {std::nullopt, false});
  anyValuesMatch(data, "if(x = 20, null::boolean, true)", {true, true});
}

TEST_F(MapMatchTest, noValuesMatch) {
  auto data = makeMapVectorFromJson<int32_t, int64_t>({
      "{1: 10, 2: 20, 3: 30}",
      "{-1: 11, -2: 22}",
  });

  noValuesMatch(data, "x = 7", {true, true});
  noValuesMatch(data, "x > 15", {false, false});
  noValuesMatch(data, "x > 25", {false, true});
  noValuesMatch(data, "x % 11 = 0", {true, false});
}

TEST_F(MapMatchTest, noValuesMatchNull) {
  auto data = makeMapVectorFromJson<int32_t, int64_t>({
      "{1: 10, 2: 20, 3: 30}",
      "{-1: 11, -2: 22}",
  });

  noValuesMatch(data, "if(x = 20, null::boolean, false)", {std::nullopt, true});
  noValuesMatch(data, "if(x = 20, null::boolean, true)", {false, false});
}

} // namespace
} // namespace facebook::velox::functions
