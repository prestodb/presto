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
#include "velox/functions/sparksql/specialforms/AtLeastNNonNulls.h"
#include "velox/common/base/tests/FloatConstants.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class AtLeastNNonNullsTest : public SparkFunctionBaseTest {
 public:
  AtLeastNNonNullsTest() {
    // Allow for parsing literal integers as INTEGER, not BIGINT.
    options_.parseIntegerAsBigint = false;
  }
};

TEST_F(AtLeastNNonNullsTest, basic) {
  auto testAtLeastNNonNulls = [&](int32_t n,
                                  const std::vector<VectorPtr>& input,
                                  const VectorPtr& expected) {
    std::string func = fmt::format("at_least_n_non_nulls({}", n);
    for (auto i = 0; i < input.size(); ++i) {
      func += fmt::format(", c{}", i);
    }
    func += ")";
    const auto result = evaluate(func, makeRowVector(input));
    assertEqualVectors(expected, result);
  };
  auto strings = makeNullableFlatVector<StringView>(
      {std::nullopt, "1", "", std::nullopt, ""});
  auto bools = makeNullableFlatVector<bool>(
      {std::nullopt, true, false, std::nullopt, std::nullopt});
  auto ints =
      makeNullableFlatVector<int32_t>({-1, 0, 1, std::nullopt, std::nullopt});
  auto floats = makeNullableFlatVector<float>(
      {FloatConstants::kMaxF, FloatConstants::kNaNF, 0.1f, 0.0f, std::nullopt});
  auto doubles = makeNullableFlatVector<double>(
      {std::log(-2.0),
       FloatConstants::kMaxD,
       FloatConstants::kNaND,
       std::nullopt,
       0.1});
  auto arrays = makeArrayVectorFromJson<int32_t>(
      {"[1, null, 3]", "[1, 2, 3]", "null", "[null]", "[]"});
  auto maps = makeMapVectorFromJson<int32_t, int32_t>(
      {"{1: 10, 2: null, 3: null}", "{1: 10, 2: 20}", "{1: 2}", "{}", "null"});
  auto consts = makeConstant<int32_t>(2, 5);
  auto indices = makeIndices({1, 2, 3, 4, 0});
  auto dicts = wrapInDictionary(indices, 5, doubles);

  auto expected = makeFlatVector<bool>({false, true, true, false, false});
  testAtLeastNNonNulls(2, {strings, bools}, expected);

  expected = makeFlatVector<bool>({false, false, false, false, false});
  testAtLeastNNonNulls(3, {strings, bools}, expected);

  expected = makeFlatVector<bool>({true, true, true, true, true});
  testAtLeastNNonNulls(0, {strings, bools}, expected);
  testAtLeastNNonNulls(-1, {strings, bools}, expected);

  expected = makeFlatVector<bool>({true, false, true, true, false});
  testAtLeastNNonNulls(1, {floats}, expected);

  expected = makeFlatVector<bool>({false, true, false, false, true});
  testAtLeastNNonNulls(1, {doubles}, expected);

  expected = makeFlatVector<bool>({false, true, true, false, false});
  testAtLeastNNonNulls(2, {strings, bools, floats}, expected);

  expected = makeFlatVector<bool>({false, true, true, false, false});
  testAtLeastNNonNulls(3, {bools, ints, floats, doubles}, expected);

  expected = makeFlatVector<bool>({false, false, false, false, false});
  testAtLeastNNonNulls(2, {floats, doubles}, expected);

  expected = makeFlatVector<bool>({true, false, false, true, false});
  testAtLeastNNonNulls(4, {maps, arrays, consts, dicts}, expected);
}

TEST_F(AtLeastNNonNullsTest, error) {
  auto input = makeFlatVector<int32_t>({1, 2, 3});

  VELOX_ASSERT_USER_THROW(
      evaluate("at_least_n_non_nulls(1.0, c0)", makeRowVector({input})),
      "The first input type should be INTEGER but got DOUBLE");
  VELOX_ASSERT_USER_THROW(
      evaluate("at_least_n_non_nulls(1)", makeRowVector({})),
      "AtLeastNNonNulls expects to receive at least 2 arguments");
  VELOX_ASSERT_USER_THROW(
      evaluate("at_least_n_non_nulls(c0, c1)", makeRowVector({input, input})),
      "The first parameter should be constant expression");
  VELOX_ASSERT_USER_THROW(
      evaluate(
          "at_least_n_non_nulls(cast(null as int), c0)",
          makeRowVector({input})),
      "The first parameter should not be null");
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
