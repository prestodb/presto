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
#include <limits>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {
constexpr float kNaNFloat = std::numeric_limits<float>::quiet_NaN();
constexpr float kInfFloat = std::numeric_limits<float>::infinity();
constexpr double kNaNDouble = std::numeric_limits<double>::quiet_NaN();
constexpr double kInfDouble = std::numeric_limits<double>::infinity();

class FromJsonTest : public SparkFunctionBaseTest {
 protected:
  core::CallTypedExprPtr createFromJson(const TypePtr& outputType) {
    std::vector<core::TypedExprPtr> inputs = {
        std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0")};

    return std::make_shared<const core::CallTypedExpr>(
        outputType, std::move(inputs), "from_json");
  }

  void testFromJson(const VectorPtr& input, const VectorPtr& expected) {
    auto expr = createFromJson(expected->type());
    testEncodings(expr, {input}, expected);
  }
};

TEST_F(FromJsonTest, basicStruct) {
  auto expected = makeFlatVector<int64_t>({1, 2, 3});
  auto input = makeFlatVector<std::string>(
      {R"({"Id": 1})", R"({"Id": 2})", R"({"Id": 3})"});
  testFromJson(input, makeRowVector({"Id"}, {expected}));
}

TEST_F(FromJsonTest, basicArray) {
  auto expected = makeArrayVector<int64_t>({{1}, {2}, {}});
  auto input = makeFlatVector<std::string>({R"([1])", R"([2])", R"([])"});
  testFromJson(input, expected);
}

TEST_F(FromJsonTest, basicMap) {
  auto expected = makeMapVector<std::string, int64_t>(
      {{{"a", 1}}, {{"b", 2}}, {{"c", 3}}, {{"3", 3}}});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})", R"({"b": 2})", R"({"c": 3})", R"({"3": 3})"});
  testFromJson(input, expected);
}

TEST_F(FromJsonTest, basicBool) {
  auto expected = makeNullableFlatVector<bool>(
      {true, false, std::nullopt, std::nullopt, std::nullopt});
  auto input = makeFlatVector<std::string>(
      {R"({"a": true})",
       R"({"a": false})",
       R"({"a": 1})",
       R"({"a": 0.0})",
       R"({"a": "true"})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, basicTinyInt) {
  auto expected = makeNullableFlatVector<int8_t>(
      {1, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})",
       R"({"a": -129})",
       R"({"a": 128})",
       R"({"a": 1.0})",
       R"({"a": "1"})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, basicSmallInt) {
  auto expected = makeNullableFlatVector<int16_t>(
      {1, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})",
       R"({"a": -32769})",
       R"({"a": 32768})",
       R"({"a": 1.0})",
       R"({"a": "1"})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, basicInt) {
  auto expected = makeNullableFlatVector<int32_t>(
      {1, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})",
       R"({"a": -2147483649})",
       R"({"a": 2147483648})",
       R"({"a": 2.0})",
       R"({"a": "3"})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, basicBigInt) {
  auto expected =
      makeNullableFlatVector<int32_t>({1, std::nullopt, std::nullopt});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})", R"({"a": 2.0})", R"({"a": "3"})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, basicFloat) {
  auto expected = makeNullableFlatVector<float>(
      {1.0,
       2.0,
       std::nullopt,
       kNaNFloat,
       -kInfFloat,
       -kInfFloat,
       kInfFloat,
       kInfFloat,
       kInfFloat});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})",
       R"({"a": 2.0})",
       R"({"a": "3"})",
       R"({"a": "NaN"})",
       R"({"a": "-Infinity"})",
       R"({"a": "-INF"})",
       R"({"a": "+Infinity"})",
       R"({"a": "Infinity"})",
       R"({"a": "+INF"})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, basicDouble) {
  auto expected = makeNullableFlatVector<double>(
      {1.0,
       2.0,
       std::nullopt,
       kNaNDouble,
       -kInfDouble,
       -kInfDouble,
       kInfDouble,
       kInfDouble,
       kInfDouble});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})",
       R"({"a": 2.0})",
       R"({"a": "3"})",
       R"({"a": "NaN"})",
       R"({"a": "-Infinity"})",
       R"({"a": "-INF"})",
       R"({"a": "+Infinity"})",
       R"({"a": "Infinity"})",
       R"({"a": "+INF"})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, basicDate) {
  auto expected = makeNullableFlatVector<int32_t>(
      {18809,
       18809,
       18809,
       0,
       18809,
       18809,
       -713975,
       15,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt},
      DATE());
  auto input = makeFlatVector<std::string>(
      {R"({"a": "2021-07-01T"})",
       R"({"a": "2021-07-01"})",
       R"({"a": "2021-7"})",
       R"({"a": "1970"})",
       R"({"a": "2021-07-01T00:00GMT+01:00"})",
       R"({"a": "2021-7-GMTGMT1"})",
       R"({"a": "0015-03-16T123123"})",
       R"({"a": "015"})",
       R"({"a": "15.0"})",
       R"({"a": "AA"})",
       R"({"a": "1999-08 01"})",
       R"({"a": "2020/12/1"})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, basicString) {
  auto expected = makeNullableFlatVector<StringView>(
      {"1", "2.0", "true", "{\"b\": \"test\"}", "[1, 2]"});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})",
       R"({"a": 2.0})",
       R"({"a": "true"})",
       R"({"a": {"b": "test"}})",
       R"({"a": [1, 2]})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, nestedComplexType) {
  auto rowVector = makeRowVector({"a"}, {makeFlatVector<int64_t>({1, 2, 2})});
  std::vector<vector_size_t> offsets;
  offsets.push_back(0);
  offsets.push_back(1);
  offsets.push_back(2);
  auto arrayVector = makeArrayVector(offsets, rowVector);
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})", R"([{"a": 2}])", R"([{"a": 2}])"});
  testFromJson(input, arrayVector);
}

TEST_F(FromJsonTest, structEmptyArray) {
  auto expected = makeNullableFlatVector<int64_t>({std::nullopt, 2, 3});
  auto input =
      makeFlatVector<std::string>({R"([])", R"({"a": 2})", R"({"a": 3})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, structEmptyStruct) {
  auto expected = makeNullableFlatVector<int64_t>({std::nullopt, 2, 3});
  auto input =
      makeFlatVector<std::string>({R"({ })", R"({"a": 2})", R"({"a": 3})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, structWrongSchema) {
  auto expected = makeNullableFlatVector<int64_t>({std::nullopt, 2, 3});
  auto input = makeFlatVector<std::string>(
      {R"({"b": 2})", R"({"a": 2})", R"({"a": 3})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, structWrongData) {
  auto expected = makeNullableFlatVector<int64_t>({std::nullopt, 2, 3});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 2.1})", R"({"a": 2})", R"({"a": 3})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, invalidType) {
  auto primitiveTypeOutput = makeFlatVector<int64_t>({2, 2, 3});
  auto decimalOutput = makeFlatVector<int64_t>({2, 2, 3}, DECIMAL(16, 7));
  auto mapOutput =
      makeMapVector<int64_t, int64_t>({{{1, 1}}, {{2, 2}}, {{3, 3}}});
  auto input = makeFlatVector<std::string>({R"(2)", R"({2)", R"({3)"});
  VELOX_ASSERT_USER_THROW(
      testFromJson(input, primitiveTypeOutput), "Unsupported type BIGINT.");
  VELOX_ASSERT_USER_THROW(
      testFromJson(input, makeRowVector({"a"}, {decimalOutput})),
      "Unsupported type ROW<a:DECIMAL(16, 7)>");
  VELOX_ASSERT_USER_THROW(
      testFromJson(input, mapOutput), "Unsupported type MAP<BIGINT,BIGINT>.");
}

TEST_F(FromJsonTest, invalidJson) {
  auto expected = makeNullableFlatVector<int32_t>(
      {std::nullopt, std::nullopt, std::nullopt});
  auto input =
      makeFlatVector<std::string>({R"("a": 1})", R"({a: 1})", R"({"a" 1})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
