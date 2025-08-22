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
#include "velox/functions/sparksql/tests/JsonTestUtil.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {
class FromJsonTest : public SparkFunctionBaseTest {
 protected:
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
      {1.0,          2.0,          -3.4028235E38, 3.4028235E38, -kInfFloat,
       kInfFloat,    0.0,          0.0,           std::nullopt, std::nullopt,
       std::nullopt, std::nullopt, std::nullopt,  std::nullopt, std::nullopt,
       std::nullopt, kNaNFloat,    kNaNFloat,     -kInfFloat,   -kInfFloat,
       -kInfFloat,   -kInfFloat,   kInfFloat,     kInfFloat,    kInfFloat,
       kInfFloat,    kInfFloat,    kInfFloat});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})",
       R"({"a": 2.0})",
       R"({"a": -3.4028235E38})", // Min float value
       R"({"a": 3.4028235E38})", // Max float value
       R"({"a": -3.4028235E39})",
       R"({"a": 3.4028235E39})",
       R"({"a": 0})",
       R"({"a": 1.0e-200})",
       R"({"a": "3"})",
       R"({"a": 1.1.0})", // Multiple decimal points.
       R"({"a": 1.})", // Missing fraction digits after a decimal point.
       R"({"a": 01})", // Leading zero.
       R"({"a": 1e})", // Missing exponent digits after ‘e’ or ‘E’.
       R"({"a": 1e+})", // Missing exponent digits after ‘e’ or ‘E’.
       R"({"a": .e10})", // Missing digits.
       R"({"a": -.})", // Missing digits entirely.
       R"({"a": "NaN"})",
       R"({"a": NaN})",
       R"({"a": "-Infinity"})",
       R"({"a": -Infinity})",
       R"({"a": "-INF"})",
       R"({"a": -INF})",
       R"({"a": "+Infinity"})",
       R"({"a": +Infinity})",
       R"({"a": "Infinity"})",
       R"({"a": Infinity})",
       R"({"a": "+INF"})",
       R"({"a": +INF})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, basicDouble) {
  auto expected = makeNullableFlatVector<double>(
      {1.0,
       2.0,
       -1.7976931348623158e+308,
       1.7976931348623158e+308,
       -kInfDouble,
       kInfDouble,
       0.0,
       0.0,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       kNaNDouble,
       kNaNDouble,
       -kInfDouble,
       -kInfDouble,
       -kInfDouble,
       -kInfDouble,
       kInfDouble,
       kInfDouble,
       kInfDouble,
       kInfDouble,
       kInfDouble,
       kInfDouble});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})",
       R"({"a": 2.0})",
       R"({"a": -1.7976931348623158e+308})", // Min double value
       R"({"a": 1.7976931348623158e+308})", // Max double value
       R"({"a": -1.7976931348623158e+309})",
       R"({"a": 1.7976931348623158e+309})",
       R"({"a": 0})",
       R"({"a": 1.0e-2000})",
       R"({"a": "3"})",
       R"({"a": 1.1.0})", // Multiple decimal points.
       R"({"a": 1.})", // Missing fraction digits after a decimal point.
       R"({"a": 01})", // Leading zero.
       R"({"a": 1e})", // Missing exponent digits after ‘e’ or ‘E’.
       R"({"a": 1e+})", // Missing exponent digits after ‘e’ or ‘E’.
       R"({"a": .e10})", // Missing digits.
       R"({"a": -.})", // Missing digits entirely.
       R"({"a": "NaN"})",
       R"({"a": NaN})",
       R"({"a": "-Infinity"})",
       R"({"a": -Infinity})",
       R"({"a": "-INF"})",
       R"({"a": -INF})",
       R"({"a": "+Infinity"})",
       R"({"a": +Infinity})",
       R"({"a": "Infinity"})",
       R"({"a": Infinity})",
       R"({"a": "+INF"})",
       R"({"a": +INF})"});
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

TEST_F(FromJsonTest, basicShortDecimal) {
  auto expected = makeNullableFlatVector<int64_t>(
      {53210, -100, std::nullopt, std::nullopt}, DECIMAL(7, 2));
  auto input = makeFlatVector<std::string>(
      {R"({"a": "5.321E2"})",
       R"({"a": -1})",
       R"({"a": 55555555555555555555.5555})",
       R"({"a": "+1BD"})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, basicLongDecimal) {
  auto expected = makeNullableFlatVector<int128_t>(
      {53210000,
       -100000,
       HugeInt::build(0xffff, 0xffffffffffffffff),
       std::nullopt},
      DECIMAL(38, 5));
  auto input = makeFlatVector<std::string>(
      {R"({"a": "5.321E2"})",
       R"({"a": -1})",
       R"({"a": 12089258196146291747.06175})",
       R"({"a": "+1BD"})"});
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
  // ARRAY(ROW(BIGINT))
  std::vector<vector_size_t> offsets;
  offsets.push_back(0);
  offsets.push_back(1);
  offsets.push_back(2);
  auto arrayVector = makeArrayVector(
      offsets, makeRowVector({"a"}, {makeFlatVector<int64_t>({1, 2, 2})}));
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})", R"([{"a": 2}])", R"([{"a": 2}])"});
  testFromJson(input, arrayVector);

  // MAP(ARRAY(ROW(BIGINT, INTEGER)))
  auto keyVector = makeFlatVector<StringView>({"a", "b", "c"});
  auto valueVector = makeArrayVector(
      offsets,
      makeRowVector(
          {"d", "e"},
          {makeFlatVector<int64_t>({1, 2, 3}),
           makeNullableFlatVector<int32_t>({3, 4, std::nullopt})}));
  auto mapVector = makeMapVector(offsets, keyVector, valueVector);
  auto mapInput = makeFlatVector<std::string>(
      {R"({"a": [{"d": 1, "e": 3}]})",
       R"({"b": [{"d": 2, "e": 4}]})",
       R"({"c": [{"d": 3}]})"});
  testFromJson(mapInput, mapVector);

  // ROW(ROW(ROW(BIGINT, INTEGER)))
  auto rowVector = makeRowVector(
      {"a"},
      {makeRowVector(
          {"b"},
          {makeRowVector(
              {"d1", "e1"},
              {makeFlatVector<int64_t>({1, 2, 3}),
               makeNullableFlatVector<int32_t>({3, 4, std::nullopt})})})});
  auto rowInput = makeFlatVector<std::string>(
      {R"({"a": {"b": {"d1": 1, "e1": 3, "e1": 4}}})", // Duplicate keys.
       R"({"a": {"b": {"D1": 2, "e1": 4}}})", // Key case insensitive.
       R"({"a": {"b": {"d1": 3, "f3": 3}}})"}); // Key not in schema.
  testFromJson(rowInput, rowVector);

  // ROW(ARRAY[BIGINT], BIGINT)
  std::vector<vector_size_t> offsets1;
  offsets1.push_back(0);
  offsets1.push_back(1);
  offsets1.push_back(3);
  auto arrayVector1 =
      makeArrayVector(offsets1, makeFlatVector<int64_t>({1, 2, 2, 3, 3, 3}));
  auto rowVector1 = makeRowVector(
      {"a", "b"}, {arrayVector1, makeFlatVector<int64_t>({1, 2, 3})});
  auto rowInput1 = makeFlatVector<std::string>(
      {R"({"a": [1], "b": 1})",
       R"({"a": [2, 2], "b": 2})",
       R"({"a": [3, 3, 3], "b": 3})"});
  testFromJson(rowInput1, rowVector1);

  // ROW(ROW(BIGINT, ARRAY[BIGINT]), ARRAY[BIGINT])
  auto rowVector2 = makeRowVector(
      {"a", "b"},
      {makeRowVector(
           {"c", "d"}, {makeFlatVector<int64_t>({1, 3, 4}), arrayVector1}),
       arrayVector1});
  auto rowInput2 = makeFlatVector<std::string>(
      {R"({"a": {"c": 1, "d": [1]}, "b": [1]})",
       R"({"a": {"c": 3, "d": [2, 2]}, "b": [2, 2]})",
       R"({"a": {"c": 4, "d": [3, 3, 3]}, "b": [3, 3, 3]})"});
  testFromJson(rowInput2, rowVector2);

  // ROW(ROW(ROW(BIGINT)), ROW(ROW(BIGINT, BIGINT)))
  auto rowVector3 = makeRowVector(
      {"a", "d"},
      {makeRowVector(
           {"b"}, {makeRowVector({"c"}, {makeFlatVector<int64_t>({1, 2, 3})})}),
       makeRowVector(
           {"e"},
           {makeRowVector(
               {"f", "g"},
               {makeFlatVector<int64_t>({1, 2, 3}),
                makeFlatVector<int64_t>({4, 5, 6})})})});
  auto rowInput3 = makeFlatVector<std::string>(
      {R"({"a": {"b": {"c": 1}}, "d": {"e": {"f": 1, "g": 4}}})",
       R"({"a": {"b": {"c": 2}}, "d": {"e": {"f": 2, "g": 5}}})",
       R"({"a": {"b": {"c": 3}}, "d": {"e": {"f": 3, "g": 6}}})"});
  testFromJson(rowInput3, rowVector3);

  // ROW(ARRAY[ROW(BIGINT)], ARRAY[ROW(BIGINT, BIGINT)])
  std::vector<vector_size_t> offsets2;
  offsets2.push_back(0);
  offsets2.push_back(1);
  offsets2.push_back(2);
  auto arrayVector3 = makeArrayVector(
      offsets2,
      makeRowVector({"c"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5})}));
  std::vector<vector_size_t> offsets3;
  offsets3.push_back(0);
  offsets3.push_back(1);
  offsets3.push_back(3);
  auto arrayVector4 = makeArrayVector(
      offsets3,
      makeRowVector(
          {"d", "e"},
          {makeFlatVector<int64_t>({3, 2, 2, 1}),
           makeFlatVector<int64_t>({7, 4, 8, 9})}));
  auto rowVector4 = makeRowVector({"a", "b"}, {arrayVector3, arrayVector4});
  auto rowInput4 = makeFlatVector<std::string>(
      {R"({"a": [{"c": 1}], "b": [{"d": 3, "e": 7}]})",
       R"({"a": [{"c": 2}], "b": [{"d": 2, "e": 4}, {"d": 2, "e": 8}]})",
       R"({"a": [{"c": 3}, {"c": 4}, {"c": 5}], "b": [{"d": 1, "e": 9}]})"});
  testFromJson(rowInput4, rowVector4);
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
  auto mapOutput =
      makeMapVector<int64_t, int64_t>({{{1, 1}}, {{2, 2}}, {{3, 3}}});
  auto input = makeFlatVector<std::string>({R"(2)", R"({2)", R"({3)"});
  VELOX_ASSERT_USER_THROW(
      testFromJson(input, primitiveTypeOutput), "Unsupported type BIGINT.");
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
