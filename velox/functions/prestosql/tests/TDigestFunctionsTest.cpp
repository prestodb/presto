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
#include <folly/base64.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TDigestRegistration.h"
#include "velox/functions/prestosql/types/TDigestType.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

class TDigestFunctionsTest : public FunctionBaseTest {
 protected:
  void SetUp() override {
    FunctionBaseTest::SetUp();
    registerTDigestType();
  }
  std::string decodeBase64(std::string_view input) {
    std::string decoded(folly::base64DecodedSize(input), '\0');
    folly::base64Decode(input, decoded.data());
    return decoded;
  }
  const TypePtr TDIGEST_DOUBLE = TDIGEST(DOUBLE());
  const TypePtr ARRAY_TDIGEST_DOUBLE = ARRAY(TDIGEST(DOUBLE()));
  // Default compression and weight
  // Digest 1 has one value of 0.1
  const std::string digest1String = decodeBase64(
      "AQCamZmZmZm5P5qZmZmZmbk/mpmZmZmZuT8AAAAAAABZQAAAAAAAAPA/AQAAAAAAAAAAAPA/mpmZmZmZuT8=");
  // Digest 2 has one value of 0.2
  const std::string digest2String = decodeBase64(
      "AQCamZmZmZnJP5qZmZmZmck/mpmZmZmZyT8AAAAAAABZQAAAAAAAAPA/AQAAAAAAAAAAAPA/mpmZmZmZyT8=");
  const std::string digest12String = decodeBase64(
      "AQCamZmZmZm5P5qZmZmZmck/NDMzMzMz0z8AAAAAAABZQAAAAAAAAABAAgAAAAAAAAAAAPA/AAAAAAAA8D+amZmZmZm5P5qZmZmZmck/");
  // Digest 12 Scaled By 2
  const std::string digest12Scale2String = decodeBase64(
      "AQCamZmZmZm5P5qZmZmZmck/NDMzMzMz0z8AAAAAAABZQAAAAAAAABBAAgAAAAAAAAAAAABAAAAAAAAAAECamZmZmZm5P5qZmZmZmck/");
};

TEST_F(TDigestFunctionsTest, valueAtQuantile) {
  const auto valueAtQuantile = [&](const std::optional<std::string>& input,
                                   const std::optional<double>& quantile) {
    return evaluateOnce<double>(
        "value_at_quantile(c0, c1)", TDIGEST_DOUBLE, input, quantile);
  };
  const std::string input = decodeBase64(
      "AQAAAAAAAADwPwAAAAAAABRAAAAAAAAALkAAAAAAAABZQAAAAAAAABRABQAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRA");
  ASSERT_EQ(1.0, valueAtQuantile(input, 0.1));
  ASSERT_EQ(3.0, valueAtQuantile(input, 0.5));
  ASSERT_EQ(5.0, valueAtQuantile(input, 0.9));
  ASSERT_EQ(5.0, valueAtQuantile(input, 0.99));
};

TEST_F(TDigestFunctionsTest, valueAtQuantileOutOfRange) {
  const auto valueAtQuantile = [&](const std::optional<std::string>& input,
                                   const std::optional<double>& quantile) {
    return evaluateOnce<double>(
        "value_at_quantile(c0, c1)", TDIGEST_DOUBLE, input, quantile);
  };
  const std::string input = decodeBase64(
      "AQAAAAAAAADwPwAAAAAAABRAAAAAAAAALkAAAAAAAABZQAAAAAAAABRABQAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRA");
  EXPECT_THROW(valueAtQuantile(input, -1), VeloxUserError);
  EXPECT_THROW(valueAtQuantile(input, 1.1), VeloxUserError);
};

TEST_F(TDigestFunctionsTest, valuesAtQuantiles) {
  const std::string input = decodeBase64(
      "AQAAAAAAAADwPwAAAAAAABRAAAAAAAAALkAAAAAAAABZQAAAAAAAABRABQAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRA");
  auto arg0 = makeFlatVector<std::string>({input}, TDIGEST_DOUBLE);
  auto arg1 = makeNullableArrayVector<double>({{0.1, 0.5, 0.9, 0.99}});
  auto expected = makeNullableArrayVector<double>({{1.0, 3.0, 5.0, 5.0}});
  auto result =
      evaluate("values_at_quantiles(c0, c1)", makeRowVector({arg0, arg1}));
  test::assertEqualVectors(expected, result);
}

TEST_F(TDigestFunctionsTest, testMergeTDigestNullInput) {
  auto arg0 = makeNullableArrayVector<std::string>(
      {std::nullopt}, ARRAY_TDIGEST_DOUBLE);
  auto expected =
      makeNullableFlatVector<std::string>({std::nullopt}, TDIGEST_DOUBLE);
  auto result = evaluate("merge_tdigest(c0)", makeRowVector({arg0}));
  test::assertEqualVectors(expected, result);
}

TEST_F(TDigestFunctionsTest, testMergeTDigestEmptyArray) {
  auto arg0 =
      vectorMaker_.arrayVectorNullable<std::string>({{}}, ARRAY_TDIGEST_DOUBLE);
  auto expected =
      makeNullableFlatVector<std::string>({std::nullopt}, TDIGEST_DOUBLE);
  auto result = evaluate("merge_tdigest(c0)", makeRowVector({arg0}));
  test::assertEqualVectors(expected, result);
}

TEST_F(TDigestFunctionsTest, testMergeTDigestEmptyArrayOfNull) {
  auto arg0 = vectorMaker_.arrayVectorNullable<std::string>(
      {{std::nullopt}}, ARRAY_TDIGEST_DOUBLE);
  auto expected =
      makeNullableFlatVector<std::string>({std::nullopt}, TDIGEST_DOUBLE);
  auto result = evaluate("merge_tdigest(c0)", makeRowVector({arg0}));
  test::assertEqualVectors(expected, result);
}

TEST_F(TDigestFunctionsTest, testMergeTDigestEmptyArrayOfNulls) {
  auto arg0 = vectorMaker_.arrayVectorNullable<std::string>(
      {{{std::nullopt, std::nullopt, std::nullopt}}}, ARRAY_TDIGEST_DOUBLE);
  auto expected =
      makeNullableFlatVector<std::string>({std::nullopt}, TDIGEST_DOUBLE);
  auto result = evaluate("merge_tdigest(c0)", makeRowVector({arg0}));
  test::assertEqualVectors(expected, result);
}

TEST_F(TDigestFunctionsTest, testMergeTDigests) {
  auto arg0 = makeNullableArrayVector<std::string>(
      {{digest1String, digest2String}}, ARRAY_TDIGEST_DOUBLE);
  auto expected =
      makeNullableFlatVector<std::string>({digest12String}, TDIGEST_DOUBLE);
  auto result = evaluate("merge_tdigest(c0)", makeRowVector({arg0}));
  test::assertEqualVectors(expected, result);
}

TEST_F(TDigestFunctionsTest, testMergeTDigestOneNull) {
  auto arg0 = makeNullableArrayVector<std::string>(
      {{digest1String, std::nullopt}}, ARRAY_TDIGEST_DOUBLE);
  auto expected =
      makeNullableFlatVector<std::string>({digest1String}, TDIGEST_DOUBLE);
  auto result = evaluate("merge_tdigest(c0)", makeRowVector({arg0}));
  test::assertEqualVectors(expected, result);
}

TEST_F(TDigestFunctionsTest, testMergeTDigestOneNullFirst) {
  auto arg0 = makeNullableArrayVector<std::string>(
      {{std::nullopt, digest1String, digest2String}}, ARRAY_TDIGEST_DOUBLE);
  auto expected =
      makeNullableFlatVector<std::string>({digest12String}, TDIGEST_DOUBLE);
  auto result = evaluate("merge_tdigest(c0)", makeRowVector({arg0}));
  test::assertEqualVectors(expected, result);
}

TEST_F(TDigestFunctionsTest, testMergeTDigestOneNullMiddle) {
  auto arg0 = makeNullableArrayVector<std::string>(
      {{digest1String, std::nullopt, digest2String}}, ARRAY_TDIGEST_DOUBLE);
  auto expected =
      makeNullableFlatVector<std::string>({digest12String}, TDIGEST_DOUBLE);
  auto result = evaluate("merge_tdigest(c0)", makeRowVector({arg0}));
  test::assertEqualVectors(expected, result);
}

TEST_F(TDigestFunctionsTest, testScale) {
  // Original TDigest
  auto arg0 =
      makeNullableFlatVector<std::string>({digest12String}, TDIGEST_DOUBLE);
  auto scaleUpArg0 = makeNullableFlatVector<std::string>(
      {digest12Scale2String}, TDIGEST_DOUBLE);
  // Scale up by 2
  auto scaleUpResult =
      evaluate("scale_tdigest(c0, 2.0)", makeRowVector({arg0}));
  test::assertEqualVectors(scaleUpResult, scaleUpResult);
  // Scale down by 0.5
  auto scaleDownResult =
      evaluate("scale_tdigest(c0, 0.5)", makeRowVector({scaleUpResult}));
  test::assertEqualVectors(arg0, scaleDownResult);
}

TEST_F(TDigestFunctionsTest, testScaleNegative) {
  const TypePtr type = TDIGEST(DOUBLE());
  const auto scaleTDigest = [&](const std::optional<std::string>& input,
                                const std::optional<double>& scale) {
    return evaluateOnce<double>("scale_tdigest(c0, c1)", type, input, scale);
  };
  const std::string input = decodeBase64(
      "AQAAAAAAAADwPwAAAAAAABRAAAAAAAAALkAAAAAAAABZQAAAAAAAABRABQAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRA");
  VELOX_ASSERT_THROW(
      scaleTDigest(input, -1.0), "Scale factor should be positive.");
}
