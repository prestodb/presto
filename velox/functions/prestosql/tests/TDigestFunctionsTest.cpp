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

 protected:
  std::string decodeBase64(std::string_view input) {
    std::string decoded(folly::base64DecodedSize(input), '\0');
    folly::base64Decode(input, decoded.data());
    return decoded;
  }
};

TEST_F(TDigestFunctionsTest, valueAtQuantile) {
  const TypePtr type = TDIGEST(DOUBLE());
  const auto valueAtQuantile = [&](const std::optional<std::string>& input,
                                   const std::optional<double>& quantile) {
    return evaluateOnce<double>(
        "value_at_quantile(c0, c1)", type, input, quantile);
  };
  const std::string input = decodeBase64(
      "AQAAAAAAAADwPwAAAAAAABRAAAAAAAAALkAAAAAAAABZQAAAAAAAABRABQAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRA");
  ASSERT_EQ(1.0, valueAtQuantile(input, 0.1));
  ASSERT_EQ(3.0, valueAtQuantile(input, 0.5));
  ASSERT_EQ(5.0, valueAtQuantile(input, 0.9));
  ASSERT_EQ(5.0, valueAtQuantile(input, 0.99));
};

TEST_F(TDigestFunctionsTest, valuesAtQuantiles) {
  const TypePtr type = TDIGEST(DOUBLE());
  const std::string input = decodeBase64(
      "AQAAAAAAAADwPwAAAAAAABRAAAAAAAAALkAAAAAAAABZQAAAAAAAABRABQAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRA");
  VectorPtr arg0 = makeFlatVector<std::string>({input}, type);
  VectorPtr arg1 = makeNullableArrayVector<double>({{0.1, 0.5, 0.9, 0.99}});
  auto expected = makeNullableArrayVector<double>({{1.0, 3.0, 5.0, 5.0}});
  auto result =
      evaluate("values_at_quantiles(c0, c1)", makeRowVector({arg0, arg1}));
  test::assertEqualVectors(expected, result);
}
