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

#include <gtest/gtest.h>

#include "velox/expression/tests/ExpressionFuzzer.h"

#include "velox/core/ITypedExpr.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::test {
class ExpressionFuzzerUnitTest : public testing::Test {
 protected:
  uint32_t countLevelOfNesting(core::TypedExprPtr expression) {
    if (expression->inputs().empty()) {
      return 0;
    }

    uint32_t maxLevelOfNesting = 1;
    for (const auto& child : expression->inputs()) {
      maxLevelOfNesting =
          std::max(maxLevelOfNesting, 1 + countLevelOfNesting(child));
    }
    return maxLevelOfNesting;
  }

  TypePtr randomType(std::mt19937& seed) {
    static std::vector<TypePtr> kSupportedTypes{
        BOOLEAN(),
        TINYINT(),
        SMALLINT(),
        INTEGER(),
        BIGINT(),
        REAL(),
        DOUBLE(),
        TIMESTAMP(),
        DATE(),
        INTERVAL_DAY_TIME()};
    auto index = folly::Random::rand32(kSupportedTypes.size(), seed);
    return kSupportedTypes[index];
  }
};

TEST_F(ExpressionFuzzerUnitTest, restrictedLevelOfNesting) {
  velox::functions::prestosql::registerAllScalarFunctions();
  std::mt19937 seed{0};

  auto testLevelOfNesting = [&](int32_t maxLevelOfNesting) {
    ExpressionFuzzer fuzzer{
        velox::getFunctionSignatures(), 0, maxLevelOfNesting};

    for (int i = 0; i < 5000; ++i) {
      auto expression = fuzzer.generateExpression(randomType(seed));
      EXPECT_LE(countLevelOfNesting(expression), std::max(1, maxLevelOfNesting))
          << fmt::format(
                 "Expression {} exceeds max level of nesting {} (original {})",
                 expression->toString(),
                 std::max(1, maxLevelOfNesting),
                 maxLevelOfNesting);
    }
  };

  testLevelOfNesting(10);

  testLevelOfNesting(5);

  testLevelOfNesting(2);

  testLevelOfNesting(1);

  testLevelOfNesting(0);

  testLevelOfNesting(-1);
}
} // namespace facebook::velox::test
