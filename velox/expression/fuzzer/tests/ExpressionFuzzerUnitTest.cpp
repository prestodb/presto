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

#include "velox/expression/fuzzer/ExpressionFuzzer.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::fuzzer::test {
class ExpressionFuzzerUnitTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

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
  std::shared_ptr<memory::MemoryPool> pool{
      memory::memoryManager()->addLeafPool()};

  std::shared_ptr<VectorFuzzer> vectorfuzzer{
      std::make_shared<VectorFuzzer>(VectorFuzzer::Options{}, pool.get())};
};

namespace {
auto makeOptionsWithMaxLevelNesting(int32_t value) {
  ExpressionFuzzer::Options options;
  options.maxLevelOfNesting = value;
  return options;
}
} // namespace
TEST_F(ExpressionFuzzerUnitTest, restrictedLevelOfNesting) {
  velox::functions::prestosql::registerAllScalarFunctions();
  std::mt19937 seed{0};

  auto testLevelOfNesting = [&](int32_t maxLevelOfNesting) {
    ExpressionFuzzer fuzzer{
        velox::getFunctionSignatures(),
        0,
        vectorfuzzer,
        makeOptionsWithMaxLevelNesting(maxLevelOfNesting),
    };

    for (int i = 0; i < 5000; ++i) {
      auto expression = fuzzer.fuzzExpression().expressions[0];
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

TEST_F(ExpressionFuzzerUnitTest, reproduceExpressionWithSeed) {
  velox::functions::prestosql::registerAllScalarFunctions();

  // Generate 10 random expressions with the same seed twice and verify they are
  // the same.
  auto generateExpressions = [&]() {
    std::vector<std::string> firstGeneration;
    std::mt19937 seed{7654321};
    ExpressionFuzzer fuzzer{
        velox::getFunctionSignatures(),
        1234567,
        vectorfuzzer,
        makeOptionsWithMaxLevelNesting(5)};
    for (auto i = 0; i < 10; ++i) {
      firstGeneration.push_back(
          fuzzer.fuzzExpression().expressions[0]->toString());
    }
    return firstGeneration;
  };

  auto firstGeneration = generateExpressions();
  auto secondGeneration = generateExpressions();
  VELOX_CHECK_EQ(firstGeneration.size(), 10);
  VELOX_CHECK_EQ(secondGeneration.size(), 10);
  for (auto i = 0; i < 10; ++i) {
    VELOX_CHECK_EQ(firstGeneration[i], secondGeneration[i]);
  }
}

TEST_F(ExpressionFuzzerUnitTest, exprBank) {
  velox::functions::prestosql::registerAllScalarFunctions();
  std::mt19937 seed{0};
  int32_t maxLevelOfNesting = 10;
  {
    ExpressionFuzzer fuzzer{
        velox::getFunctionSignatures(),
        0,
        vectorfuzzer,
        makeOptionsWithMaxLevelNesting(maxLevelOfNesting)};
    ExpressionFuzzer::ExprBank exprBank(seed, maxLevelOfNesting);
    for (int i = 0; i < 5000; ++i) {
      auto expression = fuzzer.fuzzExpression().expressions[0];
      // Verify that if there is a single expression then it is returned
      // successfully.
      exprBank.insert(expression);
      auto returned =
          exprBank.getRandomExpression(expression->type(), maxLevelOfNesting);
      EXPECT_EQ(expression, returned);
      // Verify that if no expressions exist below a max level then no
      // expression is returned.
      if (countLevelOfNesting(expression) > 0) {
        returned = exprBank.getRandomExpression(
            expression->type(), countLevelOfNesting(expression) - 1);
        EXPECT_TRUE(!returned);
      }
      exprBank.reset();
    }
  }

  {
    // Verify that randomly selected expressions do not exceed the requested max
    // nesting level.
    ExpressionFuzzer fuzzer{
        velox::getFunctionSignatures(),
        0,
        vectorfuzzer,
        makeOptionsWithMaxLevelNesting(maxLevelOfNesting)};
    ExpressionFuzzer::ExprBank exprBank(seed, maxLevelOfNesting);
    for (int i = 0; i < 1000; ++i) {
      auto expression = fuzzer.fuzzExpression().expressions[0];
      exprBank.insert(expression);
    }

    auto testLevelOfNesting = [&](int32_t requestedLevelOfNesting) {
      for (int i = 0; i < 5000; ++i) {
        auto returnType = randomType(seed);
        auto returned =
            exprBank.getRandomExpression(returnType, requestedLevelOfNesting);
        if (!returned) {
          continue;
        }
        EXPECT_LE(countLevelOfNesting(returned), requestedLevelOfNesting)
            << fmt::format(
                   "Expression {} exceeds max level of nesting {}",
                   returned->toString(),
                   requestedLevelOfNesting);
      }
    };
    testLevelOfNesting(10);
    testLevelOfNesting(5);
    testLevelOfNesting(2);
    testLevelOfNesting(1);
  }
}

} // namespace facebook::velox::fuzzer::test
