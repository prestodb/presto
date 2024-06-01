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
#include "velox/expression/fuzzer/tests/ArgGeneratorTestUtils.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/sparksql/fuzzer/AddSubtractArgGenerator.h"
#include "velox/functions/sparksql/fuzzer/DivideArgGenerator.h"
#include "velox/functions/sparksql/fuzzer/MakeTimestampArgGenerator.h"
#include "velox/functions/sparksql/fuzzer/MultiplyArgGenerator.h"
#include "velox/functions/sparksql/fuzzer/UnscaledValueArgGenerator.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::fuzzer::test;
namespace facebook::velox::functions::sparksql::test {
namespace {

class ArgGeneratorTest : public SparkFunctionBaseTest {
 protected:
  // Assert that the generated argument types meet user-specified check.
  void assertArgumentTypes(
      std::shared_ptr<velox::fuzzer::ArgGenerator> generator,
      const exec::FunctionSignature& signature,
      const TypePtr& returnType,
      std::function<void(const std::vector<TypePtr>&)> check) {
    std::mt19937 seed{0};
    const auto argTypes = generator->generateArgs(signature, returnType, seed);
    check(argTypes);
  }

  // Returns the only signature with specified return type for a given function
  // name.
  // @param returnType The name of expected return type defined in function
  // signature. Default is 'decimal'.
  const exec::FunctionSignature& getOnlySignature(
      const std::string& functionName,
      const std::string& returnType = "decimal") {
    const auto signatures = getSignatures(functionName, returnType);
    VELOX_CHECK_EQ(signatures.size(), 1);
    return *signatures[0];
  }
};

TEST_F(ArgGeneratorTest, add) {
  const auto& signature = getOnlySignature("add");
  const auto generator = std::make_shared<fuzzer::AddSubtractArgGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(18, 18));
  assertEmptyArgs(generator, signature, DECIMAL(38, 38));
}

TEST_F(ArgGeneratorTest, subtract) {
  const auto& signature = getOnlySignature("subtract");
  const auto generator = std::make_shared<fuzzer::AddSubtractArgGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(18, 18));
  assertEmptyArgs(generator, signature, DECIMAL(38, 38));
}

TEST_F(ArgGeneratorTest, multiply) {
  const auto& signature = getOnlySignature("multiply");
  const auto generator = std::make_shared<fuzzer::MultiplyArgGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(18, 18));
  assertEmptyArgs(generator, signature, DECIMAL(38, 38));
}

TEST_F(ArgGeneratorTest, divide) {
  const auto& signature = getOnlySignature("divide");
  const auto generator = std::make_shared<fuzzer::DivideArgGenerator>();

  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(18, 18));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertEmptyArgs(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(10, 2));
}

TEST_F(ArgGeneratorTest, makeTimestamp) {
  const auto signatures = getSignatures("make_timestamp", "timestamp");
  VELOX_CHECK_EQ(signatures.size(), 2);
  bool isSixArgs = signatures[0]->argumentTypes().size() == 6;
  const auto generator = std::make_shared<fuzzer::MakeTimestampArgGenerator>();

  std::function<void(const TypePtr&)> assertDecimalType =
      [](const TypePtr& type) {
        ASSERT_TRUE(type->isShortDecimal());
        auto [precision, scale] = getDecimalPrecisionScale(*type);
        ASSERT_EQ(scale, 6);
      };

  const auto& sixArgsSignature = isSixArgs ? *signatures[0] : *signatures[1];
  assertReturnType(generator, sixArgsSignature, TIMESTAMP());
  assertArgumentTypes(
      generator,
      sixArgsSignature,
      TIMESTAMP(),
      [&](const std::vector<TypePtr>& argumentTypes) {
        EXPECT_EQ(argumentTypes.size(), 6);
        assertDecimalType(argumentTypes[5]);
      });

  const auto& sevenArgsSignature = isSixArgs ? *signatures[1] : *signatures[0];
  assertReturnType(generator, sevenArgsSignature, TIMESTAMP());
  assertArgumentTypes(
      generator,
      sevenArgsSignature,
      TIMESTAMP(),
      [&](const std::vector<TypePtr>& argumentTypes) {
        EXPECT_EQ(argumentTypes.size(), 7);
        assertDecimalType(argumentTypes[5]);
      });
}

TEST_F(ArgGeneratorTest, unscaledValue) {
  const auto& signature = getOnlySignature("unscaled_value", "bigint");
  const auto generator = std::make_shared<fuzzer::UnscaledValueArgGenerator>();

  assertReturnType(generator, signature, BIGINT());
  assertArgumentTypes(
      generator,
      signature,
      BIGINT(),
      [](const std::vector<TypePtr>& argumentTypes) {
        EXPECT_EQ(argumentTypes.size(), 1);
        ASSERT_TRUE(argumentTypes[0]->isShortDecimal());
      });
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
