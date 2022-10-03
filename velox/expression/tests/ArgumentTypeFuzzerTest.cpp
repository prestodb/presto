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

#include "velox/expression/tests/ArgumentTypeFuzzer.h"

#include <gtest/gtest.h>

#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"

namespace facebook::velox::test {

class ArgumentTypeFuzzerTest : public testing::Test {
 protected:
  void testFuzzingSuccess(
      const std::shared_ptr<exec::FunctionSignature>& signature,
      const TypePtr& returnType,
      const std::vector<TypePtr>& expectedArgumentTypes) {
    std::mt19937 seed{0};
    ArgumentTypeFuzzer fuzzer{*signature, returnType, seed};
    ASSERT_TRUE(fuzzer.fuzzArgumentTypes());

    auto& argumentTypes = fuzzer.argumentTypes();
    ASSERT_LE(expectedArgumentTypes.size(), argumentTypes.size());

    auto& argumentSignatures = signature->argumentTypes();
    int i;
    for (i = 0; i < expectedArgumentTypes.size(); ++i) {
      ASSERT_TRUE(expectedArgumentTypes[i]->equivalent(*argumentTypes[i]))
          << "at " << i
          << ": Expected: " << expectedArgumentTypes[i]->toString()
          << ". Got: " << argumentTypes[i]->toString();
    }

    if (i < argumentTypes.size()) {
      ASSERT_TRUE(signature->variableArity());
      for (int j = i; j < argumentTypes.size(); ++j) {
        ASSERT_TRUE(argumentTypes[j]->equivalent(*argumentTypes[i - 1]));
      }
    }
  }

  void testFuzzingFailure(
      const std::shared_ptr<exec::FunctionSignature>& signature,
      const TypePtr& returnType) {
    std::mt19937 seed{0};
    ArgumentTypeFuzzer fuzzer{*signature, returnType, seed};
    ASSERT_FALSE(fuzzer.fuzzArgumentTypes());
  }
};

TEST_F(ArgumentTypeFuzzerTest, concreteSignature) {
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .returnType("bigint")
                         .argumentType("varchar")
                         .argumentType("array(boolean)")
                         .build();

    testFuzzingSuccess(signature, BIGINT(), {VARCHAR(), ARRAY(BOOLEAN())});
    testFuzzingFailure(signature, SMALLINT());
  }

  {
    auto signature = exec::FunctionSignatureBuilder()
                         .returnType("array(array(double))")
                         .argumentType("array(bigint)")
                         .argumentType("double")
                         .build();

    testFuzzingSuccess(
        signature, ARRAY(ARRAY(DOUBLE())), {ARRAY(BIGINT()), DOUBLE()});
    testFuzzingFailure(signature, ARRAY(ARRAY(REAL())));
    testFuzzingFailure(signature, ARRAY(DOUBLE()));
  }
}

TEST_F(ArgumentTypeFuzzerTest, signatureTemplate) {
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("T")
                         .returnType("T")
                         .argumentType("T")
                         .argumentType("array(T)")
                         .build();

    testFuzzingSuccess(signature, BIGINT(), {BIGINT(), ARRAY(BIGINT())});
    testFuzzingSuccess(
        signature, ARRAY(DOUBLE()), {ARRAY(DOUBLE()), ARRAY(ARRAY(DOUBLE()))});
    testFuzzingSuccess(
        signature, ROW({DOUBLE()}), {ROW({DOUBLE()}), ARRAY(ROW({DOUBLE()}))});
  }

  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("T")
                         .returnType("array(T)")
                         .argumentType("T")
                         .argumentType("bigint")
                         .build();

    testFuzzingSuccess(
        signature, ARRAY(ARRAY(DOUBLE())), {ARRAY(DOUBLE()), BIGINT()});
    testFuzzingSuccess(signature, ARRAY(VARCHAR()), {VARCHAR(), BIGINT()});
    testFuzzingFailure(signature, BIGINT());
    testFuzzingFailure(signature, MAP(VARCHAR(), BIGINT()));
  }

  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("K")
                         .typeVariable("V")
                         .returnType("K")
                         .argumentType("array(K)")
                         .argumentType("array(V)")
                         .build();

    auto verifyArgumentTypes = [&](const TypePtr& returnType,
                                   const TypePtr& firstArg) {
      std::mt19937 seed{0};
      ArgumentTypeFuzzer fuzzer{*signature, returnType, seed};
      ASSERT_TRUE(fuzzer.fuzzArgumentTypes());

      auto& argumentTypes = fuzzer.argumentTypes();
      ASSERT_TRUE(argumentTypes[0]->equivalent(*firstArg));
      // Only check the top-level type because the child type is randomly
      // generated.
      ASSERT_TRUE(argumentTypes[1]->isArray());
    };

    verifyArgumentTypes(SHORT_DECIMAL(10, 5), ARRAY(SHORT_DECIMAL(10, 5)));
    verifyArgumentTypes(ARRAY(DOUBLE()), ARRAY(ARRAY(DOUBLE())));
  }

  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("K")
                         .typeVariable("V")
                         .returnType("bigint")
                         .argumentType("array(K)")
                         .argumentType("array(V)")
                         .build();

    {
      std::mt19937 seed{0};
      ArgumentTypeFuzzer fuzzer{*signature, BIGINT(), seed};
      ASSERT_TRUE(fuzzer.fuzzArgumentTypes());

      auto& argumentTypes = fuzzer.argumentTypes();
      // Only check the top-level type because the children types are randomly
      // generated.
      ASSERT_TRUE(argumentTypes[0]->isArray());
      ASSERT_TRUE(argumentTypes[1]->isArray());
    }

    testFuzzingFailure(signature, DOUBLE());
  }
}

TEST_F(ArgumentTypeFuzzerTest, variableArity) {
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("X")
                         .returnType("X")
                         .argumentType("X")
                         .argumentType("bigint")
                         .variableArity()
                         .build();

    testFuzzingSuccess(signature, VARCHAR(), {VARCHAR(), BIGINT()});
  }

  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("K")
                         .returnType("bigint")
                         .argumentType("K")
                         .variableArity()
                         .build();
    std::mt19937 seed{0};
    ArgumentTypeFuzzer fuzzer{*signature, BIGINT(), seed};
    ASSERT_TRUE(fuzzer.fuzzArgumentTypes());

    auto& argumentTypes = fuzzer.argumentTypes();
    auto& firstArg = argumentTypes[0];
    for (const auto& argument : argumentTypes) {
      ASSERT_TRUE(argument->equivalent(*firstArg));
    }
  }
}

TEST_F(ArgumentTypeFuzzerTest, any) {
  auto signature = exec::FunctionSignatureBuilder()
                       .returnType("bigint")
                       .argumentType("any")
                       .build();
  std::mt19937 seed{0};
  ArgumentTypeFuzzer fuzzer{*signature, BIGINT(), seed};
  ASSERT_TRUE(fuzzer.fuzzArgumentTypes());

  auto& argumentTypes = fuzzer.argumentTypes();
  ASSERT_EQ(argumentTypes.size(), 1);
  ASSERT_TRUE(argumentTypes[0] != nullptr);
}

TEST_F(ArgumentTypeFuzzerTest, unsupported) {
  // Constraints on the return type is not supported.
  auto signature =
      exec::FunctionSignatureBuilder()
          .integerVariable("a_scale")
          .integerVariable("b_scale")
          .integerVariable("a_precision")
          .integerVariable("b_precision")
          .integerVariable(
              "r_precision",
              "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)")
          .integerVariable("r_scale", "max(a_scale, b_scale)")
          .returnType("row(array(decimal(r_precision, r_scale)))")
          .argumentType("decimal(a_precision, a_scale)")
          .argumentType("decimal(b_precision, b_scale)")
          .build();

  testFuzzingFailure(signature, DECIMAL(13, 6));
}

TEST_F(ArgumentTypeFuzzerTest, lambda) {
  // array(T), function(T, boolean) -> array(T)
  auto signature = exec::FunctionSignatureBuilder()
                       .typeVariable("T")
                       .returnType("array(T)")
                       .argumentType("array(T)")
                       .argumentType("function(T,boolean)")
                       .build();

  testFuzzingSuccess(
      signature,
      ARRAY(BIGINT()),
      {ARRAY(BIGINT()), FUNCTION(std::vector<TypePtr>{BIGINT()}, BOOLEAN())});

  // array(T), function(T, U) -> array(U)
  signature = exec::FunctionSignatureBuilder()
                  .typeVariable("T")
                  .typeVariable("U")
                  .returnType("array(U)")
                  .argumentType("array(T)")
                  .argumentType("function(T,U)")
                  .build();

  {
    std::mt19937 seed{0};
    ArgumentTypeFuzzer fuzzer{*signature, ARRAY(VARCHAR()), seed};
    ASSERT_TRUE(fuzzer.fuzzArgumentTypes());

    const auto& argumentTypes = fuzzer.argumentTypes();
    ASSERT_TRUE(argumentTypes[0]->isArray());

    // T is chosen randomly.
    auto randomType = argumentTypes[0]->asArray().elementType();
    ASSERT_EQ(*argumentTypes[1], *FUNCTION({randomType}, VARCHAR()));
  }

  // map(K, V1), function(K, V1, V2) -> map(K, V2)
  signature = exec::FunctionSignatureBuilder()
                  .typeVariable("K")
                  .typeVariable("V1")
                  .typeVariable("V2")
                  .returnType("map(K,V2)")
                  .argumentType("map(K,V1)")
                  .argumentType("function(K,V1,V2)")
                  .build();

  {
    std::mt19937 seed{0};
    ArgumentTypeFuzzer fuzzer{*signature, MAP(BIGINT(), VARCHAR()), seed};
    ASSERT_TRUE(fuzzer.fuzzArgumentTypes());

    const auto& argumentTypes = fuzzer.argumentTypes();
    ASSERT_TRUE(argumentTypes[0]->isMap());
    ASSERT_TRUE(argumentTypes[0]->asMap().keyType()->isBigint());

    // V1 is chosen randomly.
    auto randomType = argumentTypes[0]->asMap().valueType();
    ASSERT_EQ(*argumentTypes[1], *FUNCTION({BIGINT(), randomType}, VARCHAR()));
  }
}

} // namespace facebook::velox::test
