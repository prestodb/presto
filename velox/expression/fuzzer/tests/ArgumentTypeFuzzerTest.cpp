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

#include "velox/expression/fuzzer/ArgumentTypeFuzzer.h"

#include <gtest/gtest.h>

#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"

namespace facebook::velox::fuzzer::test {

namespace {
const uint32_t kMaxVariadicArgs = 5;
} // namespace

class ArgumentTypeFuzzerTest : public testing::Test {
 protected:
  void testFuzzingSuccess(
      const std::shared_ptr<exec::FunctionSignature>& signature,
      const TypePtr& returnType,
      const std::vector<TypePtr>& expectedArgumentTypes) {
    std::mt19937 seed{0};
    ArgumentTypeFuzzer fuzzer{*signature, returnType, seed};
    ASSERT_TRUE(fuzzer.fuzzArgumentTypes(kMaxVariadicArgs));

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
      ASSERT_LE(
          argumentTypes.size() - argumentSignatures.size(), kMaxVariadicArgs);
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
    ASSERT_FALSE(fuzzer.fuzzArgumentTypes(kMaxVariadicArgs));
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
                         .knownTypeVariable("K")
                         .typeVariable("V")
                         .returnType("K")
                         .argumentType("array(K)")
                         .argumentType("array(V)")
                         .build();

    auto verifyArgumentTypes = [&](const TypePtr& returnType,
                                   const TypePtr& firstArg) {
      std::mt19937 seed{0};
      ArgumentTypeFuzzer fuzzer{*signature, returnType, seed};
      ASSERT_TRUE(fuzzer.fuzzArgumentTypes(kMaxVariadicArgs));

      auto& argumentTypes = fuzzer.argumentTypes();
      ASSERT_TRUE(argumentTypes[0]->equivalent(*firstArg));
      // Only check the top-level type because the child type is randomly
      // generated.
      ASSERT_TRUE(argumentTypes[1]->isArray());
    };

    verifyArgumentTypes(DECIMAL(10, 5), ARRAY(DECIMAL(10, 5)));
    verifyArgumentTypes(ARRAY(DOUBLE()), ARRAY(ARRAY(DOUBLE())));
  }

  {
    auto signature = exec::FunctionSignatureBuilder()
                         .knownTypeVariable("K")
                         .typeVariable("V")
                         .returnType("bigint")
                         .argumentType("array(K)")
                         .argumentType("array(V)")
                         .build();

    {
      std::mt19937 seed{0};
      ArgumentTypeFuzzer fuzzer{*signature, BIGINT(), seed};
      ASSERT_TRUE(fuzzer.fuzzArgumentTypes(kMaxVariadicArgs));

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
                         .knownTypeVariable("K")
                         .returnType("bigint")
                         .argumentType("K")
                         .variableArity()
                         .build();
    std::mt19937 seed{0};
    ArgumentTypeFuzzer fuzzer{*signature, BIGINT(), seed};
    ASSERT_TRUE(fuzzer.fuzzArgumentTypes(kMaxVariadicArgs));

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
  ASSERT_TRUE(fuzzer.fuzzArgumentTypes(kMaxVariadicArgs));

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
    ASSERT_TRUE(fuzzer.fuzzArgumentTypes(kMaxVariadicArgs));

    const auto& argumentTypes = fuzzer.argumentTypes();
    ASSERT_TRUE(argumentTypes[0]->isArray());

    // T is chosen randomly.
    auto randomType = argumentTypes[0]->asArray().elementType();
    ASSERT_EQ(*argumentTypes[1], *FUNCTION({randomType}, VARCHAR()));
  }

  // map(K, V1), function(K, V1, V2) -> map(K, V2)
  signature = exec::FunctionSignatureBuilder()
                  .knownTypeVariable("K")
                  .typeVariable("V1")
                  .typeVariable("V2")
                  .returnType("map(K,V2)")
                  .argumentType("map(K,V1)")
                  .argumentType("function(K,V1,V2)")
                  .build();

  {
    std::mt19937 seed{0};
    ArgumentTypeFuzzer fuzzer{*signature, MAP(BIGINT(), VARCHAR()), seed};
    ASSERT_TRUE(fuzzer.fuzzArgumentTypes(kMaxVariadicArgs));

    const auto& argumentTypes = fuzzer.argumentTypes();
    ASSERT_TRUE(argumentTypes[0]->isMap());
    ASSERT_TRUE(argumentTypes[0]->asMap().keyType()->isBigint());

    // V1 is chosen randomly.
    auto randomType = argumentTypes[0]->asMap().valueType();
    ASSERT_EQ(*argumentTypes[1], *FUNCTION({BIGINT(), randomType}, VARCHAR()));
  }
}

TEST_F(ArgumentTypeFuzzerTest, unconstrainedSignatureTemplate) {
  auto signature = exec::FunctionSignatureBuilder()
                       .knownTypeVariable("K")
                       .typeVariable("V")
                       .returnType("V")
                       .argumentType("map(K,V)")
                       .argumentType("K")
                       .build();

  std::mt19937 seed{0};
  ArgumentTypeFuzzer fuzzer{*signature, MAP(BIGINT(), VARCHAR()), seed};
  ASSERT_TRUE(fuzzer.fuzzArgumentTypes(kMaxVariadicArgs));

  const auto& argumentTypes = fuzzer.argumentTypes();
  ASSERT_EQ(2, argumentTypes.size());
  ASSERT_TRUE(argumentTypes[0] != nullptr);
  ASSERT_TRUE(argumentTypes[1] != nullptr);

  ASSERT_EQ(argumentTypes[0]->kind(), TypeKind::MAP);

  ASSERT_EQ(argumentTypes[0]->childAt(0), argumentTypes[1]);
}

TEST_F(ArgumentTypeFuzzerTest, orderableConstraint) {
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .orderableTypeVariable("T")
                         .returnType("bigint")
                         .argumentType("T")
                         .build();

    for (size_t i = 0; i < 100; ++i) {
      std::mt19937 rng(i);
      ArgumentTypeFuzzer fuzzer{*signature, nullptr, rng};
      fuzzer.fuzzArgumentTypes(kMaxVariadicArgs);
      ASSERT_TRUE(fuzzer.argumentTypes()[0]->isOrderable())
          << fuzzer.argumentTypes()[0]->toString();
    }
  }

  {
    auto signature = exec::FunctionSignatureBuilder()
                         .orderableTypeVariable("T")
                         .returnType("T")
                         .argumentType("T")
                         .build();
    testFuzzingFailure(signature, MAP(VARCHAR(), BIGINT()));
    testFuzzingFailure(signature, ARRAY(MAP(VARCHAR(), BIGINT())));
  }

  {
    auto signature = exec::FunctionSignatureBuilder()
                         .orderableTypeVariable("T")
                         .returnType("array(T)")
                         .argumentType("array(T)")
                         .build();

    testFuzzingSuccess(signature, ARRAY(DOUBLE()), {ARRAY(DOUBLE())});
    testFuzzingSuccess(
        signature, ARRAY(ROW({DOUBLE()})), {ARRAY(ROW({DOUBLE()}))});
    testFuzzingFailure(signature, MAP(VARCHAR(), BIGINT()));
  }

  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("T")
                         .orderableTypeVariable("U")
                         .returnType("row(T, U)")
                         .argumentType("T")
                         .argumentType("row(T,U)")
                         .build();

    testFuzzingSuccess(
        signature,
        ROW({MAP(BIGINT(), BIGINT()), BIGINT()}),
        {MAP(BIGINT(), BIGINT()), ROW({MAP(BIGINT(), BIGINT()), BIGINT()})});

    testFuzzingFailure(signature, ROW({BIGINT(), MAP(VARCHAR(), BIGINT())}));
  }

  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("T")
                         .orderableTypeVariable("U")
                         .returnType("row(T,U)")
                         .argumentType("T")
                         .argumentType("function(T,U)")
                         .build();

    testFuzzingSuccess(
        signature,
        ROW({MAP(BIGINT(), BIGINT()), BIGINT()}),
        {MAP(BIGINT(), BIGINT()),
         std::make_shared<FunctionType>(
             std::vector<TypePtr>{MAP(BIGINT(), BIGINT())}, BIGINT())});

    testFuzzingFailure(signature, ROW({BIGINT(), MAP(VARCHAR(), BIGINT())}));
  }
}

TEST_F(ArgumentTypeFuzzerTest, fuzzDecimalArgumentTypes) {
  auto fuzzArgumentTypes = [](const exec::FunctionSignature& signature,
                              const TypePtr& returnType) {
    std::mt19937 seed{0};
    ArgumentTypeFuzzer fuzzer{signature, returnType, seed};
    bool ok = fuzzer.fuzzArgumentTypes(kMaxVariadicArgs);
    VELOX_CHECK(
        ok,
        "Signature: {}, Return type: {}",
        signature.toString(),
        returnType->toString());
    return fuzzer.argumentTypes();
  };

  // Argument type must match return type.
  auto signature = exec::FunctionSignatureBuilder()
                       .integerVariable("p")
                       .integerVariable("s")
                       .returnType("decimal(p,s)")
                       .argumentType("decimal(p,s)")
                       .build();

  auto argTypes = fuzzArgumentTypes(*signature, DECIMAL(10, 7));
  ASSERT_EQ(1, argTypes.size());
  EXPECT_EQ(DECIMAL(10, 7)->toString(), argTypes[0]->toString());

  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .integerVariable("s")
                  .returnType("decimal(p,s)")
                  .argumentType("decimal(p,s)")
                  .argumentType("decimal(p,s)")
                  .variableArity()
                  .build();
  argTypes = fuzzArgumentTypes(*signature, DECIMAL(10, 7));
  ASSERT_LE(1, argTypes.size());
  for (const auto& argType : argTypes) {
    EXPECT_EQ(DECIMAL(10, 7)->toString(), argType->toString());
  }

  // Argument type can be any decimal.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .integerVariable("s")
                  .returnType("boolean")
                  .argumentType("decimal(p,s)")
                  .build();

  argTypes = fuzzArgumentTypes(*signature, BOOLEAN());
  ASSERT_EQ(1, argTypes.size());
  EXPECT_TRUE(argTypes[0]->isDecimal());

  // Argument type can be any decimal with scale 30.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .returnType("boolean")
                  .argumentType("decimal(p,30)")
                  .build();

  argTypes = fuzzArgumentTypes(*signature, BOOLEAN());
  ASSERT_EQ(1, argTypes.size());
  EXPECT_TRUE(argTypes[0]->isDecimal());
  EXPECT_EQ(30, getDecimalPrecisionScale(*argTypes[0]).second);

  // Another way to specify fixed scale.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .integerVariable("s", "3")
                  .returnType("boolean")
                  .argumentType("decimal(p,s)")
                  .build();

  argTypes = fuzzArgumentTypes(*signature, BOOLEAN());
  ASSERT_EQ(1, argTypes.size());
  EXPECT_TRUE(argTypes[0]->isDecimal());
  EXPECT_EQ(3, getDecimalPrecisionScale(*argTypes[0]).second);

  // Argument type can be any decimal with precision 3.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("s")
                  .returnType("boolean")
                  .argumentType("decimal(3,s)")
                  .build();

  argTypes = fuzzArgumentTypes(*signature, BOOLEAN());
  ASSERT_EQ(1, argTypes.size());
  EXPECT_TRUE(argTypes[0]->isDecimal());
  EXPECT_EQ(3, getDecimalPrecisionScale(*argTypes[0]).first);

  // Another way to specify fixed precision.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p", "30")
                  .integerVariable("s")
                  .returnType("boolean")
                  .argumentType("decimal(p,s)")
                  .build();

  argTypes = fuzzArgumentTypes(*signature, BOOLEAN());
  ASSERT_EQ(1, argTypes.size());
  EXPECT_TRUE(argTypes[0]->isDecimal());
  EXPECT_EQ(30, getDecimalPrecisionScale(*argTypes[0]).first);

  // Multiple arguments. All must be the same as return type.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .integerVariable("s")
                  .returnType("decimal(p,s)")
                  .argumentType("decimal(p,s)")
                  .argumentType("decimal(p,s)")
                  .argumentType("decimal(p,s)")
                  .build();

  argTypes = fuzzArgumentTypes(*signature, DECIMAL(10, 7));
  ASSERT_EQ(3, argTypes.size());
  EXPECT_EQ(DECIMAL(10, 7)->toString(), argTypes[0]->toString());
  EXPECT_EQ(DECIMAL(10, 7)->toString(), argTypes[1]->toString());
  EXPECT_EQ(DECIMAL(10, 7)->toString(), argTypes[2]->toString());

  // Multiple arguments. Some have fixed precision, scale or both.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .integerVariable("s")
                  .returnType("decimal(p,s)")
                  .argumentType("decimal(p,s)")
                  .argumentType("decimal(p,10)")
                  .argumentType("decimal(12,s)")
                  .argumentType("decimal(2,1)")
                  .build();

  argTypes = fuzzArgumentTypes(*signature, DECIMAL(10, 7));
  ASSERT_EQ(4, argTypes.size());
  EXPECT_EQ(DECIMAL(10, 7)->toString(), argTypes[0]->toString());
  EXPECT_EQ(DECIMAL(10, 10)->toString(), argTypes[1]->toString());
  EXPECT_EQ(DECIMAL(12, 7)->toString(), argTypes[2]->toString());
  EXPECT_EQ(DECIMAL(2, 1)->toString(), argTypes[3]->toString());

  // Multiple pairs of precision and scale variables.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p1")
                  .integerVariable("s1")
                  .integerVariable("p2")
                  .integerVariable("s2")
                  .returnType("bigint")
                  .argumentType("decimal(p1,s1)")
                  .argumentType("decimal(p2,s2)")
                  .argumentType("decimal(p1,s1)")
                  .argumentType("decimal(p2,s2)")
                  .build();
  argTypes = fuzzArgumentTypes(*signature, BIGINT());
  ASSERT_EQ(4, argTypes.size());
  EXPECT_TRUE(argTypes[0]->isDecimal());
  EXPECT_TRUE(argTypes[1]->isDecimal());
  EXPECT_EQ(argTypes[0]->toString(), argTypes[2]->toString());
  EXPECT_EQ(argTypes[1]->toString(), argTypes[3]->toString());
}

TEST_F(ArgumentTypeFuzzerTest, fuzzDecimalReturnType) {
  auto fuzzReturnType = [](const exec::FunctionSignature& signature) {
    std::mt19937 seed{0};
    ArgumentTypeFuzzer fuzzer{signature, seed};
    return fuzzer.fuzzReturnType();
  };

  // Return type can be any decimal.
  auto signature = exec::FunctionSignatureBuilder()
                       .integerVariable("p")
                       .integerVariable("s")
                       .returnType("decimal(p,s)")
                       .argumentType("decimal(p,s)")
                       .build();

  auto returnType = fuzzReturnType(*signature);
  EXPECT_TRUE(returnType->isDecimal());

  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .integerVariable("s")
                  .returnType("decimal(p,s)")
                  .argumentType("decimal(p,s)")
                  .argumentType("decimal(p,s)")
                  .variableArity()
                  .build();

  returnType = fuzzReturnType(*signature);
  EXPECT_TRUE(returnType->isDecimal());

  // Return type can be any decimal with scale 3.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .integerVariable("s")
                  .returnType("decimal(p,3)")
                  .argumentType("decimal(p,s)")
                  .build();

  returnType = fuzzReturnType(*signature);
  EXPECT_TRUE(returnType->isDecimal());
  EXPECT_EQ(3, getDecimalPrecisionScale(*returnType).second);

  // Another way to specify that scale must be 3.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .integerVariable("s", "3")
                  .returnType("decimal(p,s)")
                  .argumentType("decimal(p,s)")
                  .build();

  returnType = fuzzReturnType(*signature);
  EXPECT_TRUE(returnType->isDecimal());
  EXPECT_EQ(3, getDecimalPrecisionScale(*returnType).second);

  // Return type can be any decimal with precision 22.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .integerVariable("s")
                  .returnType("decimal(22,s)")
                  .argumentType("decimal(p,s)")
                  .build();

  returnType = fuzzReturnType(*signature);
  EXPECT_TRUE(returnType->isDecimal());
  EXPECT_EQ(22, getDecimalPrecisionScale(*returnType).first);

  // Another way to specify that precision must be 22.
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p", "22")
                  .integerVariable("s")
                  .returnType("decimal(p,s)")
                  .argumentType("decimal(p,s)")
                  .build();

  returnType = fuzzReturnType(*signature);
  EXPECT_TRUE(returnType->isDecimal());
  EXPECT_EQ(22, getDecimalPrecisionScale(*returnType).first);

  // Return type can only be DECIMAL(10, 7).
  signature = exec::FunctionSignatureBuilder()
                  .integerVariable("p")
                  .integerVariable("s")
                  .returnType("decimal(10,7)")
                  .argumentType("decimal(p,s)")
                  .build();

  returnType = fuzzReturnType(*signature);
  EXPECT_EQ(DECIMAL(10, 7)->toString(), returnType->toString());
}

} // namespace facebook::velox::fuzzer::test
