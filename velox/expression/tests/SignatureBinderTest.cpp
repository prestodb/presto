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
#include "velox/expression/SignatureBinder.h"
#include <gtest/gtest.h>

using namespace facebook::velox;

void testSignatureBinder(
    const std::shared_ptr<exec::FunctionSignature>& signature,
    const std::vector<TypePtr>& actualTypes,
    const TypePtr& expectedReturnType) {
  exec::SignatureBinder binder(*signature, actualTypes);
  ASSERT_TRUE(binder.tryBind());

  auto returnType = binder.tryResolveReturnType();
  ASSERT_TRUE(returnType != nullptr);
  ASSERT_TRUE(expectedReturnType->kindEquals(returnType));
}

void assertCannotResolve(
    const std::shared_ptr<exec::FunctionSignature>& signature,
    const std::vector<TypePtr>& actualTypes) {
  exec::SignatureBinder binder(*signature, actualTypes);
  ASSERT_FALSE(binder.tryBind());
}

TEST(SignatureBinderTest, generics) {
  // array(T), T -> boolean
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("T")
                         .returnType("boolean")
                         .argumentType("array(T)")
                         .argumentType("T")
                         .build();

    testSignatureBinder(signature, {ARRAY(BIGINT()), BIGINT()}, BOOLEAN());
    testSignatureBinder(
        signature, {ARRAY(DECIMAL(20, 3)), DECIMAL(20, 3)}, BOOLEAN());
    assertCannotResolve(signature, {ARRAY(DECIMAL(20, 3)), DECIMAL(20, 4)});
    testSignatureBinder(
        signature,
        {ARRAY(FIXED_SIZE_ARRAY(20, BIGINT())), FIXED_SIZE_ARRAY(20, BIGINT())},
        BOOLEAN());
    assertCannotResolve(
        signature,
        {ARRAY(FIXED_SIZE_ARRAY(20, BIGINT())),
         FIXED_SIZE_ARRAY(10, BIGINT())});
  }

  // array(array(T)), array(T) -> boolean
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("T")
                         .returnType("boolean")
                         .argumentType("array(array(T))")
                         .argumentType("array(T)")
                         .build();

    testSignatureBinder(
        signature, {ARRAY(ARRAY(BIGINT())), ARRAY(BIGINT())}, BOOLEAN());
  }

  // map(K,V) -> array(K)
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("K")
                         .typeVariable("V")
                         .returnType("array(K)")
                         .argumentType("map(K,V)")
                         .build();

    testSignatureBinder(signature, {MAP(BIGINT(), DOUBLE())}, ARRAY(BIGINT()));
  }

  // map(K,V) -> array(V)
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .typeVariable("K")
                         .typeVariable("V")
                         .returnType("array(V)")
                         .argumentType("map(K,V)")
                         .build();

    testSignatureBinder(signature, {MAP(BIGINT(), DOUBLE())}, ARRAY(DOUBLE()));
  }
}

TEST(SignatureBinderTest, variableArity) {
  // varchar... -> varchar
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .returnType("varchar")
                         .argumentType("varchar")
                         .variableArity()
                         .build();

    testSignatureBinder(signature, {}, VARCHAR());
    testSignatureBinder(signature, {VARCHAR()}, VARCHAR());
    testSignatureBinder(signature, {VARCHAR(), VARCHAR()}, VARCHAR());
    testSignatureBinder(
        signature, {VARCHAR(), VARCHAR(), VARCHAR()}, VARCHAR());
  }

  // integer, double... -> varchar
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .returnType("varchar")
                         .argumentType("integer")
                         .argumentType("double")
                         .variableArity()
                         .build();

    testSignatureBinder(signature, {INTEGER()}, VARCHAR());
    testSignatureBinder(signature, {INTEGER(), DOUBLE()}, VARCHAR());
    testSignatureBinder(signature, {INTEGER(), DOUBLE(), DOUBLE()}, VARCHAR());
    testSignatureBinder(
        signature, {INTEGER(), DOUBLE(), DOUBLE(), DOUBLE()}, VARCHAR());
  }

  // any... -> varchar
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .returnType("varchar")
                         .argumentType("any")
                         .variableArity()
                         .build();

    testSignatureBinder(signature, {}, VARCHAR());
    testSignatureBinder(signature, {INTEGER()}, VARCHAR());
    testSignatureBinder(signature, {INTEGER(), DOUBLE()}, VARCHAR());
    testSignatureBinder(
        signature, {TIMESTAMP(), VARCHAR(), SMALLINT()}, VARCHAR());
  }

  // integer, any... -> timestamp
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .returnType("timestamp")
                         .argumentType("integer")
                         .argumentType("any")
                         .variableArity()
                         .build();

    testSignatureBinder(signature, {INTEGER()}, TIMESTAMP());
    testSignatureBinder(signature, {INTEGER(), DOUBLE()}, TIMESTAMP());
    testSignatureBinder(
        signature,
        {INTEGER(), TIMESTAMP(), VARCHAR(), SMALLINT()},
        TIMESTAMP());
  }
}

TEST(SignatureBinderTest, unresolvable) {
  // integer -> varchar
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .returnType("varchar")
                         .argumentType("integer")
                         .build();

    // wrong type
    assertCannotResolve(signature, {BIGINT()});

    // wrong number of arguments
    assertCannotResolve(signature, {});
    assertCannotResolve(signature, {INTEGER(), DOUBLE()});
  }

  // integer, double -> varchar
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .returnType("varchar")
                         .argumentType("integer")
                         .argumentType("double")
                         .build();

    // wrong type
    assertCannotResolve(signature, {BIGINT()});
    assertCannotResolve(signature, {INTEGER(), INTEGER()});
    assertCannotResolve(signature, {INTEGER(), DOUBLE(), INTEGER()});

    // wrong number of arguments
    assertCannotResolve(signature, {});
  }

  // integer... -> varchar
  {
    auto signature = exec::FunctionSignatureBuilder()
                         .returnType("varchar")
                         .argumentType("integer")
                         .variableArity()
                         .build();

    // wrong type
    assertCannotResolve(signature, {BIGINT()});
    assertCannotResolve(signature, {INTEGER(), BIGINT()});
  }
}

TEST(SignatureBinderTest, tryResolveTypeNullOutput) {
  auto assertNullResult = [&](const std::string& argument) {
    ASSERT_EQ(
        exec::SignatureBinder::tryResolveType(
            exec::parseTypeSignature(argument), {}),
        nullptr);
  };

  assertNullResult("T");
  assertNullResult("any");
  assertNullResult("array(T)");
  assertNullResult("array(any)");
  assertNullResult("map(int, T)");
  assertNullResult("row(int, T)");
}
