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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

class FunctionSignatureBuilderTest : public functions::test::FunctionBaseTest {
};

TEST_F(FunctionSignatureBuilderTest, basicTypeTests) {
  // All type variables should be used in the inputs arguments.
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("integer")
          .argumentType("integer")
          .build(),
      "Some type variables are not used in the inputs");

  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("integer")
          .build(),
      "Some type variables are not used in the inputs");

  // Integer variables do not have to be used in the inputs, but in that case
  // must appear in the return.
  ASSERT_NO_THROW(FunctionSignatureBuilder()
                      .integerVariable("a")
                      .returnType("DECIMAL(a, a)")
                      .argumentType("integer")
                      .build(););

  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .integerVariable("a")
          .returnType("integer")
          .argumentType("integer")
          .build(),
      "Some integer variables are not used");

  // Duplicate type params.
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .typeVariable("T")
          .returnType("integer")
          .argumentType("array(T)")
          .build(),
      "Variable T declared twice");

  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .knownTypeVariable("T")
          .returnType("integer")
          .argumentType("array(T)")
          .build(),
      "Variable T declared twice");

  // Unspecified type params.
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("integer")
          .argumentType("T")
          .argumentType("array(M)")
          .build(),
      "Specified element is not found : M");

  // Only supported types.
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("nosuchtype")
          .argumentType("array(T)")
          .build(),
      "Specified element is not found : NOSUCHTYPE");

  // Any Type.
  EXPECT_TRUE(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .typeVariable("M")
          .returnType("Array(M)")
          .argumentType("Map(Map(Array(T), M), Any)")
          .build() != nullptr);
}

TEST_F(FunctionSignatureBuilderTest, typeParamTests) {
  // Any type with argument throws exception.
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("integer")
          .argumentType("Any(T)")
          .build(),
      "Type 'Any' cannot have parameters");

  // Variable Arity in argument fails.
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("integer")
          .argumentType("row(T ..., varchar)")
          .build(),
      "Specified element is not found : T ...");

  // Type params cant have type params.
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .typeVariable("M")
          .returnType("integer")
          .argumentType("T(M)")
          .build(),
      "Named type cannot have parameters : T(M)");
}

TEST_F(FunctionSignatureBuilderTest, anyInReturn) {
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("Any")
          .argumentType("T")
          .build(),
      "Type 'Any' cannot appear in return type");

  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("row(any, T)")
          .argumentType("T")
          .build(),
      "Type 'Any' cannot appear in return type");
}

TEST_F(FunctionSignatureBuilderTest, scalarConstantFlags) {
  {
    auto signature = FunctionSignatureBuilder()
                         .returnType("bigint")
                         .argumentType("double")
                         .constantArgumentType("boolean")
                         .argumentType("bigint")
                         .build();
    EXPECT_FALSE(signature->constantArguments().at(0));
    EXPECT_TRUE(signature->constantArguments().at(1));
    EXPECT_FALSE(signature->constantArguments().at(2));
    EXPECT_EQ(
        "(double,constant boolean,bigint) -> bigint", signature->toString());
  }

  {
    auto signature = FunctionSignatureBuilder()
                         .typeVariable("T")
                         .returnType("bigint")
                         .argumentType("double")
                         .constantArgumentType("T")
                         .argumentType("bigint")
                         .constantArgumentType("boolean")
                         .variableArity()
                         .build();
    EXPECT_FALSE(signature->constantArguments().at(0));
    EXPECT_TRUE(signature->constantArguments().at(1));
    EXPECT_FALSE(signature->constantArguments().at(2));
    EXPECT_EQ(
        "(double,constant T,bigint,constant boolean...) -> bigint",
        signature->toString());
  }
}

TEST_F(FunctionSignatureBuilderTest, aggregateConstantFlags) {
  {
    auto aggSignature = AggregateFunctionSignatureBuilder()
                            .typeVariable("T")
                            .returnType("T")
                            .intermediateType("array(T)")
                            .argumentType("T")
                            .constantArgumentType("bigint")
                            .argumentType("T")
                            .build();
    EXPECT_FALSE(aggSignature->constantArguments().at(0));
    EXPECT_TRUE(aggSignature->constantArguments().at(1));
    EXPECT_FALSE(aggSignature->constantArguments().at(2));
    EXPECT_EQ("(T,constant bigint,T) -> T", aggSignature->toString());
  }

  {
    auto aggSignature = AggregateFunctionSignatureBuilder()
                            .typeVariable("T")
                            .returnType("T")
                            .intermediateType("array(T)")
                            .argumentType("bigint")
                            .constantArgumentType("T")
                            .argumentType("T")
                            .constantArgumentType("double")
                            .variableArity()
                            .build();
    EXPECT_FALSE(aggSignature->constantArguments().at(0));
    EXPECT_TRUE(aggSignature->constantArguments().at(1));
    EXPECT_FALSE(aggSignature->constantArguments().at(2));
    EXPECT_EQ(
        "(bigint,constant T,T,constant double...) -> T",
        aggSignature->toString());
  }
}
