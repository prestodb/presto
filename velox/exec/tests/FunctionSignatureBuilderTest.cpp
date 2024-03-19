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
      "Type doesn't exist: 'M'");

  // Only supported types.
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("nosuchtype")
          .argumentType("array(T)")
          .build(),
      "Type doesn't exist: 'NOSUCHTYPE'");

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
      "Failed to parse type signature [Any(T)]: syntax error, unexpected LPAREN, expecting YYEOF");

  // Variable Arity in argument fails.
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("integer")
          .argumentType("row(..., varchar)")
          .build(),
      "Failed to parse type signature [row(..., varchar)]: syntax error, unexpected COMMA");

  // Type params cant have type params.
  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .typeVariable("M")
          .returnType("integer")
          .argumentType("T(M)")
          .build(),
      "Failed to parse type signature [T(M)]: syntax error, unexpected LPAREN, expecting YYEOF");
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
    EXPECT_EQ(
        "(T,constant bigint,T) -> array(T) -> T", aggSignature->toString());
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
        "(bigint,constant T,T,constant double...) -> array(T) -> T",
        aggSignature->toString());
  }
}

TEST_F(FunctionSignatureBuilderTest, toString) {
  auto signature = FunctionSignatureBuilder()
                       .returnType("bigint")
                       .argumentType("integer")
                       .build();

  ASSERT_EQ("(integer) -> bigint", toString({signature}));

  signature = FunctionSignatureBuilder()
                  .returnType("bigint")
                  .argumentType("varchar")
                  .argumentType("integer")
                  .build();

  ASSERT_EQ("(varchar,integer) -> bigint", toString({signature}));

  signature = AggregateFunctionSignatureBuilder()
                  .returnType("bigint")
                  .argumentType("varchar")
                  .intermediateType("varbinary")
                  .build();

  ASSERT_EQ("(varchar) -> varbinary -> bigint", toString({signature}));

  ASSERT_EQ("foo(BIGINT, VARCHAR)", toString("foo", {BIGINT(), VARCHAR()}));
}

TEST_F(FunctionSignatureBuilderTest, orderableComparable) {
  {
    auto signature = FunctionSignatureBuilder()
                         .typeVariable("T")
                         .returnType("array(T)")
                         .argumentType("array(T)")
                         .build();
    ASSERT_FALSE(signature->variables().at("T").orderableTypesOnly());
    ASSERT_FALSE(signature->variables().at("T").comparableTypesOnly());
  }

  {
    auto signature = FunctionSignatureBuilder()
                         .orderableTypeVariable("T")
                         .returnType("array(T)")
                         .argumentType("array(T)")
                         .build();
    ASSERT_TRUE(signature->variables().at("T").orderableTypesOnly());
    ASSERT_TRUE(signature->variables().at("T").comparableTypesOnly());
  }

  {
    auto signature = FunctionSignatureBuilder()
                         .comparableTypeVariable("T")
                         .returnType("array(T)")
                         .argumentType("array(T)")
                         .build();
    ASSERT_FALSE(signature->variables().at("T").orderableTypesOnly());
    ASSERT_TRUE(signature->variables().at("T").comparableTypesOnly());
  }

  VELOX_ASSERT_THROW(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .orderableTypeVariable("T")
          .returnType("array(T)")
          .argumentType("array(T)")
          .build(),
      "Variable T declared twice");
}

TEST_F(FunctionSignatureBuilderTest, orderableComparableAggregate) {
  {
    auto signature = exec::AggregateFunctionSignatureBuilder()
                         .typeVariable("T")
                         .returnType("T")
                         .intermediateType("T")
                         .argumentType("T")
                         .build();
    ASSERT_FALSE(signature->variables().at("T").orderableTypesOnly());
    ASSERT_FALSE(signature->variables().at("T").comparableTypesOnly());
  }

  {
    auto signature = exec::AggregateFunctionSignatureBuilder()
                         .orderableTypeVariable("T")
                         .returnType("T")
                         .intermediateType("T")
                         .argumentType("T")
                         .build();
    ASSERT_TRUE(signature->variables().at("T").orderableTypesOnly());
    ASSERT_TRUE(signature->variables().at("T").comparableTypesOnly());
  }

  {
    auto signature = exec::AggregateFunctionSignatureBuilder()
                         .comparableTypeVariable("T")
                         .returnType("T")
                         .intermediateType("T")
                         .argumentType("T")
                         .build();
    ASSERT_FALSE(signature->variables().at("T").orderableTypesOnly());
    ASSERT_TRUE(signature->variables().at("T").comparableTypesOnly());
  }

  VELOX_ASSERT_THROW(
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .comparableTypeVariable("T")
          .returnType("T")
          .intermediateType("T")
          .argumentType("T")
          .build(),
      "Variable T declared twice");
}

TEST_F(FunctionSignatureBuilderTest, allowVariablesForIntermediateType) {
  ASSERT_NO_THROW(
      exec::AggregateFunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("i_precision", "min(38, a_precision + 10)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .intermediateType("ROW(DECIMAL(i_precision, a_scale), BIGINT)")
          .returnType("DECIMAL(a_precision, a_scale)")
          .build());
}
