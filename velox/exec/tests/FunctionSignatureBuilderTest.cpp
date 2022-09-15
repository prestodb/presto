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
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

class FunctionSignatureBuilderTest : public functions::test::FunctionBaseTest {
};

TEST_F(FunctionSignatureBuilderTest, basicTypeTests) {
  assertUserInvalidArgument(
      [=]() {
        FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("integer")
            .argumentType("integer")
            .build();
      },
      "Not all type parameters used");

  // Duplicate type params.
  assertUserInvalidArgument(
      [=]() {
        FunctionSignatureBuilder()
            .typeVariable("T")
            .typeVariable("T")
            .returnType("integer")
            .argumentType("array(T)")
            .build();
      },
      "Type parameter declared twice");

  // Unspecified type params.
  assertUserInvalidArgument(
      [=]() {
        FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("integer")
            .argumentType("array(M)")
            .build();
      },
      "Specified element is not found : M");

  // Only supported types.
  assertUserInvalidArgument(
      [=]() {
        FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("nosuchtype")
            .argumentType("array(T)")
            .build();
      },
      "Specified element is not found : NOSUCHTYPE");

  // Any Type.
  EXPECT_TRUE(
      FunctionSignatureBuilder()
          .typeVariable("T")
          .typeVariable("M")
          .returnType("Array(M)")
          .argumentType("Map(Map(Array(T)), Any)")
          .build() != nullptr);
}

TEST_F(FunctionSignatureBuilderTest, typeParamTests) {
  // Any type with argument throws exception.
  assertUserInvalidArgument(
      [=]() {
        FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("integer")
            .argumentType("Any(T)")
            .build();
      },
      "Type 'Any' cannot have parameters");

  // Variable Arity in argument fails.
  assertUserInvalidArgument(
      [=]() {
        FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("integer")
            .argumentType("row(T ..., varchar)")
            .build();
      },
      "Specified element is not found : T ...");

  // Type params cant have type params.
  assertUserInvalidArgument(
      [=]() {
        FunctionSignatureBuilder()
            .typeVariable("T")
            .typeVariable("M")
            .returnType("integer")
            .argumentType("T(M)")
            .build();
      },
      "Named type cannot have parameters : T(M)");
}
