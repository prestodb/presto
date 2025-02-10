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
#include "velox/expression/fuzzer/tests/ArgTypesGeneratorTestUtils.h"
#include "velox/functions/prestosql/fuzzer/DivideArgTypesGenerator.h"
#include "velox/functions/prestosql/fuzzer/FloorAndRoundArgTypesGenerator.h"
#include "velox/functions/prestosql/fuzzer/ModulusArgTypesGenerator.h"
#include "velox/functions/prestosql/fuzzer/MultiplyArgTypesGenerator.h"
#include "velox/functions/prestosql/fuzzer/PlusMinusArgTypesGenerator.h"
#include "velox/functions/prestosql/fuzzer/TruncateArgTypesGenerator.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::fuzzer::test;
namespace {

class ArgTypesGeneratorTest : public functions::test::FunctionBaseTest {
 protected:
  // Returns the only signature with decimal return type for a given function
  // name.
  const exec::FunctionSignature& getOnlySignature(
      const std::string& functionName) {
    const auto signatures = getSignatures(functionName, "decimal");
    VELOX_CHECK_EQ(signatures.size(), 1);
    return *signatures[0];
  }
};

TEST_F(ArgTypesGeneratorTest, plus) {
  const auto& signature = getOnlySignature("plus");
  const auto generator =
      std::make_shared<exec::test::PlusMinusArgTypesGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertReturnType(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(18, 18));
}

TEST_F(ArgTypesGeneratorTest, minus) {
  const auto& signature = getOnlySignature("minus");
  const auto generator =
      std::make_shared<exec::test::PlusMinusArgTypesGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertReturnType(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(18, 18));
}

TEST_F(ArgTypesGeneratorTest, multiply) {
  const auto& signature = getOnlySignature("multiply");
  const auto generator =
      std::make_shared<exec::test::MultiplyArgTypesGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(18, 18));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertReturnType(generator, signature, DECIMAL(38, 0));
}

TEST_F(ArgTypesGeneratorTest, divide) {
  const auto& signature = getOnlySignature("divide");
  const auto generator =
      std::make_shared<exec::test::DivideArgTypesGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(18, 18));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertReturnType(generator, signature, DECIMAL(38, 0));
}

TEST_F(ArgTypesGeneratorTest, floor) {
  const auto& signature = getOnlySignature("floor");
  const auto generator =
      std::make_shared<exec::test::FloorAndRoundArgTypesGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 0));
  assertReturnType(generator, signature, DECIMAL(18, 0));
  assertReturnType(generator, signature, DECIMAL(32, 0));
  assertReturnType(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(10, 2));
}

TEST_F(ArgTypesGeneratorTest, round) {
  const auto signatures = getSignatures("round", "decimal");
  VELOX_CHECK_EQ(signatures.size(), 2);
  bool isSingleArg = signatures[0]->argumentTypes().size() == 1;
  const auto generator =
      std::make_shared<exec::test::FloorAndRoundArgTypesGenerator>();

  const auto& singleArgSignature =
      isSingleArg ? *signatures[0] : *signatures[1];
  assertReturnType(generator, singleArgSignature, DECIMAL(10, 0));
  assertReturnType(generator, singleArgSignature, DECIMAL(18, 0));
  assertReturnType(generator, singleArgSignature, DECIMAL(32, 0));
  assertReturnType(generator, singleArgSignature, DECIMAL(38, 0));
  assertEmptyArgs(generator, singleArgSignature, DECIMAL(10, 2));

  const auto& twoArgsSignature = isSingleArg ? *signatures[1] : *signatures[0];
  assertReturnType(generator, twoArgsSignature, DECIMAL(10, 2));
  assertReturnType(generator, twoArgsSignature, DECIMAL(32, 6));
  assertReturnType(generator, twoArgsSignature, DECIMAL(38, 20));
  assertReturnType(generator, twoArgsSignature, DECIMAL(38, 0));
  assertEmptyArgs(generator, twoArgsSignature, DECIMAL(18, 18));
  assertEmptyArgs(generator, twoArgsSignature, DECIMAL(38, 38));
  assertEmptyArgs(generator, twoArgsSignature, DECIMAL(1, 0));
}

TEST_F(ArgTypesGeneratorTest, modulus) {
  const auto& signature = getOnlySignature("mod");
  const auto generator =
      std::make_shared<exec::test::ModulusArgTypesGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertReturnType(generator, signature, DECIMAL(38, 0));
}

TEST_F(ArgTypesGeneratorTest, truncate) {
  const auto signatures = getSignatures("truncate", "decimal");
  VELOX_CHECK_EQ(signatures.size(), 2);
  const auto& signature = signatures[0]->argumentTypes().size() == 1
      ? *signatures[0]
      : *signatures[1];
  const auto generator =
      std::make_shared<exec::test::TruncateArgTypesGenerator>();

  assertReturnType(generator, signature, DECIMAL(1, 0));
  assertReturnType(generator, signature, DECIMAL(10, 0));
  assertReturnType(generator, signature, DECIMAL(18, 0));
  assertReturnType(generator, signature, DECIMAL(32, 0));
  assertReturnType(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(10, 2));
}

} // namespace
