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
#include "velox/functions/prestosql/fuzzer/DivideArgGenerator.h"
#include "velox/functions/prestosql/fuzzer/FloorAndRoundArgGenerator.h"
#include "velox/functions/prestosql/fuzzer/ModulusArgGenerator.h"
#include "velox/functions/prestosql/fuzzer/MultiplyArgGenerator.h"
#include "velox/functions/prestosql/fuzzer/PlusMinusArgGenerator.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::fuzzer::test;
namespace {

class ArgGeneratorTest : public functions::test::FunctionBaseTest {
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

TEST_F(ArgGeneratorTest, plus) {
  const auto& signature = getOnlySignature("plus");
  const auto generator = std::make_shared<exec::test::PlusMinusArgGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertReturnType(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(18, 18));
}

TEST_F(ArgGeneratorTest, minus) {
  const auto& signature = getOnlySignature("minus");
  const auto generator = std::make_shared<exec::test::PlusMinusArgGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertReturnType(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(18, 18));
}

TEST_F(ArgGeneratorTest, multiply) {
  const auto& signature = getOnlySignature("multiply");
  const auto generator = std::make_shared<exec::test::MultiplyArgGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(18, 18));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertReturnType(generator, signature, DECIMAL(38, 0));
}

TEST_F(ArgGeneratorTest, divide) {
  const auto& signature = getOnlySignature("divide");
  const auto generator = std::make_shared<exec::test::DivideArgGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(18, 18));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertReturnType(generator, signature, DECIMAL(38, 0));
}

TEST_F(ArgGeneratorTest, floor) {
  const auto& signature = getOnlySignature("floor");
  const auto generator =
      std::make_shared<exec::test::FloorAndRoundArgGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 0));
  assertReturnType(generator, signature, DECIMAL(18, 0));
  assertReturnType(generator, signature, DECIMAL(32, 0));
  assertReturnType(generator, signature, DECIMAL(38, 0));
  assertEmptyArgs(generator, signature, DECIMAL(10, 2));
}

TEST_F(ArgGeneratorTest, round) {
  const auto signatures = getSignatures("round", "decimal");
  VELOX_CHECK_EQ(signatures.size(), 2);
  bool isSingleArg = signatures[0]->argumentTypes().size() == 1;
  const auto generator =
      std::make_shared<exec::test::FloorAndRoundArgGenerator>();

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

TEST_F(ArgGeneratorTest, modulus) {
  const auto& signature = getOnlySignature("mod");
  const auto generator = std::make_shared<exec::test::ModulusArgGenerator>();

  assertReturnType(generator, signature, DECIMAL(10, 2));
  assertReturnType(generator, signature, DECIMAL(32, 6));
  assertReturnType(generator, signature, DECIMAL(38, 20));
  assertReturnType(generator, signature, DECIMAL(38, 38));
  assertReturnType(generator, signature, DECIMAL(38, 0));
}

} // namespace
