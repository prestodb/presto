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
#include <gtest/gtest.h>
#include "velox/expression/SignatureBinder.h"

namespace facebook::velox::fuzzer::test {

void assertReturnType(
    const std::shared_ptr<ArgGenerator>& generator,
    const exec::FunctionSignature& signature,
    const TypePtr& returnType) {
  std::mt19937 seed{0};
  const auto argTypes = generator->generateArgs(signature, returnType, seed);

  // Resolve return type from argument types for the given signature.
  exec::SignatureBinder binder(signature, argTypes);
  VELOX_CHECK(
      binder.tryBind(),
      "Failed to resolve {} from argument types.",
      returnType->toString());
  const auto actualType = binder.tryResolveReturnType();
  EXPECT_TRUE(returnType->equivalent(*actualType))
      << "Expected type: " << returnType->toString()
      << ", actual type: " << actualType->toString();
}

void assertEmptyArgs(
    std::shared_ptr<ArgGenerator> generator,
    const exec::FunctionSignature& signature,
    const TypePtr& returnType) {
  std::mt19937 seed{0};
  const auto argTypes = generator->generateArgs(signature, returnType, seed);
  EXPECT_TRUE(argTypes.empty());
}

} // namespace facebook::velox::fuzzer::test
