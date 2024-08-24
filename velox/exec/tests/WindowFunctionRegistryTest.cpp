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

#include "velox/exec/WindowFunction.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

namespace facebook::velox::exec::test {

namespace {
void registerWindowFunction(const std::string& name) {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .argumentType("bigint")
          .argumentType("double")
          .returnType("bigint")
          .build(),
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("T")
          .argumentType("T")
          .returnType("T")
          .build(),
      exec::FunctionSignatureBuilder().returnType("date").build(),
  };
  exec::registerWindowFunction(
      name,
      std::move(signatures),
      exec::WindowFunction::Metadata::defaultMetadata(),
      nullptr);
}
} // namespace

class WindowFunctionRegistryTest : public testing::Test {
 public:
  WindowFunctionRegistryTest() {
    registerWindowFunction("window_func");
    registerWindowFunction("window_Func_Alias");
  }

  TypePtr resolveWindowFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes) {
    if (auto windowFunctionSignatures = getWindowFunctionSignatures(name)) {
      for (const auto& signature : windowFunctionSignatures.value()) {
        SignatureBinder binder(*signature, argTypes);
        if (binder.tryBind()) {
          return binder.tryResolveReturnType();
        }
      }
    }

    return nullptr;
  }

  void testResolveWindowFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& expectedType) {
    auto actualType = resolveWindowFunction(name, argTypes);
    if (expectedType) {
      EXPECT_EQ(*actualType, *expectedType);
    } else {
      EXPECT_EQ(actualType, nullptr);
    }
  }
};

TEST_F(WindowFunctionRegistryTest, basic) {
  testResolveWindowFunction("window_func", {BIGINT(), DOUBLE()}, BIGINT());
  testResolveWindowFunction("window_func", {DOUBLE(), DOUBLE()}, DOUBLE());
  testResolveWindowFunction(
      "window_func", {ARRAY(BOOLEAN()), ARRAY(BOOLEAN())}, ARRAY(BOOLEAN()));
  testResolveWindowFunction("window_func", {}, DATE());
}

TEST_F(WindowFunctionRegistryTest, wrongName) {
  testResolveWindowFunction(
      "window_func_not_exist", {BIGINT(), DOUBLE()}, nullptr);
  testResolveWindowFunction(
      "window_func_not_exist", {DOUBLE(), DOUBLE()}, nullptr);
  testResolveWindowFunction(
      "window_func_not_exist", {ARRAY(BOOLEAN()), ARRAY(BOOLEAN())}, nullptr);
  testResolveWindowFunction("window_func_not_exist", {}, nullptr);
}

TEST_F(WindowFunctionRegistryTest, wrongSignature) {
  testResolveWindowFunction("window_func", {DOUBLE(), BIGINT()}, nullptr);
  testResolveWindowFunction("window_func", {BIGINT()}, nullptr);
  testResolveWindowFunction(
      "window_func", {BIGINT(), BIGINT(), BIGINT()}, nullptr);
}

TEST_F(WindowFunctionRegistryTest, mixedCaseName) {
  testResolveWindowFunction("window_FUNC", {BIGINT(), DOUBLE()}, BIGINT());
  testResolveWindowFunction(
      "window_fUNC_alias", {BIGINT(), DOUBLE()}, BIGINT());
}

TEST_F(WindowFunctionRegistryTest, prefix) {
  // Remove all functions and check for no entries.
  exec::windowFunctions().clear();
  EXPECT_EQ(0, exec::windowFunctions().size());

  // Register without prefix and memorize function maps.
  window::prestosql::registerAllWindowFunctions();
  const auto windowFuncMapBase = exec::windowFunctions();

  // Remove all functions and check for no entries.
  exec::windowFunctions().clear();
  EXPECT_EQ(0, exec::windowFunctions().size());

  // Register with prefix and check all functions have the prefix.
  const std::string prefix{"test.abc_schema."};
  window::prestosql::registerAllWindowFunctions(prefix);
  auto& windowFuncMap = exec::windowFunctions();
  for (const auto& entry : windowFuncMap) {
    EXPECT_EQ(prefix, entry.first.substr(0, prefix.size()));
    EXPECT_EQ(1, windowFuncMapBase.count(entry.first.substr(prefix.size())));
  }
}

} // namespace facebook::velox::exec::test
