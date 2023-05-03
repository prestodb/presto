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

#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::functions::test {

class ScalarFunctionRegTest : public testing::Test {};

TEST_F(ScalarFunctionRegTest, prefix) {
  // Remove all functions and check for no entries.
  exec::vectorFunctionFactories().wlock()->clear();
  exec::mutableSimpleFunctions().testingClear();
  EXPECT_EQ(0, exec::vectorFunctionFactories().rlock()->size());
  EXPECT_EQ(0, exec::simpleFunctions().getFunctionNames().size());

  // Register without prefix and memorize function maps.
  prestosql::registerAllScalarFunctions();
  const std::unordered_map<std::string, exec::VectorFunctionEntry>
      scalarVectorFuncMapBase = *(exec::vectorFunctionFactories().rlock());
  std::unordered_set<std::string> scalarSimpleFuncBaseNames;
  for (const auto& funcName : exec::simpleFunctions().getFunctionNames()) {
    scalarSimpleFuncBaseNames.insert(funcName);
  }

  // Remove all functions and check for no entries.
  exec::vectorFunctionFactories().wlock()->clear();
  exec::mutableSimpleFunctions().testingClear();
  EXPECT_EQ(0, exec::vectorFunctionFactories().rlock()->size());
  EXPECT_EQ(0, exec::simpleFunctions().getFunctionNames().size());

  // Register with prefix and check all functions have the prefix.
  const std::string prefix{"test.abc_schema."};
  prestosql::registerAllScalarFunctions(prefix);
  std::unordered_map<std::string, exec::VectorFunctionEntry>
      scalarVectorFuncMap = *(exec::vectorFunctionFactories().rlock());

  // Remove special form functions - they don't have any prefix.
  scalarVectorFuncMap.erase("in");
  scalarVectorFuncMap.erase("row_constructor");
  scalarVectorFuncMap.erase("is_null");

  for (const auto& entry : scalarVectorFuncMap) {
    EXPECT_EQ(prefix, entry.first.substr(0, prefix.size()));
    EXPECT_EQ(
        1, scalarVectorFuncMapBase.count(entry.first.substr(prefix.size())));
  }
  for (const auto& funcName : exec::simpleFunctions().getFunctionNames()) {
    EXPECT_EQ(prefix, funcName.substr(0, prefix.size()));
    EXPECT_EQ(
        1, scalarSimpleFuncBaseNames.count(funcName.substr(prefix.size())));
  }
}

} // namespace facebook::velox::functions::test
