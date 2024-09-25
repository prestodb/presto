/*
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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/functions/DynamicLibraryLoader.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/expression/SimpleFunctionRegistry.h"

using namespace facebook::velox::functions::test;
using namespace facebook::velox;
using namespace facebook::presto;
namespace facebook::presto::functions::test {

class DynamicLinkTest : public FunctionBaseTest {};

TEST_F(DynamicLinkTest, dynamicLoad) {
  const auto dynamicFunction = [&](std::optional<double> a) {
    return evaluateOnce<int64_t>("dynamic_123()", a);
  };

  auto signaturesBefore = getFunctionSignatures().size();

  // Function does not exist yet.
  EXPECT_THROW(dynamicFunction(0), VeloxUserError);

  // Dynamically load the library.
  std::string libraryPath = MY_DYNAMIC_FUNCTION_LIBRARY_PATH;
  libraryPath +=
      "/libpresto_function_my_dynamic.dylib"; // building on MacOS leads to
                                              // .dylib file not .so file
  auto& registry = exec::simpleFunctions(); // for testing purposes
  EXPECT_TRUE(loadDynamicLibraryFunctions(libraryPath.data()));
  auto signaturesAfter = getFunctionSignatures().size();
  auto type = exec::simpleFunctions().resolveFunction("dynamic_123", {})->type();
  EXPECT_EQ(TypeKind::BIGINT, type->kind());
  EXPECT_EQ(signaturesAfter, signaturesBefore + 1);

  // Make sure the function exists now.
  EXPECT_EQ(123, dynamicFunction(0));
}

} // namespace facebook::presto::functions::test


