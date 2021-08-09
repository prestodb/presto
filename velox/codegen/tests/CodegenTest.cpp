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

#include "velox/codegen/Codegen.h"
#include <gtest/gtest.h>
#include "velox/core/PlanNode.h"
#include "velox/experimental/codegen/CodegenExceptions.h"
#if CODEGEN_ENABLED == 1
#include "velox/experimental/codegen/CodegenLogger.h"
#endif

namespace facebook::velox::codegen {

class CodegenTest : public testing::Test {};
#if CODEGEN_ENABLED == 1
/// Test that the linker chose the symbols in Codegen.cpp and not the
/// symbols from CodegenStubs.cpp
TEST_F(CodegenTest, linkedWithStrongSymbols) {
  auto codegenLogger = std::make_shared<CodegenTaskLoggerBase>("none");
  Codegen codegen(codegenLogger);
  // If the linker chose symbols in CodegenStubs.cpp, it will fail with
  // CodegenStubsException instead
  EXPECT_THROW(
      codegen.initialize("/dummy/file", /* lazyLoading */ true),
      CodegenInitializationException);
};
#else

TEST_F(CodegenTest, dummyCodegen) {
  Codegen codegen;
  EXPECT_TRUE(codegen.initialize("/dummy/file", /* lazyLoading */ true));
};

#endif

} // namespace facebook::velox::codegen
