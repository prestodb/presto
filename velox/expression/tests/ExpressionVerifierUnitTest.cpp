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
#include <fstream>

#include "velox/expression/tests/ExpressionVerifier.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/Registerer.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

namespace {

template <typename T>
struct AlwaysThrowsUserErrorFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult&, const TInput&) {
    VELOX_USER_FAIL("expected");
  }
};

template <typename T>
struct AlwaysThrowsRuntimeErrorFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult&, const TInput&) {
    VELOX_FAIL("expected");
  }
};

void removeDirecrtoryIfExist(
    std::shared_ptr<filesystems::FileSystem>& localFs,
    const std::string& folderPath) {
  if (localFs->exists(folderPath)) {
    localFs->rmdir(folderPath);
  }
  EXPECT_FALSE(localFs->exists(folderPath));
}

} // namespace

class ExpressionVerifierUnitTest : public testing::Test, public VectorTestBase {
 public:
  ExpressionVerifierUnitTest() {
    parse::registerTypeResolver();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    parse::ParseOptions options;
    auto untyped = parse::parseExpr(text, options);
    return core::Expressions::inferTypes(untyped, rowType, pool_.get());
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
};

TEST_F(ExpressionVerifierUnitTest, persistReproInfo) {
  filesystems::registerLocalFileSystem();
  auto reproFolder = exec::test::TempDirectoryPath::create();
  const auto reproPath = reproFolder->getPath();
  auto localFs = filesystems::getFileSystem(reproPath, nullptr);

  ExpressionVerifierOptions options{false, reproPath.c_str(), false};
  ExpressionVerifier verifier{&execCtx_, options};

  auto testReproPersistency = [this](
                                  ExpressionVerifier& verifier,
                                  const std::string& reproPath,
                                  auto& localFs) {
    auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
    auto plan = parseExpression("always_throws(c0)", asRowType(data->type()));

    removeDirecrtoryIfExist(localFs, reproPath);
    VELOX_ASSERT_THROW(verifier.verify({plan}, data, nullptr, false), "");
    EXPECT_TRUE(localFs->exists(reproPath));
    EXPECT_FALSE(localFs->list(reproPath).empty());
    removeDirecrtoryIfExist(localFs, reproPath);
  };

  // User errors.
  {
    registerFunction<AlwaysThrowsUserErrorFunction, int32_t, int32_t>(
        {"always_throws"});
    testReproPersistency(verifier, reproPath, localFs);
  }

  // Runtime errors.
  {
    registerFunction<AlwaysThrowsRuntimeErrorFunction, int32_t, int32_t>(
        {"always_throws"});
    testReproPersistency(verifier, reproPath, localFs);
  }
}

} // namespace facebook::velox::test
