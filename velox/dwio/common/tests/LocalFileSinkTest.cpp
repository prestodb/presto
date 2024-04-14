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

#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <gtest/gtest.h>

using namespace ::testing;
using namespace facebook::velox::exec::test;

namespace facebook::velox::dwio::common {

class LocalFileSinkTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void runTest() {
    auto root = TempDirectoryPath::create();
    auto filePath = fs::path(root->getPath()) / "xxx/yyy/zzz/test_file.ext";

    ASSERT_FALSE(fs::exists(filePath.string()));

    auto localFileSink = FileSink::create(
        fmt::format("file:{}", filePath.string()), {.pool = pool_.get()});
    ASSERT_TRUE(localFileSink->isBuffered());
    localFileSink->close();

    EXPECT_TRUE(fs::exists(filePath.string()));
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

TEST_F(LocalFileSinkTest, missingRegistration) {
  VELOX_ASSERT_THROW(runTest(), "FileSink is not registered for file:");
}

TEST_F(LocalFileSinkTest, create) {
  LocalFileSink::registerFactory();
  runTest();
}

} // namespace facebook::velox::dwio::common
