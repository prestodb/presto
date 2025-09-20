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

#include "gtest/gtest.h"

#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/tests/HdfsMiniCluster.h"
#include "velox/connectors/hive/storage_adapters/test_common/InsertTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::test;

class HdfsInsertTest : public testing::Test, public InsertTest {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    filesystems::registerHdfsFileSystem();
    if (miniCluster == nullptr) {
      miniCluster = std::make_shared<filesystems::test::HdfsMiniCluster>();
      miniCluster->start();
    }
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(3);

    InsertTest::SetUp(
        std::make_shared<const config::ConfigBase>(
            std::unordered_map<std::string, std::string>()),
        ioExecutor_.get());
  }

  void TearDown() override {
    for (const auto& [_, filesystem] :
         facebook::velox::filesystems::registeredFilesystems) {
      filesystem->close();
    }
    InsertTest::TearDown();
    miniCluster->stop();
  }

  static std::shared_ptr<filesystems::test::HdfsMiniCluster> miniCluster;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
};

std::shared_ptr<filesystems::test::HdfsMiniCluster>
    HdfsInsertTest::miniCluster = nullptr;

TEST_F(HdfsInsertTest, hdfsInsertTest) {
  const int64_t kExpectedRows = 1'000;
  runInsertTest(fmt::format("{}/", miniCluster->url()), kExpectedRows, pool());
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
