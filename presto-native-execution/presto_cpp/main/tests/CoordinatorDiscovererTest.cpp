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

#include "presto_cpp/main/CoordinatorDiscoverer.h"
#include <gtest/gtest.h>
#include "presto_cpp/main/common/Configs.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/Config.h"

using namespace facebook::presto;
using namespace facebook::velox;

class CoordinatorDiscovererTest : public testing::Test {
 public:
  void SetUp() override {
    filesystems::registerLocalFileSystem();

    char path[] = "/tmp/velox_system_config_test_XXXXXX";
    const char* tempDirectoryPath = mkdtemp(path);
    if (tempDirectoryPath == nullptr) {
      throw std::logic_error("Cannot open temp directory");
    }
    configFilePath = tempDirectoryPath;
    configFilePath += "/config.properties";

    auto fileSystem = filesystems::getFileSystem(configFilePath, nullptr);
    auto sysConfigFile = fileSystem->openFileForWrite(configFilePath);
    sysConfigFile->append(
        fmt::format("{}={}\n", ConfigBase::kMutableConfig, "true"));
    sysConfigFile->close();
  }

  std::string configFilePath;
};

TEST_F(CoordinatorDiscovererTest, basic) {
  auto systemConfig = SystemConfig::instance();
  systemConfig->initialize(configFilePath);
  auto coordinatorDiscoverer = std::make_unique<CoordinatorDiscoverer>();
  EXPECT_TRUE(coordinatorDiscoverer->updateAddress().empty());

  systemConfig->setValue(
      std::string(SystemConfig::kDiscoveryUri), "http://88.88.88.88:8888");

  EXPECT_TRUE(coordinatorDiscoverer->getAddress().empty());
  auto updatedAddress = coordinatorDiscoverer->updateAddress();
  EXPECT_FALSE(updatedAddress.empty());
  EXPECT_EQ(updatedAddress.getAddressStr(), "88.88.88.88");
  EXPECT_EQ(updatedAddress.getPort(), 8888);

  auto cachedAddress = coordinatorDiscoverer->getAddress();
  EXPECT_EQ(cachedAddress.getAddressStr(), "88.88.88.88");
  EXPECT_EQ(cachedAddress.getPort(), 8888);
}
