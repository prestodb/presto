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
#include <gtest/gtest.h>
#include "presto_cpp/main/common/Configs.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::presto::test {

using namespace velox;

class SystemConfigTest : public testing::Test {
 protected:
  void setUpConfigFile(bool isMutable) {
    velox::filesystems::registerLocalFileSystem();

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
        fmt::format("{}={}\n", SystemConfig::kPrestoVersion, prestoVersion));
    sysConfigFile->append(
        fmt::format("{}=11kB\n", SystemConfig::kQueryMaxMemoryPerNode));
    if (isMutable) {
      sysConfigFile->append(
          fmt::format("{}={}\n", SystemConfig::kMutableConfig, "true"));
    }
    sysConfigFile->close();
  }

  void init(
      SystemConfig& config,
      std::unordered_map<std::string, std::string> properties) {
    config.initialize(std::make_unique<core::MemConfig>(std::move(properties)));
  }

  std::string configFilePath;
  const std::string prestoVersion{"SystemConfigTest1"};
  const std::string prestoVersion2{"SystemConfigTest2"};
};

TEST_F(SystemConfigTest, defaultConfig) {
  setUpConfigFile(false);
  auto systemConfig = SystemConfig::instance();
  systemConfig->initialize(configFilePath);

  ASSERT_FALSE(systemConfig->mutableConfig());
  ASSERT_EQ(prestoVersion, systemConfig->prestoVersion());
  ASSERT_EQ(11 << 10, systemConfig->queryMaxMemoryPerNode());
  ASSERT_THROW(
      systemConfig->setValue(
          std::string(SystemConfig::kPrestoVersion), prestoVersion2),
      VeloxException);
}

TEST_F(SystemConfigTest, mutableConfig) {
  setUpConfigFile(true);
  auto systemConfig = SystemConfig::instance();
  systemConfig->initialize(configFilePath);

  ASSERT_TRUE(systemConfig->mutableConfig());
  ASSERT_EQ(prestoVersion, systemConfig->prestoVersion());
  ASSERT_EQ(
      prestoVersion,
      systemConfig
          ->setValue(std::string(SystemConfig::kPrestoVersion), prestoVersion2)
          .value());
  ASSERT_EQ(prestoVersion2, systemConfig->prestoVersion());
  ASSERT_EQ(
      "11kB",
      systemConfig
          ->setValue(std::string(SystemConfig::kQueryMaxMemoryPerNode), "5GB")
          .value());
  ASSERT_EQ(5UL << 30, systemConfig->queryMaxMemoryPerNode());
  ASSERT_THROW(
      systemConfig->setValue("unregisteredProp1", "x"), VeloxException);
}

TEST_F(SystemConfigTest, requiredConfigs) {
  SystemConfig config;
  init(config, {});

  ASSERT_THROW(config.httpServerHttpPort(), VeloxUserError);
  ASSERT_THROW(config.prestoVersion(), VeloxUserError);

  init(
      config,
      {{std::string(SystemConfig::kPrestoVersion), "1234"},
       {std::string(SystemConfig::kHttpServerHttpPort), "8080"}});

  ASSERT_EQ(config.prestoVersion(), "1234");
  ASSERT_EQ(config.httpServerHttpPort(), 8080);
}

TEST_F(SystemConfigTest, optionalConfigs) {
  SystemConfig config;
  init(config, {});
  ASSERT_EQ(folly::none, config.discoveryUri());

  init(config, {{std::string(SystemConfig::kDiscoveryUri), "my uri"}});
  ASSERT_EQ(config.discoveryUri(), "my uri");
}

TEST_F(SystemConfigTest, optionalWithDefault) {
  SystemConfig config;
  init(config, {});
  ASSERT_EQ(config.maxDriversPerTask(), 16);
  init(config, {{std::string(SystemConfig::kMaxDriversPerTask), "1024"}});
  ASSERT_EQ(config.maxDriversPerTask(), 1024);
}

TEST_F(SystemConfigTest, remoteFunctionServer) {
  SystemConfig config;
  init(config, {});
  ASSERT_EQ(folly::none, config.remoteFunctionServerLocation());

  init(
      config,
      {{std::string(SystemConfig::kRemoteFunctionServerThriftPort), "8081"}});
  ASSERT_EQ(
      config.remoteFunctionServerLocation(),
      (folly::SocketAddress{"::1", 8081}));
}

} // namespace facebook::presto::test
