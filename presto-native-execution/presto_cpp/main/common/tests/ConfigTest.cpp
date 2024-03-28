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
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/common/Configs.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::presto::test {

using namespace velox;

class ConfigTest : public testing::Test {
 protected:
  void setUpConfigFilePath() {
    velox::filesystems::registerLocalFileSystem();

    char path[] = "/tmp/velox_system_config_test_XXXXXX";
    const char* tempDirectoryPath = mkdtemp(path);
    if (tempDirectoryPath == nullptr) {
      throw std::logic_error("Cannot open temp directory");
    }
    configFilePath = tempDirectoryPath;
    configFilePath += "/config.properties";
  }

  void writeDefaultConfigFile(bool isMutable) {
    auto fileSystem = filesystems::getFileSystem(configFilePath, nullptr);
    auto sysConfigFile = fileSystem->openFileForWrite(configFilePath);
    sysConfigFile->append(
        fmt::format("{}={}\n", SystemConfig::kPrestoVersion, prestoVersion));
    sysConfigFile->append(
        fmt::format("{}=11kB\n", SystemConfig::kQueryMaxMemoryPerNode));
    if (isMutable) {
      sysConfigFile->append(
          fmt::format("{}={}\n", ConfigBase::kMutableConfig, "true"));
    }
    sysConfigFile->close();
  }

  void writeConfigFile(const std::string& config) {
    auto fileSystem = filesystems::getFileSystem(configFilePath, nullptr);
    auto sysConfigFile = fileSystem->openFileForWrite(configFilePath);
    sysConfigFile->append(config);
    sysConfigFile->close();
  }

  void init(
      ConfigBase& config,
      std::unordered_map<std::string, std::string> properties) {
    config.initialize(std::make_unique<core::MemConfig>(std::move(properties)));
  }

  std::string configFilePath;
  const std::string prestoVersion{"SystemConfigTest1"};
  const std::string prestoVersion2{"SystemConfigTest2"};
};

TEST_F(ConfigTest, defaultSystemConfig) {
  setUpConfigFilePath();
  writeDefaultConfigFile(false);
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

TEST_F(ConfigTest, mutableSystemConfig) {
  setUpConfigFilePath();
  writeDefaultConfigFile(true);
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

TEST_F(ConfigTest, requiredSystemConfigs) {
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

TEST_F(ConfigTest, optionalSystemConfigs) {
  SystemConfig config;
  init(config, {});
  ASSERT_EQ(folly::none, config.discoveryUri());

  init(config, {{std::string(SystemConfig::kDiscoveryUri), "my uri"}});
  ASSERT_EQ(config.discoveryUri(), "my uri");
}

TEST_F(ConfigTest, optionalNodeConfigs) {
  NodeConfig config;
  init(config, {});
  ASSERT_EQ(config.nodeInternalAddress([]() { return "0.0.0.0"; }), "0.0.0.0");

  init(config, {{std::string(NodeConfig::kNodeInternalAddress), "localhost"}});
  ASSERT_EQ(
      config.nodeInternalAddress([]() { return "0.0.0.0"; }), "localhost");

  // "node.internal-address" is the new config replacing legacy config "node.ip"
  init(
      config,
      {{std::string(NodeConfig::kNodeInternalAddress), "localhost"},
       {std::string(NodeConfig::kNodeIp), "127.0.0.1"}});
  ASSERT_EQ(
      config.nodeInternalAddress([]() { return "0.0.0.0"; }), "localhost");

  // make sure "node.ip" works too
  init(config, {{std::string(NodeConfig::kNodeIp), "127.0.0.1"}});
  ASSERT_EQ(
      config.nodeInternalAddress([]() { return "0.0.0.0"; }), "127.0.0.1");
}

TEST_F(ConfigTest, optionalSystemConfigsWithDefault) {
  SystemConfig config;
  init(config, {});
  ASSERT_EQ(config.maxDriversPerTask(), 16);
  init(config, {{std::string(SystemConfig::kMaxDriversPerTask), "1024"}});
  ASSERT_EQ(config.maxDriversPerTask(), 1024);
}

TEST_F(ConfigTest, remoteFunctionServer) {
  SystemConfig config;
  init(config, {});
  ASSERT_EQ(folly::none, config.remoteFunctionServerLocation());

  // Only address. Throw.
  init(
      config,
      {{std::string(SystemConfig::kRemoteFunctionServerThriftAddress),
        "127.1.2.3"}});
  EXPECT_THROW(config.remoteFunctionServerLocation(), VeloxException);

  // Only port (address fallback to loopback).
  init(
      config,
      {{std::string(SystemConfig::kRemoteFunctionServerThriftPort), "8081"}});
  ASSERT_EQ(
      config.remoteFunctionServerLocation(),
      (folly::SocketAddress{"::1", 8081}));

  // Port and address.
  init(
      config,
      {{std::string(SystemConfig::kRemoteFunctionServerThriftPort), "8081"},
       {std::string(SystemConfig::kRemoteFunctionServerThriftAddress),
        "127.1.2.3"}});
  ASSERT_EQ(
      config.remoteFunctionServerLocation(),
      (folly::SocketAddress{"127.1.2.3", 8081}));

  // UDS path.
  init(
      config,
      {{std::string(SystemConfig::kRemoteFunctionServerThriftUdsPath),
        "/tmp/any.socket"}});
  ASSERT_EQ(
      config.remoteFunctionServerLocation(),
      (folly::SocketAddress::makeFromPath("/tmp/any.socket")));
}

TEST_F(ConfigTest, parseValid) {
  setUpConfigFilePath();
  writeConfigFile(
      "#comment\n"
      "#a comment with space\n"
      "# a comment with leading space\n"
      " # a comment with leading space before comment\n"
      "# a comment with key word = == ==== =\n"
      "## a comment with double hashtag\n"
      "key=value\n"
      "k-e-y=v-a-l-u-e\n"
      " key with space=value\n"
      "key1= value with space\n"
      "key2=value=with=key=word\n"
      "emptyvaluekey=");
  auto configMap = presto::util::readConfig(configFilePath);
  ASSERT_EQ(configMap.size(), 6);

  std::unordered_map<std::string, std::string> expected{
      {"key2", "value=with=key=word"},
      {"key1", "valuewithspace"},
      {"keywithspace", "value"},
      {"k-e-y", "v-a-l-u-e"},
      {"key", "value"},
      {"emptyvaluekey", ""}};
  ASSERT_EQ(configMap, expected);
}

TEST_F(ConfigTest, parseInvalid) {
  auto testInvalid = [this](
                         const std::string& fileContent,
                         const std::string& expectedErrorMsg) {
    setUpConfigFilePath();
    writeConfigFile(fileContent);
    VELOX_ASSERT_THROW(
        presto::util::readConfig(configFilePath), expectedErrorMsg);
  };
  testInvalid(
      "noequalsign\n", "No '=' sign found for property pair 'noequalsign'");
  testInvalid("===\n", "property pair '===' has empty key");
}

} // namespace facebook::presto::test
