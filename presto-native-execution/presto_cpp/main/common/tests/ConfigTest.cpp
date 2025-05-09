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
#include <filesystem>
#include <unordered_set>
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/common/Configs.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Window.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

namespace facebook::presto::test {

using namespace velox;

namespace {

template <typename FunctionMap>
bool validateDefaultNamespacePrefix(
    const FunctionMap& functionMap,
    const std::string& prestoDefaultNamespacePrefix) {
  static const std::unordered_set<std::string> kBlockList = {
      "row_constructor", "in", "is_null"};

  std::vector<std::string> prestoDefaultNamespacePrefixParts;
  folly::split(
      '.',
      prestoDefaultNamespacePrefix,
      prestoDefaultNamespacePrefixParts,
      true);
  VELOX_CHECK_EQ(prestoDefaultNamespacePrefixParts.size(), 2);
  for (const auto& [functionName, _] : functionMap) {
    // Skip internal functions. They don't have any prefix.
    if ((kBlockList.count(functionName) != 0) ||
        (functionName.find("$internal$") != std::string::npos)) {
      continue;
    }

    std::vector<std::string> parts;
    folly::split('.', functionName, parts, true);
    VELOX_CHECK_EQ(parts.size(), 3);
    if ((parts[0] != prestoDefaultNamespacePrefixParts[0]) ||
        (parts[1] != prestoDefaultNamespacePrefixParts[1])) {
      return false;
    }
  }
  return true;
}
} // namespace

class ConfigTest : public testing::Test {
 protected:
  void SetUp() override {
    velox::filesystems::registerLocalFileSystem();
    setUpConfigFilePath();
  }

  void TearDown() override {
    cleanupConfigFilePath();
  }

  void setUpConfigFilePath() {
    char path[] = "/tmp/velox_system_config_test_XXXXXX";
    const char* tempDirectoryPath = mkdtemp(path);
    if (tempDirectoryPath == nullptr) {
      throw std::logic_error("Cannot open temp directory");
    }
    configFilePath_ = tempDirectoryPath;
    configFilePath_ += "/config.properties";
  }

  void cleanupConfigFilePath() {
    auto fileSystem = filesystems::getFileSystem(configFilePath_, nullptr);
    auto dirPath = std::filesystem::path(configFilePath_).parent_path();
    fileSystem->rmdir(dirPath.string());
  }

  void writeDefaultConfigFile(bool isMutable) {
    auto fileSystem = filesystems::getFileSystem(configFilePath_, nullptr);
    auto sysConfigFile = fileSystem->openFileForWrite(configFilePath_);
    sysConfigFile->append(
        fmt::format("{}={}\n", SystemConfig::kPrestoVersion, kPrestoVersion_));
    sysConfigFile->append(
        fmt::format("{}=11kB\n", SystemConfig::kQueryMaxMemoryPerNode));
    if (isMutable) {
      sysConfigFile->append(
          fmt::format("{}={}\n", ConfigBase::kMutableConfig, "true"));
    }
    sysConfigFile->close();
  }

  void writeConfigFile(const std::string& config) {
    auto fileSystem = filesystems::getFileSystem(configFilePath_, nullptr);
    auto sysConfigFile = fileSystem->openFileForWrite(configFilePath_);
    sysConfigFile->append(config);
    sysConfigFile->close();
  }

  void init(
      ConfigBase& config,
      std::unordered_map<std::string, std::string> properties) {
    config.initialize(
        std::make_unique<config::ConfigBase>(std::move(properties)));
  }

  std::string configFilePath_;
  const std::string_view kPrestoVersion_{"SystemConfigTest1"};
  const std::string_view kPrestoVersion2_{"SystemConfigTest2"};
};

TEST_F(ConfigTest, defaultSystemConfig) {
  writeDefaultConfigFile(false);
  auto systemConfig = SystemConfig::instance();
  systemConfig->initialize(configFilePath_);

  ASSERT_FALSE(systemConfig->mutableConfig());
  ASSERT_EQ(kPrestoVersion_, systemConfig->prestoVersion());
  ASSERT_EQ(11 << 10, systemConfig->queryMaxMemoryPerNode());
  ASSERT_THROW(
      systemConfig->setValue(
          std::string(SystemConfig::kPrestoVersion),
          std::string(kPrestoVersion2_)),
      VeloxException);
}

TEST_F(ConfigTest, mutableSystemConfig) {
  writeDefaultConfigFile(true);
  auto systemConfig = SystemConfig::instance();
  systemConfig->initialize(configFilePath_);

  ASSERT_TRUE(systemConfig->mutableConfig());
  ASSERT_EQ(kPrestoVersion_, systemConfig->prestoVersion());
  ASSERT_EQ(
      kPrestoVersion_,
      systemConfig
          ->setValue(
              std::string(SystemConfig::kPrestoVersion),
              std::string(kPrestoVersion2_))
          .value());
  ASSERT_EQ(kPrestoVersion2_, systemConfig->prestoVersion());
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

  // make sure "node.kNodePrometheusExecutorThreads" works too
  init(config, {{std::string(NodeConfig::kNodePrometheusExecutorThreads), "4"}});
  ASSERT_EQ(
      config.prometheusExecutorThreads(), 4);
}

TEST_F(ConfigTest, optionalSystemConfigsWithDefault) {
  SystemConfig config;
  init(config, {});
  ASSERT_EQ(config.maxDriversPerTask(), std::thread::hardware_concurrency());
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
  auto configMap = presto::util::readConfig(configFilePath_);
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
    cleanupConfigFilePath();
    setUpConfigFilePath();
    writeConfigFile(fileContent);
    VELOX_ASSERT_THROW(
        presto::util::readConfig(configFilePath_), expectedErrorMsg);
  };
  testInvalid(
      "noequalsign\n", "No '=' sign found for property pair 'noequalsign'");
  testInvalid("===\n", "property pair '===' has empty key");
}

TEST_F(ConfigTest, optionalNodeId) {
  NodeConfig config;
  auto nodeId = config.nodeId();
  // Same value must be returned.
  EXPECT_EQ(nodeId, config.nodeId());
  EXPECT_EQ(nodeId, config.nodeId());
}

TEST_F(ConfigTest, readConfigEnvVarTest) {
  const std::string kEnvVarName = "PRESTO_READ_CONFIG_TEST_VAR";
  const std::string kEmptyEnvVarName = "PRESTO_READ_CONFIG_TEST_EMPTY_VAR";

  const std::string kPlainTextKey = "plain-text";
  const std::string kPlainTextValue = "plain-text-value";

  const std::string kEnvVarKey = "env-var";
  const std::string kEnvVarValue = "env-var-value";

  // Keys to test invalid environment variable values.
  const std::string kEnvVarKey2 = "env-var2";
  const std::string kEnvVarKey3 = "env-var3";
  const std::string kNoEnvVarKey = "no-env-var";
  const std::string kEmptyEnvVarKey = "empty-env-var";

  writeConfigFile(
      fmt::format("{}={}\n", kPlainTextKey, kPlainTextValue) +
      fmt::format("{}=${{{}}}\n", kEnvVarKey, kEnvVarName) +
      fmt::format("{}=${{{}\n", kEnvVarKey2, kEnvVarName) +
      fmt::format("{}={}}}\n", kEnvVarKey3, kEnvVarName) +
      fmt::format("{}=${{}}\n", kNoEnvVarKey) +
      fmt::format("{}=${{{}}}\n", kEmptyEnvVarKey, kEmptyEnvVarName));

  setenv(kEnvVarName.c_str(), kEnvVarValue.c_str(), 1);
  setenv(kEmptyEnvVarName.c_str(), "", 1);

  auto properties = presto::util::readConfig(configFilePath_);
  std::unordered_map<std::string, std::string> expected{
      {kPlainTextKey, kPlainTextValue},
      {kEnvVarKey, kEnvVarValue},
      {kEnvVarKey2, "${PRESTO_READ_CONFIG_TEST_VAR"},
      {kEnvVarKey3, "PRESTO_READ_CONFIG_TEST_VAR}"},
      {kNoEnvVarKey, "${}"},
      {kEmptyEnvVarKey, ""}};
  ASSERT_EQ(properties, expected);

  unsetenv(kEnvVarName.c_str());
  unsetenv(kEmptyEnvVarName.c_str());
}

TEST_F(ConfigTest, prestoDefaultNamespacePrefix) {
  SystemConfig config;
  init(
      config,
      {{std::string(SystemConfig::kPrestoDefaultNamespacePrefix),
        "presto.default"}});
  std::string prestoBuiltinFunctionPrefix =
      config.prestoDefaultNamespacePrefix();

  velox::functions::prestosql::registerAllScalarFunctions(
      prestoBuiltinFunctionPrefix);
  velox::aggregate::prestosql::registerAllAggregateFunctions(
      prestoBuiltinFunctionPrefix);
  velox::window::prestosql::registerAllWindowFunctions(
      prestoBuiltinFunctionPrefix);

  // Get all registered scalar functions in Velox
  auto scalarFunctions = getFunctionSignatures();
  ASSERT_TRUE(validateDefaultNamespacePrefix(
      scalarFunctions, prestoBuiltinFunctionPrefix));

  // Get all registered aggregate functions in Velox
  auto aggregateFunctions = exec::aggregateFunctions().copy();
  ASSERT_TRUE(validateDefaultNamespacePrefix(
      aggregateFunctions, prestoBuiltinFunctionPrefix));

  // Get all registered window functions in Velox
  auto windowFunctions = exec::windowFunctions();
  ASSERT_TRUE(validateDefaultNamespacePrefix(
      windowFunctions, prestoBuiltinFunctionPrefix));
}
} // namespace facebook::presto::test
