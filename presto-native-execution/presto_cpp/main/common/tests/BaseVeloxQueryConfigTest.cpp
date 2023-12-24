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
#include "velox/core/QueryConfig.h"

namespace facebook::presto::test {

using namespace velox;
using namespace velox::core;

class BaseVeloxQueryConfigTest : public testing::Test {
 protected:
  void setUpConfigFile(bool isMutable, bool setupSystemConfig = false) {
    velox::filesystems::registerLocalFileSystem();

    char path[] = "/tmp/base_velox_query_config_test_XXXXXX";
    const char* tempDirectoryPath = mkdtemp(path);
    if (tempDirectoryPath == nullptr) {
      throw std::logic_error("Cannot open temp directory");
    }
    configFilePath = tempDirectoryPath;
    configFilePath += "/velox.properties";
    systemConfigFilePath = tempDirectoryPath;
    systemConfigFilePath += "/config.properties";

    if (setupSystemConfig) {
      auto fileSystem =
          filesystems::getFileSystem(systemConfigFilePath, nullptr);
      auto systemConfigFile =
          fileSystem->openFileForWrite(systemConfigFilePath);
      systemConfigFile->append(
          fmt::format("{}=true\n", SystemConfig::kUseLegacyArrayAgg));
      systemConfigFile->append(
          fmt::format("{}=17MB\n", SystemConfig::kSinkMaxBufferSize));
      systemConfigFile->append(fmt::format(
          "{}=6MB\n", SystemConfig::kDriverMaxPagePartitioningBufferSize));
      systemConfigFile->close();
      SystemConfig::instance()->initialize(systemConfigFilePath);
    }

    auto fileSystem = filesystems::getFileSystem(configFilePath, nullptr);
    auto sysConfigFile = fileSystem->openFileForWrite(configFilePath);
    if (isMutable) {
      sysConfigFile->append(
          fmt::format("{}=true\n", QueryConfig::kCodegenEnabled));
      sysConfigFile->append(
          fmt::format("{}=100\n", QueryConfig::kMaxOutputBatchRows));
      sysConfigFile->append(
          fmt::format("{}=true\n", ConfigBase::kMutableConfig));
    }
    sysConfigFile->close();
  }

  std::string configFilePath;
  std::string systemConfigFilePath;
  const std::string tzPropName{QueryConfig::kSessionTimezone};
};

TEST_F(BaseVeloxQueryConfigTest, defaultConfig) {
  setUpConfigFile(false);
  auto cfg = BaseVeloxQueryConfig::instance();
  cfg->initialize(configFilePath);

  ASSERT_FALSE(cfg->optionalProperty<bool>(ConfigBase::kMutableConfig).value());
  ASSERT_FALSE(
      cfg->optionalProperty<bool>(std::string(QueryConfig::kCodegenEnabled))
          .value());
  ASSERT_EQ(
      10'000,
      cfg->optionalProperty<uint32_t>(
             std::string(QueryConfig::kMaxOutputBatchRows))
          .value());
  ASSERT_EQ("", cfg->optionalProperty(tzPropName).value());
  ASSERT_THROW(cfg->setValue(tzPropName, "TZ1"), VeloxException);
}

TEST_F(BaseVeloxQueryConfigTest, mutableConfig) {
  setUpConfigFile(true);
  auto cfg = BaseVeloxQueryConfig::instance();
  cfg->initialize(configFilePath);

  ASSERT_TRUE(cfg->optionalProperty<bool>(ConfigBase::kMutableConfig).value());
  ASSERT_TRUE(
      cfg->optionalProperty<bool>(std::string(QueryConfig::kCodegenEnabled))
          .value());
  ASSERT_EQ(
      100,
      cfg->optionalProperty<uint32_t>(
             std::string(QueryConfig::kMaxOutputBatchRows))
          .value());
  ASSERT_EQ("", cfg->optionalProperty(tzPropName).value());
  auto ret = cfg->setValue(tzPropName, "TZ1");
  ASSERT_EQ("", ret.value());
  ASSERT_EQ(
      folly::Optional<std::string>{"TZ1"},
      ret = cfg->optionalProperty(tzPropName));
}

TEST_F(BaseVeloxQueryConfigTest, fromSystemConfig) {
#define GET_VAL(_name_) cfg->optionalProperty(std::string(_name_))

  auto cfg = BaseVeloxQueryConfig::instance();
  ASSERT_EQ("false", GET_VAL(QueryConfig::kPrestoArrayAggIgnoreNulls));

  setUpConfigFile(true, true);
  cfg->initialize(configFilePath);

  ASSERT_EQ("true", GET_VAL(QueryConfig::kPrestoArrayAggIgnoreNulls));
  ASSERT_EQ("17825792", GET_VAL(QueryConfig::kMaxArbitraryBufferSize));
  ASSERT_EQ("6291456", GET_VAL(QueryConfig::kMaxPartitionedOutputBufferSize));

#undef GET_VAL
}

} // namespace facebook::presto::test
