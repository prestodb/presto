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

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/ConfigReader.h"

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

namespace facebook::presto {

ConfigBase::ConfigBase()
    : config_(std::make_unique<velox::core::MemConfig>()) {}

void ConfigBase::initialize(const std::string& filePath) {
  // See if we want to create a mutable config.
  auto values = util::readConfig(fs::path(filePath));
  bool mutableConfig{false};
  auto it = values.find(std::string(SystemConfig::kMutableConfig));
  if (it != values.end()) {
    mutableConfig = folly::to<bool>(it->second);
  }

  if (mutableConfig) {
    config_ = std::make_unique<velox::core::MemConfigMutable>(values);
  } else {
    config_ = std::make_unique<velox::core::MemConfig>(values);
  };

  filePath_ = filePath;
}

folly::Optional<std::string> ConfigBase::setValue(
    const std::string& propertyName,
    const std::string& value) {
  if (auto* memConfig =
          dynamic_cast<velox::core::MemConfigMutable*>(config_.get())) {
    auto oldValue = config_->get(propertyName);
    memConfig->setValue(propertyName, value);
    return oldValue;
  }
  VELOX_USER_FAIL(
      "Config is not mutable. Consider setting {} to 'true'.",
      SystemConfig::kMutableConfig);
}

SystemConfig* SystemConfig::instance() {
  static std::unique_ptr<SystemConfig> instance =
      std::make_unique<SystemConfig>();
  return instance.get();
}

int SystemConfig::httpServerHttpPort() const {
  return requiredProperty<int>(std::string(kHttpServerHttpPort));
}

bool SystemConfig::httpServerReusePort() const {
  auto opt = optionalProperty<bool>(std::string(kHttpServerReusePort));
  return opt.value_or(kHttpServerReusePortDefault);
}

int SystemConfig::httpServerHttpsPort() const {
  return requiredProperty<int>(std::string(kHttpServerHttpsPort));
}

bool SystemConfig::enableHttps() const {
  auto opt = optionalProperty<bool>(std::string(kHttpServerHttpsEnabled));
  return opt.value_or(kHttpServerHttpsEnabledDefault);
}

std::string SystemConfig::httpsSupportedCiphers() const {
  auto opt = optionalProperty<std::string>(std::string(kHttpsSupportedCiphers));
  return opt.value_or(std::string(kHttpsSupportedCiphersDefault));
}

std::optional<std::string> SystemConfig::httpsCertPath() const {
  return static_cast<std::optional<std::string>>(
      optionalProperty<std::string>(std::string(kHttpsCertPath)));
}

std::optional<std::string> SystemConfig::httpsKeyPath() const {
  return static_cast<std::optional<std::string>>(
      optionalProperty<std::string>(std::string(kHttpsKeyPath)));
}

std::optional<std::string> SystemConfig::httpsClientCertAndKeyPath() const {
  return static_cast<std::optional<std::string>>(
      optionalProperty<std::string>(std::string(kHttpsClientCertAndKeyPath)));
}

std::string SystemConfig::prestoVersion() const {
  return requiredProperty(std::string(kPrestoVersion));
}

std::optional<std::string> SystemConfig::discoveryUri() const {
  return static_cast<std::optional<std::string>>(
      optionalProperty<std::string>(std::string(kDiscoveryUri)));
}

int32_t SystemConfig::maxDriversPerTask() const {
  auto opt = optionalProperty<int32_t>(std::string(kMaxDriversPerTask));
  return opt.value_or(kMaxDriversPerTaskDefault);
}

int32_t SystemConfig::concurrentLifespansPerTask() const {
  auto opt =
      optionalProperty<int32_t>(std::string(kConcurrentLifespansPerTask));
  return opt.value_or(kConcurrentLifespansPerTaskDefault);
}

int32_t SystemConfig::httpExecThreads() const {
  auto opt = optionalProperty<int32_t>(std::string(kHttpExecThreads));
  return opt.value_or(kHttpExecThreadsDefault);
}

int32_t SystemConfig::numIoThreads() const {
  auto opt = optionalProperty<int32_t>(std::string(kNumIoThreads));
  return opt.value_or(kNumIoThreadsDefault);
}

int32_t SystemConfig::numQueryThreads() const {
  auto opt = optionalProperty<int32_t>(std::string(kNumQueryThreads));
  return opt.value_or(std::thread::hardware_concurrency() * 4);
}

int32_t SystemConfig::numSpillThreads() const {
  auto opt = optionalProperty<int32_t>(std::string(kNumSpillThreads));
  return opt.hasValue() ? opt.value() : std::thread::hardware_concurrency();
}

std::string SystemConfig::spillerSpillPath() const {
  auto opt = optionalProperty<std::string>(std::string(kSpillerSpillPath));
  return opt.hasValue() ? opt.value() : "";
}

int32_t SystemConfig::shutdownOnsetSec() const {
  auto opt = optionalProperty<int32_t>(std::string(kShutdownOnsetSec));
  return opt.value_or(kShutdownOnsetSecDefault);
}

int32_t SystemConfig::systemMemoryGb() const {
  auto opt = optionalProperty<int32_t>(std::string(kSystemMemoryGb));
  return opt.value_or(kSystemMemoryGbDefault);
}

uint64_t SystemConfig::asyncCacheSsdGb() const {
  auto opt = optionalProperty<uint64_t>(std::string(kAsyncCacheSsdGb));
  return opt.value_or(kAsyncCacheSsdGbDefault);
}

uint64_t SystemConfig::asyncCacheSsdCheckpointGb() const {
  auto opt =
      optionalProperty<uint64_t>(std::string(kAsyncCacheSsdCheckpointGb));
  return opt.value_or(kAsyncCacheSsdCheckpointGbDefault);
}

uint64_t SystemConfig::localShuffleMaxPartitionBytes() const {
  auto opt =
      optionalProperty<uint32_t>(std::string(kLocalShuffleMaxPartitionBytes));
  return opt.value_or(kLocalShuffleMaxPartitionBytesDefault);
}

std::string SystemConfig::asyncCacheSsdPath() const {
  auto opt = optionalProperty<std::string>(std::string(kAsyncCacheSsdPath));
  return opt.hasValue() ? opt.value() : std::string(kAsyncCacheSsdPathDefault);
}

bool SystemConfig::asyncCacheSsdDisableFileCow() const {
  auto opt = optionalProperty<bool>(std::string(kAsyncCacheSsdDisableFileCow));
  return opt.value_or(kAsyncCacheSsdDisableFileCowDefault);
}

std::string SystemConfig::shuffleName() const {
  auto opt = optionalProperty<std::string>(std::string(kShuffleName));
  return opt.hasValue() ? opt.value() : std::string(kShuffleNameDefault);
}

bool SystemConfig::enableSerializedPageChecksum() const {
  auto opt = optionalProperty<bool>(std::string(kEnableSerializedPageChecksum));
  return opt.value_or(kEnableSerializedPageChecksumDefault);
}

bool SystemConfig::enableVeloxTaskLogging() const {
  auto opt = optionalProperty<bool>(std::string(kEnableVeloxTaskLogging));
  return opt.value_or(kEnableVeloxTaskLoggingDefault);
}

bool SystemConfig::enableVeloxExprSetLogging() const {
  auto opt = optionalProperty<bool>(std::string(kEnableVeloxExprSetLogging));
  return opt.value_or(kEnableVeloxExprSetLoggingDefault);
}

bool SystemConfig::useMmapArena() const {
  auto opt = optionalProperty<bool>(std::string(kUseMmapArena));
  return opt.value_or(kUseMmapArenaDefault);
}

int32_t SystemConfig::mmapArenaCapacityRatio() const {
  auto opt = optionalProperty<int32_t>(std::string(kMmapArenaCapacityRatio));
  return opt.hasValue() ? opt.value() : kMmapArenaCapacityRatioDefault;
}

bool SystemConfig::useMmapAllocator() const {
  auto opt = optionalProperty<bool>(std::string(kUseMmapAllocator));
  return opt.value_or(kUseMmapAllocatorDefault);
}

bool SystemConfig::enableHttpAccessLog() const {
  auto opt = optionalProperty<bool>(std::string(kHttpEnableAccessLog));
  return opt.value_or(kHttpEnableAccessLogDefault);
}

bool SystemConfig::enableHttpStatsFilter() const {
  auto opt = optionalProperty<bool>(std::string(kHttpEnableStatFilter));
  return opt.value_or(kHttpEnableStatsFilterDefault);
}

bool SystemConfig::registerTestFunctions() const {
  auto opt = optionalProperty<bool>(std::string(kRegisterTestFunctions));
  return opt.value_or(kRegisterTestFunctionsDefault);
}

uint64_t SystemConfig::httpMaxAllocateBytes() const {
  auto opt = optionalProperty<uint64_t>(std::string(kHttpMaxAllocateBytes));
  return opt.value_or(kHttpMaxAllocateBytesDefault);
}

NodeConfig* NodeConfig::instance() {
  static std::unique_ptr<NodeConfig> instance = std::make_unique<NodeConfig>();
  return instance.get();
}

std::string NodeConfig::nodeEnvironment() const {
  return requiredProperty(std::string(kNodeEnvironment));
}

std::string NodeConfig::nodeId() const {
  return requiredProperty(std::string(kNodeId));
}

std::string NodeConfig::nodeLocation() const {
  return requiredProperty(std::string(kNodeLocation));
}

std::string NodeConfig::nodeIp(
    const std::function<std::string()>& defaultIp) const {
  auto resultOpt = optionalProperty(std::string(kNodeIp));
  if (resultOpt.has_value()) {
    return resultOpt.value();
  } else if (defaultIp != nullptr) {
    return defaultIp();
  } else {
    VELOX_FAIL(
        "Node IP was not found in NodeConfigs. Default IP was not provided "
        "either.");
  }
}

uint64_t NodeConfig::nodeMemoryGb(
    const std::function<uint64_t()>& defaultNodeMemoryGb) const {
  auto resultOpt = optionalProperty<uint64_t>(std::string(kNodeMemoryGb));
  uint64_t result = 0;
  if (resultOpt.has_value()) {
    result = resultOpt.value();
  } else if (defaultNodeMemoryGb != nullptr) {
    result = defaultNodeMemoryGb();
  } else {
    VELOX_FAIL(
        "Node memory GB was not found in NodeConfigs. Default node memory was "
        "not provided either.");
  }
  if (result == 0) {
    LOG(ERROR) << "Bad node memory size.";
    exit(1);
  }
  return result;
}

} // namespace facebook::presto
