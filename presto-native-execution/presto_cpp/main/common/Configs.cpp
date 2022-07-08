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
  config_ = std::make_unique<velox::core::MemConfig>(
      util::readConfig(fs::path(filePath)));
  filePath_ = filePath;
}

SystemConfig* SystemConfig::instance() {
  static std::unique_ptr<SystemConfig> instance =
      std::make_unique<SystemConfig>();
  return instance.get();
}

int SystemConfig::httpServerHttpPort() const {
  return requiredProperty<int>(std::string(kHttpServerHttpPort));
}

std::string SystemConfig::prestoVersion() const {
  return requiredProperty(std::string(kPrestoVersion));
}

std::string SystemConfig::discoveryUri() const {
  return requiredProperty(std::string(kDiscoveryUri));
}

int32_t SystemConfig::maxDriversPerTask() const {
  auto opt = optionalProperty<int32_t>(std::string(kMaxDriversPerTask));
  if (opt.has_value()) {
    return opt.value();
  } else {
    return kMaxDriversPerTaskDefault;
  }
}

int32_t SystemConfig::concurrentLifespansPerTask() const {
  auto opt =
      optionalProperty<int32_t>(std::string(kConcurrentLifespansPerTask));
  if (opt.has_value()) {
    return opt.value();
  } else {
    return kConcurrentLifespansPerTaskDefault;
  }
}

int32_t SystemConfig::httpExecThreads() const {
  auto threadsOpt = optionalProperty<int32_t>(std::string(kHttpExecThreads));
  return threadsOpt.hasValue() ? threadsOpt.value() : kHttpExecThreadsDefault;
}

int32_t SystemConfig::numIoThreads() const {
  auto opt = optionalProperty<int32_t>(std::string(kNumIoThreads));
  return opt.hasValue() ? opt.value() : kNumIoThreadsDefault;
}

int32_t SystemConfig::shutdownOnsetSec() const {
  auto opt = optionalProperty<int32_t>(std::string(kShutdownOnsetSec));
  return opt.hasValue() ? opt.value() : kShutdownOnsetSecDefault;
}

int32_t SystemConfig::systemMemoryGb() const {
  auto opt = optionalProperty<int32_t>(std::string(kSystemMemoryGb));
  return opt.hasValue() ? opt.value() : kSystemMemoryGbDefault;
}

int32_t SystemConfig::asyncCacheSsdGb() const {
  auto opt = optionalProperty<int32_t>(std::string(kAsyncCacheSsdGb));
  return opt.hasValue() ? opt.value() : kAsyncCacheSsdGbDefault;
}

std::string_view SystemConfig::asyncCacheSsdPath() const {
  auto opt =
      optionalProperty<std::string_view>(std::string(kAsyncCacheSsdPath));
  return opt.hasValue() ? opt.value() : kAsyncCacheSsdPathDefault;
}

bool SystemConfig::enableSerializedPageChecksum() const {
  auto opt = optionalProperty<bool>(std::string(kEnableSerializedPageChecksum));
  return opt.hasValue() ? opt.value() : kEnableSerializedPageChecksumDefault;
}

bool SystemConfig::enableVeloxTaskLogging() const {
  auto loggingOpt =
      optionalProperty<bool>(std::string(kEnableVeloxTaskLogging));
  return loggingOpt.hasValue() ? loggingOpt.value()
                               : kEnableVeloxTaskLoggingDefault;
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
