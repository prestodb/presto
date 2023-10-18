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

#include <re2/re2.h>

#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "velox/core/QueryConfig.h"

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

namespace facebook::presto {

namespace {

// folly::to<> does not generate 'true' and 'false', so we do it ourselves.
std::string bool2String(bool value) {
  return value ? "true" : "false";
}

#define STR_PROP(_key_, _val_) \
  { std::string(_key_), std::string(_val_) }
#define NUM_PROP(_key_, _val_) \
  { std::string(_key_), folly::to<std::string>(_val_) }
#define BOOL_PROP(_key_, _val_) \
  { std::string(_key_), bool2String(_val_) }
#define NONE_PROP(_key_) \
  { std::string(_key_), folly::none }
} // namespace

ConfigBase::ConfigBase()
    : config_(std::make_unique<velox::core::MemConfig>()) {}

void ConfigBase::initialize(const std::string& filePath) {
  // See if we want to create a mutable config.
  auto values = util::readConfig(fs::path(filePath));
  filePath_ = filePath;
  checkRegisteredProperties(values);

  bool mutableConfig{false};
  auto it = values.find(std::string(kMutableConfig));
  if (it != values.end()) {
    mutableConfig = folly::to<bool>(it->second);
  }

  if (mutableConfig) {
    config_ = std::make_unique<velox::core::MemConfigMutable>(values);
  } else {
    config_ = std::make_unique<velox::core::MemConfig>(values);
  };
}

bool ConfigBase::registerProperty(
    const std::string& propertyName,
    const folly::Optional<std::string>& defaultValue) {
  if (registeredProps_.count(propertyName) != 0) {
    PRESTO_STARTUP_LOG(WARNING)
        << "Property '" << propertyName
        << "' is already registered with default value '"
        << registeredProps_[propertyName].value_or("<none>") << "'.";
    return false;
  }

  registeredProps_[propertyName] = defaultValue;
  return true;
}

folly::Optional<std::string> ConfigBase::setValue(
    const std::string& propertyName,
    const std::string& value) {
  VELOX_USER_CHECK_EQ(
      1,
      registeredProps_.count(propertyName),
      "Property '{}' is not registered in the config.",
      propertyName);
  if (auto* memConfig =
          dynamic_cast<velox::core::MemConfigMutable*>(config_.get())) {
    auto oldValue = config_->get(propertyName);
    memConfig->setValue(propertyName, value);
    if (oldValue.hasValue()) {
      return oldValue;
    }
    return registeredProps_[propertyName];
  }
  VELOX_USER_FAIL(
      "Config is not mutable. Consider setting '{}' to 'true'.",
      kMutableConfig);
}

void ConfigBase::checkRegisteredProperties(
    const std::unordered_map<std::string, std::string>& values) {
  std::stringstream supported;
  std::stringstream unsupported;
  for (const auto& pair : values) {
    ((registeredProps_.count(pair.first) != 0) ? supported : unsupported)
        << "  " << pair.first << "=" << pair.second << "\n";
  }
  auto str = supported.str();
  if (!str.empty()) {
    PRESTO_STARTUP_LOG(INFO)
        << "Registered properties from '" << filePath_ << "':\n"
        << str;
  }
  str = unsupported.str();
  if (!str.empty()) {
    PRESTO_STARTUP_LOG(WARNING)
        << "Unregistered properties from '" << filePath_ << "':\n"
        << str;
  }
}

SystemConfig::SystemConfig() {
  registeredProps_ =
      std::unordered_map<std::string, folly::Optional<std::string>>{
          STR_PROP(kMutableConfig, "false"),
          NONE_PROP(kPrestoVersion),
          NONE_PROP(kHttpServerHttpPort),
          STR_PROP(kHttpServerReusePort, "false"),
          NONE_PROP(kDiscoveryUri),
          NUM_PROP(kMaxDriversPerTask, 16),
          NUM_PROP(kConcurrentLifespansPerTask, 1),
          NUM_PROP(kHttpExecThreads, 8),
          NONE_PROP(kHttpServerHttpsPort),
          STR_PROP(kHttpServerHttpsEnabled, "false"),
          STR_PROP(
              kHttpsSupportedCiphers,
              "ECDHE-ECDSA-AES256-GCM-SHA384,AES256-GCM-SHA384"),
          NONE_PROP(kHttpsCertPath),
          NONE_PROP(kHttpsKeyPath),
          NONE_PROP(kHttpsClientCertAndKeyPath),
          NUM_PROP(kNumIoThreads, 30),
          NUM_PROP(kNumConnectorIoThreads, 30),
          NUM_PROP(kNumQueryThreads, std::thread::hardware_concurrency() * 4),
          NUM_PROP(kNumSpillThreads, std::thread::hardware_concurrency()),
          NONE_PROP(kSpillerSpillPath),
          NUM_PROP(kShutdownOnsetSec, 10),
          NUM_PROP(kSystemMemoryGb, 40),
          STR_PROP(kAsyncDataCacheEnabled, "true"),
          NUM_PROP(kAsyncCacheSsdGb, 0),
          NUM_PROP(kAsyncCacheSsdCheckpointGb, 0),
          STR_PROP(kAsyncCacheSsdPath, "/mnt/flash/async_cache."),
          STR_PROP(kAsyncCacheSsdDisableFileCow, "false"),
          STR_PROP(kEnableSerializedPageChecksum, "true"),
          STR_PROP(kUseMmapArena, "false"),
          NUM_PROP(kMmapArenaCapacityRatio, 10),
          STR_PROP(kUseMmapAllocator, "true"),
          STR_PROP(kMemoryArbitratorKind, ""),
          NUM_PROP(kQueryMemoryGb, 38),
          STR_PROP(kEnableVeloxTaskLogging, "false"),
          STR_PROP(kEnableVeloxExprSetLogging, "false"),
          NUM_PROP(kLocalShuffleMaxPartitionBytes, 268435456),
          STR_PROP(kShuffleName, ""),
          STR_PROP(kRemoteFunctionServerCatalogName, ""),
          STR_PROP(kHttpEnableAccessLog, "false"),
          STR_PROP(kHttpEnableStatsFilter, "false"),
          STR_PROP(kHttpEnableEndpointLatencyFilter, "false"),
          STR_PROP(kRegisterTestFunctions, "false"),
          NUM_PROP(kHttpMaxAllocateBytes, 65536),
          STR_PROP(kQueryMaxMemoryPerNode, "4GB"),
          STR_PROP(kEnableMemoryLeakCheck, "true"),
          NONE_PROP(kRemoteFunctionServerThriftPort),
          STR_PROP(kSkipRuntimeStatsInRunningTaskInfo, "true"),
          STR_PROP(kLogZombieTaskInfo, "false"),
          NUM_PROP(kLogNumZombieTasks, 20),
          NUM_PROP(kAnnouncementMaxFrequencyMs, 30'000), // 30s
          NUM_PROP(kHeartbeatFrequencyMs, 0),
          STR_PROP(kExchangeMaxErrorDuration, "30s"),
          STR_PROP(kExchangeRequestTimeout, "10s"),
          BOOL_PROP(kExchangeImmediateBufferTransfer, true),
          NUM_PROP(kTaskRunTimeSliceMicros, 50'000),
          BOOL_PROP(kIncludeNodeInSpillPath, false),
          NUM_PROP(kOldTaskCleanUpMs, 60'000),
          STR_PROP(kInternalCommunicationJwtEnabled, "false"),
          STR_PROP(kInternalCommunicationSharedSecret, ""),
          NUM_PROP(kInternalCommunicationJwtExpirationSeconds, 300),
      };
}

SystemConfig* SystemConfig::instance() {
  static std::unique_ptr<SystemConfig> instance =
      std::make_unique<SystemConfig>();
  return instance.get();
}

int SystemConfig::httpServerHttpPort() const {
  return requiredProperty<int>(kHttpServerHttpPort);
}

bool SystemConfig::httpServerReusePort() const {
  return optionalProperty<bool>(kHttpServerReusePort).value();
}

int SystemConfig::httpServerHttpsPort() const {
  return requiredProperty<int>(kHttpServerHttpsPort);
}

bool SystemConfig::httpServerHttpsEnabled() const {
  return optionalProperty<bool>(kHttpServerHttpsEnabled).value();
}

std::string SystemConfig::httpsSupportedCiphers() const {
  return optionalProperty(kHttpsSupportedCiphers).value();
}

folly::Optional<std::string> SystemConfig::httpsCertPath() const {
  return optionalProperty(kHttpsCertPath);
}

folly::Optional<std::string> SystemConfig::httpsKeyPath() const {
  return optionalProperty(kHttpsKeyPath);
}

folly::Optional<std::string> SystemConfig::httpsClientCertAndKeyPath() const {
  return optionalProperty(kHttpsClientCertAndKeyPath);
}

std::string SystemConfig::prestoVersion() const {
  return requiredProperty(std::string(kPrestoVersion));
}

bool SystemConfig::mutableConfig() const {
  return optionalProperty<bool>(kMutableConfig).value();
}

folly::Optional<std::string> SystemConfig::discoveryUri() const {
  return optionalProperty(kDiscoveryUri);
}

folly::Optional<folly::SocketAddress>
SystemConfig::remoteFunctionServerLocation() const {
  // First check if there is a UDS path registered. If there's one, use it.
  auto remoteServerUdsPath =
      optionalProperty(kRemoteFunctionServerThriftUdsPath);
  if (remoteServerUdsPath.hasValue()) {
    return folly::SocketAddress::makeFromPath(remoteServerUdsPath.value());
  }

  // Otherwise, check for address and port parameters.
  auto remoteServerAddress =
      optionalProperty(kRemoteFunctionServerThriftAddress);
  auto remoteServerPort =
      optionalProperty<uint16_t>(kRemoteFunctionServerThriftPort);

  if (remoteServerPort.hasValue()) {
    // Fallback to localhost if address is not specified.
    return remoteServerAddress.hasValue()
        ? folly::
              SocketAddress{remoteServerAddress.value(), remoteServerPort.value()}
        : folly::SocketAddress{"::1", remoteServerPort.value()};
  } else if (remoteServerAddress.hasValue()) {
    VELOX_FAIL(
        "Remote function server port not provided using '{}'.",
        kRemoteFunctionServerThriftPort);
  }

  // No remote function server configured.
  return folly::none;
}

folly::Optional<std::string>
SystemConfig::remoteFunctionServerSignatureFilesDirectoryPath() const {
  return optionalProperty(kRemoteFunctionServerSignatureFilesDirectoryPath);
}

std::string SystemConfig::remoteFunctionServerCatalogName() const {
  return optionalProperty(kRemoteFunctionServerCatalogName).value();
}

int32_t SystemConfig::maxDriversPerTask() const {
  return optionalProperty<int32_t>(kMaxDriversPerTask).value();
}

int32_t SystemConfig::concurrentLifespansPerTask() const {
  return optionalProperty<int32_t>(kConcurrentLifespansPerTask).value();
}

int32_t SystemConfig::httpExecThreads() const {
  return optionalProperty<int32_t>(kHttpExecThreads).value();
}

int32_t SystemConfig::numIoThreads() const {
  return optionalProperty<int32_t>(kNumIoThreads).value();
}

int32_t SystemConfig::numConnectorIoThreads() const {
  return optionalProperty<int32_t>(kNumConnectorIoThreads).value();
}

int32_t SystemConfig::numQueryThreads() const {
  return optionalProperty<int32_t>(kNumQueryThreads).value();
}

int32_t SystemConfig::numSpillThreads() const {
  return optionalProperty<int32_t>(kNumSpillThreads).value();
}

folly::Optional<std::string> SystemConfig::spillerSpillPath() const {
  return optionalProperty(kSpillerSpillPath);
}

int32_t SystemConfig::shutdownOnsetSec() const {
  return optionalProperty<int32_t>(kShutdownOnsetSec).value();
}

int32_t SystemConfig::systemMemoryGb() const {
  return optionalProperty<int32_t>(kSystemMemoryGb).value();
}

uint64_t SystemConfig::asyncCacheSsdGb() const {
  return optionalProperty<uint64_t>(kAsyncCacheSsdGb).value();
}

bool SystemConfig::asyncDataCacheEnabled() const {
  return optionalProperty<bool>(kAsyncDataCacheEnabled).value();
}

uint64_t SystemConfig::asyncCacheSsdCheckpointGb() const {
  return optionalProperty<uint64_t>(kAsyncCacheSsdCheckpointGb).value();
}

uint64_t SystemConfig::localShuffleMaxPartitionBytes() const {
  return optionalProperty<uint32_t>(kLocalShuffleMaxPartitionBytes).value();
}

std::string SystemConfig::asyncCacheSsdPath() const {
  return optionalProperty(kAsyncCacheSsdPath).value();
}

bool SystemConfig::asyncCacheSsdDisableFileCow() const {
  return optionalProperty<bool>(kAsyncCacheSsdDisableFileCow).value();
}

std::string SystemConfig::shuffleName() const {
  return optionalProperty(kShuffleName).value();
}

bool SystemConfig::enableSerializedPageChecksum() const {
  return optionalProperty<bool>(kEnableSerializedPageChecksum).value();
}

bool SystemConfig::enableVeloxTaskLogging() const {
  return optionalProperty<bool>(kEnableVeloxTaskLogging).value();
}

bool SystemConfig::enableVeloxExprSetLogging() const {
  return optionalProperty<bool>(kEnableVeloxExprSetLogging).value();
}

bool SystemConfig::useMmapArena() const {
  return optionalProperty<bool>(kUseMmapArena).value();
}

int32_t SystemConfig::mmapArenaCapacityRatio() const {
  return optionalProperty<int32_t>(kMmapArenaCapacityRatio).value();
}

bool SystemConfig::useMmapAllocator() const {
  return optionalProperty<bool>(kUseMmapAllocator).value();
}

std::string SystemConfig::memoryArbitratorKind() const {
  return optionalProperty<std::string>(kMemoryArbitratorKind).value_or("");
}

int32_t SystemConfig::queryMemoryGb() const {
  return optionalProperty<int32_t>(kQueryMemoryGb).value();
}

uint64_t SystemConfig::memoryPoolInitCapacity() const {
  static constexpr uint64_t kMemoryPoolInitCapacityDefault = 128 << 20;
  return optionalProperty<uint64_t>(kMemoryPoolInitCapacity)
      .value_or(kMemoryPoolInitCapacityDefault);
}

uint64_t SystemConfig::memoryPoolTransferCapacity() const {
  static constexpr uint64_t kMemoryPoolTransferCapacityDefault = 32 << 20;
  return optionalProperty<uint64_t>(kMemoryPoolTransferCapacity)
      .value_or(kMemoryPoolTransferCapacityDefault);
}

bool SystemConfig::enableSystemMemoryPoolUsageTracking() const {
  return optionalProperty<bool>(kEnableSystemMemoryPoolUsageTracking)
      .value_or(true);
}

bool SystemConfig::enableHttpAccessLog() const {
  return optionalProperty<bool>(kHttpEnableAccessLog).value();
}

bool SystemConfig::enableHttpStatsFilter() const {
  return optionalProperty<bool>(kHttpEnableStatsFilter).value();
}

bool SystemConfig::enableHttpEndpointLatencyFilter() const {
  return optionalProperty<bool>(kHttpEnableEndpointLatencyFilter).value();
}

bool SystemConfig::registerTestFunctions() const {
  return optionalProperty<bool>(kRegisterTestFunctions).value();
}

uint64_t SystemConfig::httpMaxAllocateBytes() const {
  return optionalProperty<uint64_t>(kHttpMaxAllocateBytes).value();
}

uint64_t SystemConfig::queryMaxMemoryPerNode() const {
  return toCapacity(
      optionalProperty(kQueryMaxMemoryPerNode).value(),
      velox::core::CapacityUnit::BYTE);
}

bool SystemConfig::enableMemoryLeakCheck() const {
  return optionalProperty<bool>(kEnableMemoryLeakCheck).value();
}

bool SystemConfig::skipRuntimeStatsInRunningTaskInfo() const {
  return optionalProperty<bool>(kSkipRuntimeStatsInRunningTaskInfo).value();
}

bool SystemConfig::logZombieTaskInfo() const {
  return optionalProperty<bool>(kLogZombieTaskInfo).value();
}

uint32_t SystemConfig::logNumZombieTasks() const {
  return optionalProperty<uint32_t>(kLogNumZombieTasks).value();
}

uint64_t SystemConfig::announcementMaxFrequencyMs() const {
  return optionalProperty<uint64_t>(kAnnouncementMaxFrequencyMs).value();
}

uint64_t SystemConfig::heartbeatFrequencyMs() const {
  return optionalProperty<uint64_t>(kHeartbeatFrequencyMs).value();
}

std::chrono::duration<double> SystemConfig::exchangeMaxErrorDuration() const {
  return velox::core::toDuration(
      optionalProperty(kExchangeMaxErrorDuration).value());
}

std::chrono::duration<double> SystemConfig::exchangeRequestTimeout() const {
  return velox::core::toDuration(
      optionalProperty(kExchangeRequestTimeout).value());
}

bool SystemConfig::exchangeImmediateBufferTransfer() const {
  return optionalProperty<bool>(kExchangeImmediateBufferTransfer).value();
}

int32_t SystemConfig::taskRunTimeSliceMicros() const {
  return optionalProperty<int32_t>(kTaskRunTimeSliceMicros).value();
}

bool SystemConfig::includeNodeInSpillPath() const {
  return optionalProperty<bool>(kIncludeNodeInSpillPath).value();
}

int32_t SystemConfig::oldTaskCleanUpMs() const {
  return optionalProperty<int32_t>(kOldTaskCleanUpMs).value();
}

// The next three toggles govern the use of JWT for authentication
// for communication between the cluster nodes.
bool SystemConfig::internalCommunicationJwtEnabled() const {
  return optionalProperty<bool>(kInternalCommunicationJwtEnabled).value();
}

std::string SystemConfig::internalCommunicationSharedSecret() const {
  return optionalProperty(kInternalCommunicationSharedSecret).value();
}

int32_t SystemConfig::internalCommunicationJwtExpirationSeconds() const {
  return optionalProperty<int32_t>(kInternalCommunicationJwtExpirationSeconds)
      .value();
}

NodeConfig::NodeConfig() {
  registeredProps_ =
      std::unordered_map<std::string, folly::Optional<std::string>>{
          NONE_PROP(kNodeEnvironment),
          NONE_PROP(kNodeId),
          NONE_PROP(kNodeIp),
          NONE_PROP(kNodeInternalAddress),
          NONE_PROP(kNodeLocation),
          NONE_PROP(kNodeMemoryGb),
      };
}

NodeConfig* NodeConfig::instance() {
  static std::unique_ptr<NodeConfig> instance = std::make_unique<NodeConfig>();
  return instance.get();
}

std::string NodeConfig::nodeEnvironment() const {
  return requiredProperty(kNodeEnvironment);
}

std::string NodeConfig::nodeId() const {
  return requiredProperty(kNodeId);
}

std::string NodeConfig::nodeLocation() const {
  return requiredProperty(kNodeLocation);
}

std::string NodeConfig::nodeInternalAddress(
    const std::function<std::string()>& defaultIp) const {
  auto resultOpt = optionalProperty(kNodeInternalAddress);
  /// node.ip(kNodeIp) is legacy config replaced with node.internal-address, but
  /// still valid config in Presto, so handling both.
  if (!resultOpt.hasValue()) {
    resultOpt = optionalProperty(kNodeIp);
  }
  if (resultOpt.has_value()) {
    return resultOpt.value();
  } else if (defaultIp != nullptr) {
    return defaultIp();
  } else {
    VELOX_FAIL(
        "Node Internal Address or IP was not found in NodeConfigs. Default IP was not provided "
        "either.");
  }
}

uint64_t NodeConfig::nodeMemoryGb(
    const std::function<uint64_t()>& defaultNodeMemoryGb) const {
  auto resultOpt = optionalProperty<uint64_t>(kNodeMemoryGb);
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

BaseVeloxQueryConfig::BaseVeloxQueryConfig() {
  // Use empty instance to get default property values.
  velox::core::QueryConfig c{{}};
  using namespace velox::core;
  registeredProps_ =
      std::unordered_map<std::string, folly::Optional<std::string>>{
          STR_PROP(kMutableConfig, "false"),
          BOOL_PROP(QueryConfig::kCodegenEnabled, c.codegenEnabled()),
          STR_PROP(
              QueryConfig::kCodegenConfigurationFilePath,
              c.codegenConfigurationFilePath()),
          BOOL_PROP(QueryConfig::kCodegenLazyLoading, c.codegenLazyLoading()),
          STR_PROP(QueryConfig::kSessionTimezone, c.sessionTimezone()),
          BOOL_PROP(
              QueryConfig::kAdjustTimestampToTimezone,
              c.adjustTimestampToTimezone()),
          BOOL_PROP(QueryConfig::kExprEvalSimplified, c.exprEvalSimplified()),
          BOOL_PROP(QueryConfig::kExprTrackCpuUsage, c.exprTrackCpuUsage()),
          BOOL_PROP(
              QueryConfig::kOperatorTrackCpuUsage, c.operatorTrackCpuUsage()),
          BOOL_PROP(
              QueryConfig::kCastMatchStructByName, c.isMatchStructByName()),
          BOOL_PROP(
              QueryConfig::kCastToIntByTruncate, c.isCastToIntByTruncate()),
          NUM_PROP(
              QueryConfig::kMaxLocalExchangeBufferSize,
              c.maxLocalExchangeBufferSize()),
          NUM_PROP(
              QueryConfig::kMaxPartialAggregationMemory,
              c.maxPartialAggregationMemoryUsage()),
          NUM_PROP(
              QueryConfig::kMaxExtendedPartialAggregationMemory,
              c.maxExtendedPartialAggregationMemoryUsage()),
          NUM_PROP(
              QueryConfig::kAbandonPartialAggregationMinRows,
              c.abandonPartialAggregationMinRows()),
          NUM_PROP(
              QueryConfig::kAbandonPartialAggregationMinPct,
              c.abandonPartialAggregationMinPct()),
          NUM_PROP(
              QueryConfig::kMaxPartitionedOutputBufferSize,
              c.maxPartitionedOutputBufferSize()),
          NUM_PROP(
              QueryConfig::kPreferredOutputBatchBytes,
              c.preferredOutputBatchBytes()),
          NUM_PROP(
              QueryConfig::kPreferredOutputBatchRows,
              c.preferredOutputBatchRows()),
          NUM_PROP(QueryConfig::kMaxOutputBatchRows, c.maxOutputBatchRows()),
          BOOL_PROP(
              QueryConfig::kHashAdaptivityEnabled, c.hashAdaptivityEnabled()),
          BOOL_PROP(
              QueryConfig::kAdaptiveFilterReorderingEnabled,
              c.adaptiveFilterReorderingEnabled()),
          BOOL_PROP(QueryConfig::kSpillEnabled, c.spillEnabled()),
          BOOL_PROP(
              QueryConfig::kAggregationSpillEnabled,
              c.aggregationSpillEnabled()),
          BOOL_PROP(QueryConfig::kAggregationSpillAll, c.aggregationSpillAll()),
          BOOL_PROP(QueryConfig::kJoinSpillEnabled, c.joinSpillEnabled()),
          BOOL_PROP(QueryConfig::kOrderBySpillEnabled, c.orderBySpillEnabled()),
          NUM_PROP(
              QueryConfig::kAggregationSpillMemoryThreshold,
              c.aggregationSpillMemoryThreshold()),
          NUM_PROP(
              QueryConfig::kJoinSpillMemoryThreshold,
              c.joinSpillMemoryThreshold()),
          NUM_PROP(
              QueryConfig::kOrderBySpillMemoryThreshold,
              c.orderBySpillMemoryThreshold()),
          NUM_PROP(QueryConfig::kTestingSpillPct, c.testingSpillPct()),
          NUM_PROP(QueryConfig::kMaxSpillLevel, c.maxSpillLevel()),
          NUM_PROP(QueryConfig::kMaxSpillFileSize, c.maxSpillFileSize()),
          NUM_PROP(QueryConfig::kMinSpillRunSize, c.minSpillRunSize()),
          NUM_PROP(
              QueryConfig::kSpillStartPartitionBit, c.spillStartPartitionBit()),
          NUM_PROP(
              QueryConfig::kJoinSpillPartitionBits, c.joinSpillPartitionBits()),
          NUM_PROP(
              QueryConfig::kAggregationSpillPartitionBits,
              c.aggregationSpillPartitionBits()),
          NUM_PROP(
              QueryConfig::kSpillableReservationGrowthPct,
              c.spillableReservationGrowthPct()),
          BOOL_PROP(
              QueryConfig::kSparkLegacySizeOfNull, c.sparkLegacySizeOfNull()),
      };
}

BaseVeloxQueryConfig* BaseVeloxQueryConfig::instance() {
  static std::unique_ptr<BaseVeloxQueryConfig> instance =
      std::make_unique<BaseVeloxQueryConfig>();
  return instance.get();
}

} // namespace facebook::presto
