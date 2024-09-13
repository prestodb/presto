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
#include "presto_cpp/main/common/Utils.h"
#include "velox/core/QueryConfig.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
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

void ConfigBase::initialize(const std::string& filePath, bool optionalConfig) {
  auto path = fs::path(filePath);
  std::unordered_map<std::string, std::string> values;
  if (!optionalConfig || fs::exists(path)) {
    // See if we want to create a mutable config.
    values = util::readConfig(path);
  }

  filePath_ = filePath;
  checkRegisteredProperties(values);
  updateLoadedValues(values);

  bool mutableConfig{false};
  auto it = values.find(std::string(kMutableConfig));
  if (it != values.end()) {
    mutableConfig = folly::to<bool>(it->second);
  }

  config_ = std::make_unique<velox::config::ConfigBase>(
      std::move(values), mutableConfig);
}

std::string ConfigBase::capacityPropertyAsBytesString(
    std::string_view propertyName) const {
  return folly::to<std::string>(velox::config::toCapacity(
      optionalProperty(propertyName).value(),
      velox::config::CapacityUnit::BYTE));
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
  auto oldValue = config_->get<std::string>(propertyName);
  config_->set(propertyName, value);
  if (oldValue.hasValue()) {
    return oldValue;
  }
  return registeredProps_[propertyName];
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
          BOOL_PROP(kMutableConfig, false),
          NONE_PROP(kPrestoVersion),
          NONE_PROP(kHttpServerHttpPort),
          BOOL_PROP(kHttpServerReusePort, false),
          BOOL_PROP(kHttpServerBindToNodeInternalAddressOnlyEnabled, false),
          NONE_PROP(kDiscoveryUri),
          NUM_PROP(kMaxDriversPerTask, 16),
          NUM_PROP(kConcurrentLifespansPerTask, 1),
          STR_PROP(kTaskMaxPartialAggregationMemory, "16MB"),
          NUM_PROP(kHttpServerNumIoThreadsHwMultiplier, 1.0),
          NUM_PROP(kHttpServerNumCpuThreadsHwMultiplier, 1.0),
          NONE_PROP(kHttpServerHttpsPort),
          BOOL_PROP(kHttpServerHttpsEnabled, false),
          STR_PROP(
              kHttpsSupportedCiphers,
              "ECDHE-ECDSA-AES256-GCM-SHA384,AES256-GCM-SHA384"),
          NONE_PROP(kHttpsCertPath),
          NONE_PROP(kHttpsKeyPath),
          NONE_PROP(kHttpsClientCertAndKeyPath),
          NUM_PROP(kExchangeHttpClientNumIoThreadsHwMultiplier, 1.0),
          NUM_PROP(kExchangeHttpClientNumCpuThreadsHwMultiplier, 1.0),
          NUM_PROP(kConnectorNumIoThreadsHwMultiplier, 1.0),
          NUM_PROP(kDriverNumCpuThreadsHwMultiplier, 4.0),
          BOOL_PROP(kDriverThreadsBatchSchedulingEnabled, false),
          NUM_PROP(kDriverStuckOperatorThresholdMs, 30 * 60 * 1000),
          NUM_PROP(
              kDriverCancelTasksWithStuckOperatorsThresholdMs, 40 * 60 * 1000),
          NUM_PROP(kSpillerNumCpuThreadsHwMultiplier, 1.0),
          STR_PROP(kSpillerFileCreateConfig, ""),
          NONE_PROP(kSpillerSpillPath),
          NUM_PROP(kShutdownOnsetSec, 10),
          NUM_PROP(kSystemMemoryGb, 40),
          BOOL_PROP(kSystemMemPushbackEnabled, false),
          NUM_PROP(kSystemMemLimitGb, 55),
          NUM_PROP(kSystemMemShrinkGb, 8),
          BOOL_PROP(kMallocMemHeapDumpEnabled, false),
          BOOL_PROP(kSystemMemPushbackAbortEnabled, false),
          NUM_PROP(kMallocHeapDumpThresholdGb, 20),
          NUM_PROP(kMallocMemMinHeapDumpInterval, 10),
          NUM_PROP(kMallocMemMaxHeapDumpFiles, 5),
          BOOL_PROP(kNativeSidecar, false),
          BOOL_PROP(kAsyncDataCacheEnabled, true),
          NUM_PROP(kAsyncCacheSsdGb, 0),
          NUM_PROP(kAsyncCacheSsdCheckpointGb, 0),
          STR_PROP(kAsyncCacheSsdPath, "/mnt/flash/async_cache."),
          NUM_PROP(kAsyncCacheMaxSsdWriteRatio, 0.7),
          NUM_PROP(kAsyncCacheSsdSavableRatio, 0.125),
          NUM_PROP(kAsyncCacheMinSsdSavableBytes, 1 << 24 /*16MB*/),
          STR_PROP(kAsyncCachePersistenceInterval, "0s"),
          BOOL_PROP(kAsyncCacheSsdDisableFileCow, false),
          BOOL_PROP(kSsdCacheChecksumEnabled, false),
          BOOL_PROP(kSsdCacheReadVerificationEnabled, false),
          BOOL_PROP(kEnableSerializedPageChecksum, true),
          BOOL_PROP(kUseMmapAllocator, true),
          STR_PROP(kMemoryArbitratorKind, ""),
          NUM_PROP(kQueryMemoryGb, 38),
          STR_PROP(kSharedArbitratorReservedCapacity, "4GB"),
          STR_PROP(kSharedArbitratorMemoryPoolInitialCapacity, "128MB"),
          STR_PROP(kSharedArbitratorMemoryPoolReservedCapacity, "64MB"),
          STR_PROP(kSharedArbitratorMemoryPoolTransferCapacity, "32MB"),
          STR_PROP(kSharedArbitratorMemoryReclaimMaxWaitTime, "5m"),
          STR_PROP(kSharedArbitratorGlobalArbitrationEnabled, "false"),
          NUM_PROP(kLargestSizeClassPages, 256),
          BOOL_PROP(kEnableVeloxTaskLogging, false),
          BOOL_PROP(kEnableVeloxExprSetLogging, false),
          NUM_PROP(kLocalShuffleMaxPartitionBytes, 268435456),
          STR_PROP(kShuffleName, ""),
          STR_PROP(kRemoteFunctionServerCatalogName, ""),
          STR_PROP(kRemoteFunctionServerSerde, "presto_page"),
          BOOL_PROP(kHttpEnableAccessLog, false),
          BOOL_PROP(kHttpEnableStatsFilter, false),
          BOOL_PROP(kHttpEnableEndpointLatencyFilter, false),
          BOOL_PROP(kRegisterTestFunctions, false),
          NUM_PROP(kHttpMaxAllocateBytes, 65536),
          STR_PROP(kQueryMaxMemoryPerNode, "4GB"),
          BOOL_PROP(kEnableMemoryLeakCheck, true),
          NONE_PROP(kRemoteFunctionServerThriftPort),
          BOOL_PROP(kSkipRuntimeStatsInRunningTaskInfo, true),
          BOOL_PROP(kLogZombieTaskInfo, false),
          NUM_PROP(kLogNumZombieTasks, 20),
          NUM_PROP(kAnnouncementMaxFrequencyMs, 30'000), // 30s
          NUM_PROP(kHeartbeatFrequencyMs, 0),
          STR_PROP(kExchangeMaxErrorDuration, "3m"),
          STR_PROP(kExchangeRequestTimeout, "20s"),
          STR_PROP(kExchangeConnectTimeout, "20s"),
          BOOL_PROP(kExchangeEnableConnectionPool, true),
          BOOL_PROP(kExchangeEnableBufferCopy, true),
          BOOL_PROP(kExchangeImmediateBufferTransfer, true),
          NUM_PROP(kTaskRunTimeSliceMicros, 50'000),
          BOOL_PROP(kIncludeNodeInSpillPath, false),
          NUM_PROP(kOldTaskCleanUpMs, 60'000),
          BOOL_PROP(kEnableOldTaskCleanUp, true),
          BOOL_PROP(kInternalCommunicationJwtEnabled, false),
          STR_PROP(kInternalCommunicationSharedSecret, ""),
          NUM_PROP(kInternalCommunicationJwtExpirationSeconds, 300),
          BOOL_PROP(kUseLegacyArrayAgg, false),
          STR_PROP(kSinkMaxBufferSize, "32MB"),
          STR_PROP(kDriverMaxPagePartitioningBufferSize, "32MB"),
          BOOL_PROP(kCacheVeloxTtlEnabled, false),
          STR_PROP(kCacheVeloxTtlThreshold, "2d"),
          STR_PROP(kCacheVeloxTtlCheckInterval, "1h"),
          BOOL_PROP(kEnableRuntimeMetricsCollection, false),
          BOOL_PROP(kPlanValidatorFailOnNestedLoopJoin, false),
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

bool SystemConfig::httpServerBindToNodeInternalAddressOnlyEnabled() const {
  return optionalProperty<bool>(kHttpServerBindToNodeInternalAddressOnlyEnabled)
      .value_or(false);
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

std::string SystemConfig::remoteFunctionServerSerde() const {
  return optionalProperty(kRemoteFunctionServerSerde).value();
}

int32_t SystemConfig::maxDriversPerTask() const {
  return optionalProperty<int32_t>(kMaxDriversPerTask).value();
}

int32_t SystemConfig::concurrentLifespansPerTask() const {
  return optionalProperty<int32_t>(kConcurrentLifespansPerTask).value();
}

double SystemConfig::httpServerNumIoThreadsHwMultiplier() const {
  return optionalProperty<double>(kHttpServerNumIoThreadsHwMultiplier).value();
}

double SystemConfig::httpServerNumCpuThreadsHwMultiplier() const {
  return optionalProperty<double>(kHttpServerNumCpuThreadsHwMultiplier).value();
}

double SystemConfig::exchangeHttpClientNumIoThreadsHwMultiplier() const {
  return optionalProperty<double>(kExchangeHttpClientNumIoThreadsHwMultiplier)
      .value();
}

double SystemConfig::exchangeHttpClientNumCpuThreadsHwMultiplier() const {
  return optionalProperty<double>(kExchangeHttpClientNumCpuThreadsHwMultiplier)
      .value();
}

double SystemConfig::connectorNumIoThreadsHwMultiplier() const {
  return optionalProperty<double>(kConnectorNumIoThreadsHwMultiplier).value();
}

double SystemConfig::driverNumCpuThreadsHwMultiplier() const {
  return optionalProperty<double>(kDriverNumCpuThreadsHwMultiplier).value();
}

bool SystemConfig::driverThreadsBatchSchedulingEnabled() const {
  return optionalProperty<bool>(kDriverThreadsBatchSchedulingEnabled).value();
}

size_t SystemConfig::driverStuckOperatorThresholdMs() const {
  return optionalProperty<size_t>(kDriverStuckOperatorThresholdMs).value();
}

size_t SystemConfig::driverCancelTasksWithStuckOperatorsThresholdMs() const {
  return optionalProperty<size_t>(
             kDriverCancelTasksWithStuckOperatorsThresholdMs)
      .value();
}

double SystemConfig::spillerNumCpuThreadsHwMultiplier() const {
  return optionalProperty<double>(kSpillerNumCpuThreadsHwMultiplier).value();
}

std::string SystemConfig::spillerFileCreateConfig() const {
  return optionalProperty<std::string>(kSpillerFileCreateConfig).value();
}

folly::Optional<std::string> SystemConfig::spillerSpillPath() const {
  return optionalProperty(kSpillerSpillPath);
}

int32_t SystemConfig::shutdownOnsetSec() const {
  return optionalProperty<int32_t>(kShutdownOnsetSec).value();
}

uint32_t SystemConfig::systemMemoryGb() const {
  return optionalProperty<uint32_t>(kSystemMemoryGb).value();
}

bool SystemConfig::prestoNativeSidecar() const {
  return optionalProperty<bool>(kNativeSidecar).value();
}

uint32_t SystemConfig::systemMemLimitGb() const {
  return optionalProperty<uint32_t>(kSystemMemLimitGb).value();
}

uint32_t SystemConfig::systemMemShrinkGb() const {
  return optionalProperty<uint32_t>(kSystemMemShrinkGb).value();
}

bool SystemConfig::systemMemPushbackEnabled() const {
  return optionalProperty<bool>(kSystemMemPushbackEnabled).value();
}

bool SystemConfig::systemMemPushBackAbortEnabled() const {
  return optionalProperty<bool>(kSystemMemPushbackAbortEnabled).value();
}

bool SystemConfig::mallocMemHeapDumpEnabled() const {
  return optionalProperty<bool>(kMallocMemHeapDumpEnabled).value();
}

uint32_t SystemConfig::mallocHeapDumpThresholdGb() const {
  return optionalProperty<uint32_t>(kMallocHeapDumpThresholdGb).value();
}

uint32_t SystemConfig::mallocMemMinHeapDumpInterval() const {
  return optionalProperty<uint32_t>(kMallocMemMinHeapDumpInterval).value();
}

uint32_t SystemConfig::mallocMemMaxHeapDumpFiles() const {
  return optionalProperty<uint32_t>(kMallocMemMaxHeapDumpFiles).value();
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

double SystemConfig::asyncCacheMaxSsdWriteRatio() const {
  return optionalProperty<double>(kAsyncCacheMaxSsdWriteRatio).value();
}

double SystemConfig::asyncCacheSsdSavableRatio() const {
  return optionalProperty<double>(kAsyncCacheSsdSavableRatio).value();
}

int32_t SystemConfig::asyncCacheMinSsdSavableBytes() const {
  return optionalProperty<int32_t>(kAsyncCacheMinSsdSavableBytes).value();
}

std::chrono::duration<double> SystemConfig::asyncCachePersistenceInterval()
    const {
  return velox::config::toDuration(
      optionalProperty(kAsyncCachePersistenceInterval).value());
}

bool SystemConfig::asyncCacheSsdDisableFileCow() const {
  return optionalProperty<bool>(kAsyncCacheSsdDisableFileCow).value();
}

bool SystemConfig::ssdCacheChecksumEnabled() const {
  return optionalProperty<bool>(kSsdCacheChecksumEnabled).value();
}

bool SystemConfig::ssdCacheReadVerificationEnabled() const {
  return optionalProperty<bool>(kSsdCacheReadVerificationEnabled).value();
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

bool SystemConfig::useMmapAllocator() const {
  return optionalProperty<bool>(kUseMmapAllocator).value();
}

std::string SystemConfig::memoryArbitratorKind() const {
  return optionalProperty<std::string>(kMemoryArbitratorKind).value_or("");
}

int32_t SystemConfig::queryMemoryGb() const {
  return optionalProperty<int32_t>(kQueryMemoryGb).value();
}

std::string SystemConfig::sharedArbitratorGlobalArbitrationEnabled() const {
  return optionalProperty<std::string>(
             kSharedArbitratorGlobalArbitrationEnabled)
      .value_or("false");
}

std::string SystemConfig::sharedArbitratorReservedCapacity() const {
  return optionalProperty<std::string>(kSharedArbitratorReservedCapacity)
      .value();
}

std::string SystemConfig::sharedArbitratorMemoryPoolInitialCapacity() const {
  static constexpr std::string_view
      kSharedArbitratorMemoryPoolInitialCapacityDefault = "128MB";
  return optionalProperty<std::string>(
             kSharedArbitratorMemoryPoolInitialCapacity)
      .value_or(std::string(kSharedArbitratorMemoryPoolInitialCapacityDefault));
}

std::string SystemConfig::sharedArbitratorMemoryPoolReservedCapacity() const {
  static constexpr std::string_view
      kSharedArbitratorMemoryPoolReservedCapacityDefault = "64MB";
  return optionalProperty<std::string>(
             kSharedArbitratorMemoryPoolReservedCapacity)
      .value_or(
          std::string(kSharedArbitratorMemoryPoolReservedCapacityDefault));
}

std::string SystemConfig::sharedArbitratorMemoryPoolTransferCapacity() const {
  static constexpr std::string_view
      kSharedArbitratorMemoryPoolTransferCapacityDefault = "32MB";
  return optionalProperty<std::string>(
             kSharedArbitratorMemoryPoolTransferCapacity)
      .value_or(
          std::string(kSharedArbitratorMemoryPoolTransferCapacityDefault));
}

std::string SystemConfig::sharedArbitratorMemoryReclaimWaitTime() const {
  static constexpr std::string_view
      kSharedArbitratorMemoryReclaimMaxWaitTimeDefault = "5m";
  return optionalProperty<std::string>(
             kSharedArbitratorMemoryReclaimMaxWaitTime)
      .value_or(std::string(kSharedArbitratorMemoryReclaimMaxWaitTimeDefault));
}

std::string SystemConfig::sharedArbitratorFastExponentialGrowthCapacityLimit()
    const {
  static constexpr std::string_view
      kSharedArbitratorFastExponentialGrowthCapacityLimitDefault = "512MB";
  return optionalProperty<std::string>(
             kSharedArbitratorFastExponentialGrowthCapacityLimit)
      .value_or(std::string(
          kSharedArbitratorFastExponentialGrowthCapacityLimitDefault));
}

std::string SystemConfig::sharedArbitratorSlowCapacityGrowPct() const {
  static constexpr std::string_view
      kSharedArbitratorSlowCapacityGrowPctDefault = "0.25";
  return optionalProperty<std::string>(kSharedArbitratorSlowCapacityGrowPct)
      .value_or(std::string(kSharedArbitratorSlowCapacityGrowPctDefault));
}

std::string SystemConfig::sharedArbitratorMemoryPoolMinFreeCapacity() const {
  static constexpr std::string_view
      kSharedArbitratorMemoryPoolMinFreeCapacityDefault = "128MB";
  return optionalProperty<std::string>(
             kSharedArbitratorMemoryPoolMinFreeCapacity)
      .value_or(std::string(kSharedArbitratorMemoryPoolMinFreeCapacityDefault));
}

std::string SystemConfig::sharedArbitratorMemoryPoolMinFreeCapacityPct() const {
  static constexpr std::string_view
      kSharedArbitratorMemoryPoolMinFreeCapacityPctDefault = "0.25";
  return optionalProperty<std::string>(
             kSharedArbitratorMemoryPoolMinFreeCapacityPct)
      .value_or(
          std::string(kSharedArbitratorMemoryPoolMinFreeCapacityPctDefault));
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
  return velox::config::toCapacity(
      optionalProperty(kQueryMaxMemoryPerNode).value(),
      velox::config::CapacityUnit::BYTE);
}

bool SystemConfig::enableMemoryLeakCheck() const {
  return optionalProperty<bool>(kEnableMemoryLeakCheck).value();
}

bool SystemConfig::coreOnAllocationFailureEnabled() const {
  return optionalProperty<bool>(kCoreOnAllocationFailureEnabled)
      .value_or(false);
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
  return velox::config::toDuration(
      optionalProperty(kExchangeMaxErrorDuration).value());
}

std::chrono::duration<double> SystemConfig::exchangeRequestTimeoutMs() const {
  return velox::config::toDuration(
      optionalProperty(kExchangeRequestTimeout).value());
}

std::chrono::duration<double> SystemConfig::exchangeConnectTimeoutMs() const {
  return velox::config::toDuration(
      optionalProperty(kExchangeConnectTimeout).value());
}

bool SystemConfig::exchangeEnableConnectionPool() const {
  return optionalProperty<bool>(kExchangeEnableConnectionPool).value();
}

bool SystemConfig::exchangeEnableBufferCopy() const {
  return optionalProperty<bool>(kExchangeEnableBufferCopy).value();
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

bool SystemConfig::enableOldTaskCleanUp() const {
  return optionalProperty<bool>(kEnableOldTaskCleanUp).value();
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

bool SystemConfig::useLegacyArrayAgg() const {
  return optionalProperty<bool>(kUseLegacyArrayAgg).value();
}

bool SystemConfig::cacheVeloxTtlEnabled() const {
  return optionalProperty<bool>(kCacheVeloxTtlEnabled).value();
}

std::chrono::duration<double> SystemConfig::cacheVeloxTtlThreshold() const {
  return velox::config::toDuration(
      optionalProperty(kCacheVeloxTtlThreshold).value());
}

std::chrono::duration<double> SystemConfig::cacheVeloxTtlCheckInterval() const {
  return velox::config::toDuration(
      optionalProperty(kCacheVeloxTtlCheckInterval).value());
}

int32_t SystemConfig::largestSizeClassPages() const {
  return optionalProperty<int32_t>(kLargestSizeClassPages).value();
}

bool SystemConfig::enableRuntimeMetricsCollection() const {
  return optionalProperty<bool>(kEnableRuntimeMetricsCollection).value();
}

NodeConfig::NodeConfig() {
  registeredProps_ =
      std::unordered_map<std::string, folly::Optional<std::string>>{
          NONE_PROP(kNodeEnvironment),
          NONE_PROP(kNodeId),
          NONE_PROP(kNodeIp),
          NONE_PROP(kNodeInternalAddress),
          NONE_PROP(kNodeLocation),
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
  auto resultOpt = optionalProperty(kNodeId);
  if (resultOpt.hasValue()) {
    return resultOpt.value();
  }
  // Generate the nodeId which must be a UUID. nodeId must be a singleton.
  static auto nodeId =
      boost::lexical_cast<std::string>(boost::uuids::random_generator()());
  return nodeId;
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

BaseVeloxQueryConfig::BaseVeloxQueryConfig() {
  // Use empty instance to get default property values.
  velox::core::QueryConfig c{{}};
  using namespace velox::core;
  registeredProps_ =
      std::unordered_map<std::string, folly::Optional<std::string>>{
          BOOL_PROP(kMutableConfig, false),
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
          BOOL_PROP(QueryConfig::kJoinSpillEnabled, c.joinSpillEnabled()),
          BOOL_PROP(QueryConfig::kOrderBySpillEnabled, c.orderBySpillEnabled()),
          NUM_PROP(QueryConfig::kMaxSpillLevel, c.maxSpillLevel()),
          NUM_PROP(QueryConfig::kMaxSpillFileSize, c.maxSpillFileSize()),
          NUM_PROP(
              QueryConfig::kSpillStartPartitionBit, c.spillStartPartitionBit()),
          NUM_PROP(
              QueryConfig::kSpillNumPartitionBits, c.spillNumPartitionBits()),
          NUM_PROP(
              QueryConfig::kSpillableReservationGrowthPct,
              c.spillableReservationGrowthPct()),
          BOOL_PROP(
              QueryConfig::kPrestoArrayAggIgnoreNulls,
              c.prestoArrayAggIgnoreNulls()),
          BOOL_PROP(
              QueryConfig::kSelectiveNimbleReaderEnabled,
              c.selectiveNimbleReaderEnabled()),
          NUM_PROP(QueryConfig::kMaxOutputBufferSize, c.maxOutputBufferSize()),
      };
}

BaseVeloxQueryConfig* BaseVeloxQueryConfig::instance() {
  static std::unique_ptr<BaseVeloxQueryConfig> instance =
      std::make_unique<BaseVeloxQueryConfig>();
  return instance.get();
}

void BaseVeloxQueryConfig::updateLoadedValues(
    std::unordered_map<std::string, std::string>& values) const {
  // Update velox config with values from presto system config.
  auto systemConfig = SystemConfig::instance();

  using namespace velox::core;
  const std::unordered_map<std::string, std::string> updatedValues{
      {QueryConfig::kPrestoArrayAggIgnoreNulls,
       bool2String(systemConfig->useLegacyArrayAgg())},
      {QueryConfig::kMaxOutputBufferSize,
       systemConfig->capacityPropertyAsBytesString(
           SystemConfig::kSinkMaxBufferSize)},
      {QueryConfig::kMaxPartitionedOutputBufferSize,
       systemConfig->capacityPropertyAsBytesString(
           SystemConfig::kDriverMaxPagePartitioningBufferSize)},
      {QueryConfig::kMaxPartialAggregationMemory,
       systemConfig->capacityPropertyAsBytesString(
           SystemConfig::kTaskMaxPartialAggregationMemory)},
  };

  std::stringstream updated;
  for (const auto& pair : updatedValues) {
    updated << "  " << pair.first << "=" << pair.second << "\n";
    values[pair.first] = pair.second;
  }
  auto str = updated.str();
  if (!str.empty()) {
    PRESTO_STARTUP_LOG(INFO)
        << "Updated in '" << filePath_ << "' from SystemProperties:\n"
        << str;
  }
}

} // namespace facebook::presto
