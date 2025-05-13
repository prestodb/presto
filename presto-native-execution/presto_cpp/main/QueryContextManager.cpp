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

#include "presto_cpp/main/QueryContextManager.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/core/QueryConfig.h"
#include "velox/type/tz/TimeZoneMap.h"

using namespace facebook::velox;

using facebook::presto::protocol::QueryId;
using facebook::presto::protocol::TaskId;

namespace facebook::presto {
namespace {

// Update passed in query session configs with system configs. For any pairing
// system/session configs if session config is present, it overrides system
// config, otherwise system config is fed in queryConfigs. E.g.
// "query.max-memory-per-node" system config and "query_max_memory_per_node"
// session config is a pairing config. If system config is 7GB but session
// config is not provided, then 7GB will be added to 'queryConfigs'. On the
// other hand if system config is 7GB but session config is 4GB then 4GB will
// be preserved in 'queryConfigs'.
void updateFromSystemConfigs(
    std::unordered_map<std::string, std::string>& queryConfigs) {
  const auto& systemConfig = SystemConfig::instance();
  static const std::unordered_map<std::string, std::string>
      sessionSystemConfigMapping{
          {core::QueryConfig::kQueryMaxMemoryPerNode,
           std::string(SystemConfig::kQueryMaxMemoryPerNode)},
          {core::QueryConfig::kSpillFileCreateConfig,
           std::string(SystemConfig::kSpillerFileCreateConfig)},
          {core::QueryConfig::kSpillEnabled,
          std::string(SystemConfig::kSpillEnabled)},
          {core::QueryConfig::kJoinSpillEnabled,
          std::string(SystemConfig::kJoinSpillEnabled)},
          {core::QueryConfig::kOrderBySpillEnabled,
          std::string(SystemConfig::kOrderBySpillEnabled)},
          {core::QueryConfig::kAggregationSpillEnabled,
          std::string(SystemConfig::kAggregationSpillEnabled)},
          {core::QueryConfig::kRequestDataSizesMaxWaitSec,
          std::string(SystemConfig::kRequestDataSizesMaxWaitSec)}};
  for (const auto& configNameEntry : sessionSystemConfigMapping) {
    const auto& sessionName = configNameEntry.first;
    const auto& systemConfigName = configNameEntry.second;
    if (queryConfigs.count(sessionName) == 0) {
      const auto propertyOpt = systemConfig->optionalProperty(systemConfigName);
      if (propertyOpt.hasValue()) {
        queryConfigs[sessionName] = propertyOpt.value();
      }
    }
  }
}

std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
toConnectorConfigs(const protocol::TaskUpdateRequest& taskUpdateRequest) {
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
      connectorConfigs;
  for (const auto& entry : taskUpdateRequest.session.catalogProperties) {
    std::unordered_map<std::string, std::string> connectorConfig;
    // remove native prefix from native connector session property names
    for (const auto& sessionProperty : entry.second) {
      auto veloxConfig = (sessionProperty.first.rfind("native_", 0) == 0)
          ? sessionProperty.first.substr(7)
          : sessionProperty.first;
      connectorConfig.emplace(veloxConfig, sessionProperty.second);
    }
    connectorConfig.insert(
        taskUpdateRequest.extraCredentials.begin(),
        taskUpdateRequest.extraCredentials.end());
    connectorConfig.insert({"user", taskUpdateRequest.session.user});
    connectorConfigs.insert(
        {entry.first, connectorConfig});
  }

  return connectorConfigs;
}

void updateVeloxConfigs(
    std::unordered_map<std::string, std::string>& configStrings) {
  // If `legacy_timestamp` is true, the coordinator expects timestamp
  // conversions without a timezone to be converted to the user's
  // session_timezone.
  auto it = configStrings.find("legacy_timestamp");
  // `legacy_timestamp` default value is true in the coordinator.
  if ((it == configStrings.end()) || (folly::to<bool>(it->second))) {
    configStrings.emplace(
        core::QueryConfig::kAdjustTimestampToTimezone, "true");
  }
  // TODO: remove this once cpu driver slicing config is turned on by default in
  // Velox.
  it = configStrings.find(core::QueryConfig::kDriverCpuTimeSliceLimitMs);
  if (it == configStrings.end()) {
    // Set it to 1 second to be aligned with Presto Java.
    configStrings.emplace(
        core::QueryConfig::kDriverCpuTimeSliceLimitMs, "1000");
  }
}

} // namespace

QueryContextManager::QueryContextManager(
    folly::Executor* driverExecutor,
    folly::Executor* spillerExecutor)
    : driverExecutor_(driverExecutor),
      spillerExecutor_(spillerExecutor),
      sessionProperties_(SessionProperties()) {}

std::shared_ptr<velox::core::QueryCtx>
QueryContextManager::findOrCreateQueryCtx(
    const protocol::TaskId& taskId,
    const protocol::TaskUpdateRequest& taskUpdateRequest) {
  return findOrCreateQueryCtx(
      taskId,
      toVeloxConfigs(taskUpdateRequest.session),
      toConnectorConfigs(taskUpdateRequest));
}

std::shared_ptr<core::QueryCtx> QueryContextManager::findOrCreateQueryCtx(
    const TaskId& taskId,
    std::unordered_map<std::string, std::string>&& configStrings,
    std::unordered_map<
        std::string,
        std::unordered_map<std::string, std::string>>&&
        connectorConfigStrings) {
  QueryId queryId = taskId.substr(0, taskId.find('.'));

  auto lockedCache = queryContextCache_.wlock();
  if (auto queryCtx = lockedCache->get(queryId)) {
    return queryCtx;
  }

  updateVeloxConfigs(configStrings);

  std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
      connectorConfigs;
  for (auto& entry : connectorConfigStrings) {
    connectorConfigs.insert(
        {entry.first,
         std::make_shared<config::ConfigBase>(std::move(entry.second))});
  }

  velox::core::QueryConfig queryConfig{std::move(configStrings)};
  // NOTE: the monotonically increasing 'poolId' is appended to 'queryId' to
  // ensure that the name of root memory pool instance is always unique. In some
  // edge case, we found some background activities such as the long-running
  // memory arbitration process will still hold the query root memory pool even
  // though the query ctx has been evicted out of the cache. The query ctx cache
  // is still indexed by the query id.
  static std::atomic_uint64_t poolId{0};
  std::optional<memory::MemoryPool::DebugOptions> poolDbgOpts;
  const auto debugMemoryPoolNameRegex = queryConfig.debugMemoryPoolNameRegex();
  if (!debugMemoryPoolNameRegex.empty()) {
    poolDbgOpts = memory::MemoryPool::DebugOptions{
        .debugPoolNameRegex = debugMemoryPoolNameRegex};
  }
  auto pool = memory::MemoryManager::getInstance()->addRootPool(
      fmt::format("{}_{}", queryId, poolId++),
      queryConfig.queryMaxMemoryPerNode(),
      nullptr,
      poolDbgOpts);

  auto queryCtx = core::QueryCtx::create(
      driverExecutor_,
      std::move(queryConfig),
      connectorConfigs,
      cache::AsyncDataCache::getInstance(),
      std::move(pool),
      spillerExecutor_,
      queryId);

  return lockedCache->insert(queryId, queryCtx);
}

void QueryContextManager::visitAllContexts(
    std::function<void(const protocol::QueryId&, const velox::core::QueryCtx*)>
        visitor) const {
  auto lockedCache = queryContextCache_.rlock();
  for (const auto& it : lockedCache->ctxs()) {
    if (const auto queryCtxSP = it.second.first.lock()) {
      visitor(it.first, queryCtxSP.get());
    }
  }
}

void QueryContextManager::testingClearCache() {
  queryContextCache_.wlock()->testingClear();
}

void QueryContextCache::testingClear() {
  queryCtxs_.clear();
  queryIds_.clear();
}

std::unordered_map<std::string, std::string>
QueryContextManager::toVeloxConfigs(
    const protocol::SessionRepresentation& session) {
  // Use base velox query config as the starting point and add Presto session
  // properties on top of it.
  auto configs = BaseVeloxQueryConfig::instance()->values();
  std::optional<std::string> traceFragmentId;
  std::optional<std::string> traceShardId;
  for (const auto& it : session.systemProperties) {
    if (it.first == SessionProperties::kQueryTraceFragmentId) {
      traceFragmentId = it.second;
    } else if (it.first == SessionProperties::kQueryTraceShardId) {
      traceShardId = it.second;
    } else if (it.first == SessionProperties::kShuffleCompressionCodec) {
      auto compression = it.second;
      std::transform(
          compression.begin(),
          compression.end(),
          compression.begin(),
          ::tolower);
      velox::common::CompressionKind compressionKind =
          common::stringToCompressionKind(compression);
      configs[core::QueryConfig::kShuffleCompressionKind] =
          velox::common::compressionKindToString(compressionKind);
    } else {
      configs[sessionProperties_.toVeloxConfig(it.first)] = it.second;
      sessionProperties_.updateVeloxConfig(it.first, it.second);
    }
  }

  // If there's a timeZoneKey, convert to timezone name and add to the
  // configs. Throws if timeZoneKey can't be resolved.
  if (session.timeZoneKey != 0) {
    configs.emplace(
        velox::core::QueryConfig::kSessionTimezone,
        velox::tz::getTimeZoneName(session.timeZoneKey));
  }

  // Construct query tracing regex and pass to Velox config.
  // It replaces the given native_query_trace_task_reg_exp if also set.
  if (traceFragmentId.has_value() || traceShardId.has_value()) {
    configs.emplace(
        velox::core::QueryConfig::kQueryTraceTaskRegExp,
        ".*\\." + traceFragmentId.value_or(".*") + "\\..*\\." +
            traceShardId.value_or(".*") + "\\..*");
  }

  updateFromSystemConfigs(configs);
  return configs;
}

} // namespace facebook::presto
