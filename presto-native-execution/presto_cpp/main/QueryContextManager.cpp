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
           std::string(SystemConfig::kSpillerFileCreateConfig)}};

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
toConnectorConfigs(const protocol::SessionRepresentation& session) {
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
      connectorConfigs;
  for (const auto& entry : session.catalogProperties) {
    connectorConfigs.insert(
        {entry.first,
         std::unordered_map<std::string, std::string>(
             entry.second.begin(), entry.second.end())});
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

void updateVeloxConnectorConfigs(
    std::unordered_map<
        std::string,
        std::unordered_map<std::string, std::string>>& connectorConfigStrings) {
  const auto& systemConfig = SystemConfig::instance();

  for (auto& entry : connectorConfigStrings) {
    auto& connectorConfig = entry.second;

    // Do not retain cache if `node_selection_strategy` is explicitly set to
    // `NO_PREFERENCE`.
    auto it = connectorConfig.find("node_selection_strategy");
    if (it != connectorConfig.end() && it->second == "NO_PREFERENCE") {
      connectorConfig.emplace(
          connector::hive::HiveConfig::kCacheNoRetentionSession, "true");
    }
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
    const protocol::SessionRepresentation& session) {
  return findOrCreateQueryCtx(
      taskId, toVeloxConfigs(session), toConnectorConfigs(session));
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
  updateVeloxConnectorConfigs(connectorConfigStrings);

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
  auto pool = memory::MemoryManager::getInstance()->addRootPool(
      fmt::format("{}_{}", queryId, poolId++),
      queryConfig.queryMaxMemoryPerNode());

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
  for (const auto& it : session.systemProperties) {
    configs[sessionProperties_.toVeloxConfig(it.first)] = it.second;
    sessionProperties_.updateVeloxConfig(it.first, it.second);
  }

  // If there's a timeZoneKey, convert to timezone name and add to the
  // configs. Throws if timeZoneKey can't be resolved.
  if (session.timeZoneKey != 0) {
    configs.emplace(
        velox::core::QueryConfig::kSessionTimezone,
        velox::tz::getTimeZoneName(session.timeZoneKey));
  }
  updateFromSystemConfigs(configs);
  return configs;
}

} // namespace facebook::presto
