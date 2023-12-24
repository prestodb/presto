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
#include "velox/core/QueryConfig.h"
#include "velox/type/tz/TimeZoneMap.h"

using namespace facebook::velox;

using facebook::presto::protocol::QueryId;
using facebook::presto::protocol::TaskId;

namespace facebook::presto {
namespace {
// Utility function to translate a config name in Presto to its equivalent in
// Velox. Returns 'name' as is if there is no mapping.
std::string toVeloxConfig(const std::string& name) {
  using velox::core::QueryConfig;
  static const folly::F14FastMap<std::string, std::string>
      kPrestoToVeloxMapping = {
          {"native_aggregation_spill_memory_threshold",
           QueryConfig::kAggregationSpillMemoryThreshold},
          {"native_simplified_expression_evaluation_enabled",
           QueryConfig::kExprEvalSimplified},
          {"native_join_spill_memory_threshold",
           QueryConfig::kJoinSpillMemoryThreshold},
          {"native_order_by_spill_memory_threshold",
           QueryConfig::kOrderBySpillMemoryThreshold},
          {"native_max_spill_level", QueryConfig::kMaxSpillLevel},
          {"native_max_spill_file_size", QueryConfig::kMaxSpillFileSize},
          {"native_spill_compression_codec",
           QueryConfig::kSpillCompressionKind},
          {"native_spill_write_buffer_size",
           QueryConfig::kSpillWriteBufferSize},
          {"native_spill_file_create_config",
           QueryConfig::kSpillFileCreateConfig},
          {"native_join_spill_enabled", QueryConfig::kJoinSpillEnabled},
          {"native_debug_validate_output_from_operators",
           QueryConfig::kValidateOutputFromOperators}};
  auto it = kPrestoToVeloxMapping.find(name);
  return it == kPrestoToVeloxMapping.end() ? name : it->second;
}

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

std::unordered_map<std::string, std::string> toConfigs(
    const protocol::SessionRepresentation& session) {
  // Use base velox query config as the starting point and add Presto session
  // properties on top of it.
  auto configs = BaseVeloxQueryConfig::instance()->values();
  for (const auto& it : session.systemProperties) {
    configs[toVeloxConfig(it.first)] = it.second;
  }

  // If there's a timeZoneKey, convert to timezone name and add to the
  // configs. Throws if timeZoneKey can't be resolved.
  if (session.timeZoneKey != 0) {
    configs.emplace(
        velox::core::QueryConfig::kSessionTimezone,
        velox::util::getTimeZoneName(session.timeZoneKey));
  }
  updateFromSystemConfigs(configs);
  return configs;
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
} // namespace

QueryContextManager::QueryContextManager(
    folly::Executor* driverExecutor,
    folly::Executor* spillerExecutor)
    : driverExecutor_(driverExecutor), spillerExecutor_(spillerExecutor) {}

std::shared_ptr<velox::core::QueryCtx>
QueryContextManager::findOrCreateQueryCtx(
    const protocol::TaskId& taskId,
    const protocol::SessionRepresentation& session) {
  return findOrCreateQueryCtx(
      taskId, toConfigs(session), toConnectorConfigs(session));
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

  // If `legacy_timestamp` is true, the coordinator expects timestamp
  // conversions without a timezone to be converted to the user's
  // session_timezone.
  auto it = configStrings.find("legacy_timestamp");

  // `legacy_timestamp` default value is true in the coordinator.
  if ((it == configStrings.end()) || (folly::to<bool>(it->second))) {
    configStrings.emplace(
        core::QueryConfig::kAdjustTimestampToTimezone, "true");
  }

  std::unordered_map<std::string, std::shared_ptr<Config>> connectorConfigs;
  for (auto& entry : connectorConfigStrings) {
    connectorConfigs.insert(
        {entry.first, std::make_shared<core::MemConfig>(entry.second)});
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
      queryConfig.queryMaxMemoryPerNode(),
      !SystemConfig::instance()->memoryArbitratorKind().empty()
          ? memory::MemoryReclaimer::create()
          : nullptr);

  auto queryCtx = std::make_shared<core::QueryCtx>(
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

} // namespace facebook::presto
