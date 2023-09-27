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
#include "velox/type/tz/TimeZoneMap.h"

using namespace facebook::velox;

using facebook::presto::protocol::QueryId;
using facebook::presto::protocol::TaskId;

namespace facebook::presto {
namespace {
static std::shared_ptr<folly::CPUThreadPoolExecutor>& executor() {
  static auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      SystemConfig::instance()->numQueryThreads(),
      std::make_shared<folly::NamedThreadFactory>("Driver"));
  return executor;
}

std::shared_ptr<folly::IOThreadPoolExecutor> spillExecutor() {
  const int32_t numSpillThreads = SystemConfig::instance()->numSpillThreads();
  if (numSpillThreads <= 0) {
    return nullptr;
  }
  static auto executor = std::make_shared<folly::IOThreadPoolExecutor>(
      numSpillThreads, std::make_shared<folly::NamedThreadFactory>("Spiller"));
  return executor;
}
} // namespace

folly::CPUThreadPoolExecutor* driverCPUExecutor() {
  return executor().get();
}

folly::IOThreadPoolExecutor* spillExecutorPtr() {
  return spillExecutor().get();
}

namespace {
std::unordered_map<std::string, std::string> toConfigs(
    const protocol::SessionRepresentation& session) {
  // Use base velox query config as the starting point and add Presto session
  // properties on top of it.
  auto configs = BaseVeloxQueryConfig::instance()->values();
  for (const auto& it : session.systemProperties) {
    configs[it.first] = it.second;
  }

  // If there's a timeZoneKey, convert to timezone name and add to the
  // configs. Throws if timeZoneKey can't be resolved.
  if (session.timeZoneKey != 0) {
    configs.emplace(
        velox::core::QueryConfig::kSessionTimezone,
        velox::util::getTimeZoneName(session.timeZoneKey));
  }
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

  auto pool = memory::defaultMemoryManager().addRootPool(
      queryId,
      SystemConfig::instance()->queryMaxMemoryPerNode(),
      !SystemConfig::instance()->memoryArbitratorKind().empty()
          ? memory::MemoryReclaimer::create()
          : nullptr);

  auto queryCtx = std::make_shared<core::QueryCtx>(
      executor().get(),
      std::move(configStrings),
      connectorConfigs,
      cache::AsyncDataCache::getInstance(),
      std::move(pool),
      spillExecutor(),
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

} // namespace facebook::presto
