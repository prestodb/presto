/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector {
namespace {
std::unordered_map<std::string, std::shared_ptr<ConnectorFactory>>&
connectorFactories() {
  static std::unordered_map<std::string, std::shared_ptr<ConnectorFactory>>
      factories;
  return factories;
}

std::unordered_map<std::string, std::shared_ptr<Connector>>& connectors() {
  static std::unordered_map<std::string, std::shared_ptr<Connector>> connectors;
  return connectors;
}
} // namespace

bool DataSink::Stats::empty() const {
  return numWrittenBytes == 0 && numWrittenFiles == 0 && spillStats.empty();
}

std::string DataSink::Stats::toString() const {
  return fmt::format(
      "numWrittenBytes {} numWrittenFiles {} {}",
      succinctBytes(numWrittenBytes),
      numWrittenFiles,
      spillStats.toString());
}

bool registerConnectorFactory(std::shared_ptr<ConnectorFactory> factory) {
  factory->initialize();
  bool ok =
      connectorFactories().insert({factory->connectorName(), factory}).second;
  VELOX_CHECK(
      ok,
      "ConnectorFactory with name '{}' is already registered",
      factory->connectorName());
  return true;
}

bool hasConnectorFactory(const std::string& connectorName) {
  return connectorFactories().count(connectorName) == 1;
}

bool unregisterConnectorFactory(const std::string& connectorName) {
  auto count = connectorFactories().erase(connectorName);
  return count == 1;
}

std::shared_ptr<ConnectorFactory> getConnectorFactory(
    const std::string& connectorName) {
  auto it = connectorFactories().find(connectorName);
  VELOX_CHECK(
      it != connectorFactories().end(),
      "ConnectorFactory with name '{}' not registered",
      connectorName);
  return it->second;
}

bool registerConnector(std::shared_ptr<Connector> connector) {
  bool ok = connectors().insert({connector->connectorId(), connector}).second;
  VELOX_CHECK(
      ok,
      "Connector with ID '{}' is already registered",
      connector->connectorId());
  return true;
}

bool unregisterConnector(const std::string& connectorId) {
  auto count = connectors().erase(connectorId);
  return count == 1;
}

std::shared_ptr<Connector> getConnector(const std::string& connectorId) {
  auto it = connectors().find(connectorId);
  VELOX_CHECK(
      it != connectors().end(),
      "Connector with ID '{}' not registered",
      connectorId);
  return it->second;
}

const std::unordered_map<std::string, std::shared_ptr<Connector>>&
getAllConnectors() {
  return connectors();
}

folly::Synchronized<
    std::unordered_map<std::string_view, std::weak_ptr<cache::ScanTracker>>>
    Connector::trackers_;

// static
void Connector::unregisterTracker(cache::ScanTracker* tracker) {
  trackers_.withWLock([&](auto& trackers) { trackers.erase(tracker->id()); });
}

std::shared_ptr<cache::ScanTracker> Connector::getTracker(
    const std::string& scanId,
    int32_t loadQuantum) {
  return trackers_.withWLock([&](auto& trackers) -> auto {
    auto it = trackers.find(scanId);
    if (it == trackers.end()) {
      auto newTracker = std::make_shared<cache::ScanTracker>(
          scanId, unregisterTracker, loadQuantum);
      trackers[newTracker->id()] = newTracker;
      return newTracker;
    }
    std::shared_ptr<cache::ScanTracker> tracker = it->second.lock();
    if (!tracker) {
      tracker = std::make_shared<cache::ScanTracker>(
          scanId, unregisterTracker, loadQuantum);
      trackers[tracker->id()] = tracker;
    }
    return tracker;
  });
}

std::string commitStrategyToString(CommitStrategy commitStrategy) {
  switch (commitStrategy) {
    case CommitStrategy::kNoCommit:
      return "NO_COMMIT";
    case CommitStrategy::kTaskCommit:
      return "TASK_COMMIT";
    default:
      VELOX_UNREACHABLE(
          "UNKOWN COMMIT STRATEGY: {}", static_cast<int>(commitStrategy));
  }
}

CommitStrategy stringToCommitStrategy(const std::string& strategy) {
  if (strategy == "NO_COMMIT") {
    return CommitStrategy::kNoCommit;
  } else if (strategy == "TASK_COMMIT") {
    return CommitStrategy::kTaskCommit;
  } else {
    VELOX_UNREACHABLE("UNKOWN COMMIT STRATEGY: {}", strategy);
  }
}

folly::dynamic ColumnHandle::serializeBase(std::string_view name) {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = name;
  return obj;
}

folly::dynamic ColumnHandle::serialize() const {
  return serializeBase("ColumnHandle");
}

folly::dynamic ConnectorTableHandle::serializeBase(
    std::string_view name) const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = name;
  obj["connectorId"] = connectorId_;
  return obj;
}

folly::dynamic ConnectorTableHandle::serialize() const {
  return serializeBase("ConnectorTableHandle");
}
} // namespace facebook::velox::connector
