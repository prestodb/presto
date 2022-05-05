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

bool registerConnectorFactory(std::shared_ptr<ConnectorFactory> factory) {
  bool ok =
      connectorFactories().insert({factory->connectorName(), factory}).second;
  VELOX_CHECK(
      ok,
      "ConnectorFactory with name '{}' is already registered",
      factory->connectorName());
  return true;
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

} // namespace facebook::velox::connector
