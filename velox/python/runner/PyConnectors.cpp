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

#include "velox/python/runner/PyConnectors.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"

namespace facebook::velox::py {
namespace {

std::unordered_set<std::string>& connectorRegistry() {
  static std::unordered_set<std::string> registry;
  return registry;
}

template <typename TConnectorFactory>
void registerConnector(
    const std::string& connectorId,
    std::unordered_map<std::string, std::string> configs) {
  const auto configBase =
      std::make_shared<velox::config::ConfigBase>(std::move(configs));
  TConnectorFactory factory;
  auto connector = factory.newConnector(
      connectorId, configBase, folly::getGlobalCPUExecutor().get());
  connector::registerConnector(connector);
  connectorRegistry().insert(connectorId);
}

} // namespace

void registerHive(
    const std::string& connectorId,
    std::unordered_map<std::string, std::string> configs) {
  registerConnector<connector::hive::HiveConnectorFactory>(
      connectorId, std::move(configs));
}

void registerTpch(
    const std::string& connectorId,
    std::unordered_map<std::string, std::string> configs) {
  registerConnector<connector::tpch::TpchConnectorFactory>(
      connectorId, std::move(configs));
}

// Is it ok to unregister connectors that were not registered.
void unregister(const std::string& connectorId) {
  if (!facebook::velox::connector::unregisterConnector(connectorId)) {
    throw std::runtime_error(
        fmt::format("Unable to unregister connector '{}'", connectorId));
  }
  connectorRegistry().erase(connectorId);
}

void unregisterAll() {
  while (!connectorRegistry().empty()) {
    unregister(*connectorRegistry().begin());
  }
}

} // namespace facebook::velox::py
