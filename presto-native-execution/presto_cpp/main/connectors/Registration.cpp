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
#include "presto_cpp/main/connectors/Registration.h"
#include "presto_cpp/main/connectors/IcebergPrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/SystemConnector.h"

#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowPrestoToVeloxConnector.h"
#endif

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#ifdef PRESTO_ENABLE_CUDF
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#endif

namespace facebook::presto {
namespace {

constexpr char const* kHiveHadoop2ConnectorName = "hive-hadoop2";
constexpr char const* kIcebergConnectorName = "iceberg";

const std::unordered_map<
    std::string,
    const std::shared_ptr<velox::connector::ConnectorFactory>>&
connectorFactories() {
  static const std::unordered_map<
      std::string,
      const std::shared_ptr<velox::connector::ConnectorFactory>>
      factories = {
          {velox::connector::hive::HiveConnectorFactory::kHiveConnectorName,
           std::make_shared<velox::connector::hive::HiveConnectorFactory>()},
          {kHiveHadoop2ConnectorName,
           std::make_shared<velox::connector::hive::HiveConnectorFactory>(
               kHiveHadoop2ConnectorName)},
          {velox::connector::tpch::TpchConnectorFactory::kTpchConnectorName,
           std::make_shared<velox::connector::tpch::TpchConnectorFactory>()},
          {kIcebergConnectorName,
           std::make_shared<velox::connector::hive::HiveConnectorFactory>(
               kIcebergConnectorName)},
#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
          {ArrowFlightConnectorFactory::kArrowFlightConnectorName,
           std::make_shared<ArrowFlightConnectorFactory>()},
#endif
      };
  return factories;
}

const auto& getRegistry() {
  static const std::unordered_map<
      std::string,
      std::function<void(const std::string&)>>
      registeredConnectors_ = {
          {velox::connector::hive::HiveConnectorFactory::kHiveConnectorName,
           [](const auto& catalogName) {
             registerPrestoToVeloxConnector(
                 catalogName,
                 std::make_unique<HivePrestoToVeloxConnector>(
                     velox::connector::hive::HiveConnectorFactory::
                         kHiveConnectorName));
           }},
          {velox::connector::tpch::TpchConnectorFactory::kTpchConnectorName,
           [](const auto& catalogName) {
             registerPrestoToVeloxConnector(
                 catalogName,
                 std::make_unique<TpchPrestoToVeloxConnector>(
                     velox::connector::tpch::TpchConnectorFactory::
                         kTpchConnectorName));
           }},
          {kIcebergConnectorName,
           [](const auto& catalogName) {
             registerPrestoToVeloxConnector(
                 catalogName,
                 std::make_unique<IcebergPrestoToVeloxConnector>(
                     kIcebergConnectorName));
           }},
          {kHiveHadoop2ConnectorName,
           [](const auto& catalogName) {
             registerPrestoToVeloxConnector(
                 catalogName,
                 std::make_unique<HivePrestoToVeloxConnector>(
                     kHiveHadoop2ConnectorName));
           }},
#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
          {ArrowFlightConnectorFactory::kArrowFlightConnectorName,
           [](const auto& catalogName) {
             registerPrestoToVeloxConnector(
                 catalogName,
                 std::make_unique<ArrowPrestoToVeloxConnector>(
                     ArrowFlightConnectorFactory::kArrowFlightConnectorName));
           }},
#endif
      };
  return registeredConnectors_;
}

} // namespace

velox::connector::ConnectorFactory* getConnectorFactory(
    const std::string& connectorName) {
  {
#ifdef PRESTO_ENABLE_CUDF
    if (velox::cudf_velox::CudfConfig::getInstance().enabled) {
      if (connectorName ==
          velox::connector::hive::HiveConnectorFactory::kHiveConnectorName) {
        static const auto factory = std::make_shared<
            velox::cudf_velox::connector::hive::CudfHiveConnectorFactory>();
        return factory.get();
      }
      if (connectorName == kHiveHadoop2ConnectorName) {
        static const auto factory = std::make_shared<
            velox::cudf_velox::connector::hive::CudfHiveConnectorFactory>(
            kHiveHadoop2ConnectorName);
        return factory.get();
      }
    }
#endif
    auto it = connectorFactories().find(connectorName);
    if (it != connectorFactories().end()) {
      return it->second.get();
    }
  }
  if (!velox::connector::hasConnectorFactory(connectorName)) {
    VELOX_FAIL("ConnectorFactory with name '{}' not registered", connectorName);
  }
  return velox::connector::getConnectorFactory(connectorName).get();
}

void registerConnectorTest(
    std::string& catalogName,
    std::string& connectorName) {
  const auto& it = getRegistry().find(connectorName);
  if (it != getRegistry().end()) {
    it->second(catalogName);
    return;
  }
}

void registerConnectors() {
  // Presto server uses system catalog or system schema in other catalogs
  // in different places in the code. All these resolve to the SystemConnector.
  // Depending on where the operator or column is used, different prefixes can
  // be used in the naming. So the protocol class is mapped
  // to all the different prefixes for System tables/columns.
  registerPrestoToVeloxConnector(
    "$system",
      std::make_unique<SystemPrestoToVeloxConnector>("$system"));
  registerPrestoToVeloxConnector(
    "system",
      std::make_unique<SystemPrestoToVeloxConnector>("system"));
  registerPrestoToVeloxConnector(
    "$system@system",
      std::make_unique<SystemPrestoToVeloxConnector>("$system@system"));
}
} // namespace facebook::presto