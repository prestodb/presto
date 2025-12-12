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
#include "presto_cpp/main/connectors/HivePrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/IcebergPrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/SystemConnector.h"

#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowPrestoToVeloxConnector.h"
#endif

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpcds/TpcdsConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#ifdef PRESTO_ENABLE_CUDF
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#endif

namespace facebook::presto {
namespace {

constexpr char const* kHiveHadoop2ConnectorName = "hive-hadoop2";
constexpr char const* kIcebergConnectorName = "iceberg";

} // namespace

std::vector<std::string> listConnectorFactories() {
  std::vector<std::string> names;
  const auto& factories = detail::connectorFactories();
  names.reserve(factories.size());
  for (const auto& [name, _] : factories) {
    names.push_back(name);
  }
  return names;
}

void registerConnectors() {
  registerConnectorFactories();

  registerPrestoToVeloxConnector(
      std::make_unique<HivePrestoToVeloxConnector>(
          velox::connector::hive::HiveConnectorFactory::kHiveConnectorName));
  registerPrestoToVeloxConnector(
      std::make_unique<HivePrestoToVeloxConnector>(kHiveHadoop2ConnectorName));
  registerPrestoToVeloxConnector(
      std::make_unique<IcebergPrestoToVeloxConnector>(kIcebergConnectorName));
  registerPrestoToVeloxConnector(
      std::make_unique<TpchPrestoToVeloxConnector>(
          velox::connector::tpch::TpchConnectorFactory::kTpchConnectorName));
  registerPrestoToVeloxConnector(
      std::make_unique<TpcdsPrestoToVeloxConnector>(
          velox::connector::tpcds::TpcdsConnectorFactory::kTpcdsConnectorName));

  // Presto server uses system catalog or system schema in other catalogs
  // in different places in the code. All these resolve to the SystemConnector.
  // Depending on where the operator or column is used, different prefixes can
  // be used in the naming. So the protocol class is mapped
  // to all the different prefixes for System tables/columns.
  registerPrestoToVeloxConnector(
      std::make_unique<SystemPrestoToVeloxConnector>("$system"));
  registerPrestoToVeloxConnector(
      std::make_unique<SystemPrestoToVeloxConnector>("system"));
  registerPrestoToVeloxConnector(
      std::make_unique<SystemPrestoToVeloxConnector>("$system@system"));
  registerPrestoToVeloxConnector(
      std::make_unique<TvfNativePrestoToVeloxConnector>(
          "system:com.facebook.presto.tvf.NativeTableFunctionSplit"));

#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
  registerPrestoToVeloxConnector(
      std::make_unique<ArrowPrestoToVeloxConnector>(
          ArrowFlightConnectorFactory::kArrowFlightConnectorName));
#endif
}

void registerConnectorFactories() {
  // Register all connector factories using the facebook::presto namespace
  // factory registry

  // Register Hive connector factory
  facebook::presto::registerConnectorFactory(
      std::make_shared<
          facebook::velox::connector::hive::HiveConnectorFactory>());

  // Register Hive Hadoop2 connector factory
  facebook::presto::registerConnectorFactory(
      std::make_shared<facebook::velox::connector::hive::HiveConnectorFactory>(
          kHiveHadoop2ConnectorName));
#ifdef PRESTO_ENABLE_CUDF
  facebook::presto::unregisterConnectorFactory(
      facebook::velox::connector::hive::HiveConnectorFactory::
          kHiveConnectorName);
  facebook::presto::unregisterConnectorFactory(kHiveHadoop2ConnectorName);

  // Register cuDF Hive connector factory
  facebook::presto::registerConnectorFactory(
      std::make_shared<facebook::velox::cudf_velox::connector::hive::
                           CudfHiveConnectorFactory>());

  // Register cudf Hive connector factory
  facebook::presto::registerConnectorFactory(
      std::make_shared<facebook::velox::cudf_velox::connector::hive::
                           CudfHiveConnectorFactory>(
          kHiveHadoop2ConnectorName));
#endif

  // Register TPC-DS connector factory
  facebook::presto::registerConnectorFactory(
      std::make_shared<
          facebook::velox::connector::tpcds::TpcdsConnectorFactory>());

  // Register TPCH connector factory
  facebook::presto::registerConnectorFactory(
      std::make_shared<
          facebook::velox::connector::tpch::TpchConnectorFactory>());

  // Register Iceberg connector factory (using Hive implementation)
  facebook::presto::registerConnectorFactory(
      std::make_shared<facebook::velox::connector::hive::HiveConnectorFactory>(
          kIcebergConnectorName));

#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
  // Note: ArrowFlightConnectorFactory would need to be implemented in Presto
  // namespace For now, keep the Velox version
  facebook::presto::registerConnectorFactory(
      std::make_shared<ArrowFlightConnectorFactory>());
#endif
}

} // namespace facebook::presto
