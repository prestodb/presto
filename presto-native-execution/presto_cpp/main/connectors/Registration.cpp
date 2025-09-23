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
#include "presto_cpp/main/connectors/SystemConnector.h"

#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowPrestoToVeloxConnector.h"
#endif

#ifdef PRESTO_ENABLE_CUDF
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#endif

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"

namespace facebook::presto {
namespace {

constexpr char const* kHiveHadoop2ConnectorName = "hive-hadoop2";
constexpr char const* kIcebergConnectorName = "iceberg";

void registerConnectorFactories() {
  // These checks for connector factories can be removed after we remove the
  // registrations from the Velox library.
#ifdef PRESTO_ENABLE_CUDF
  if (!velox::connector::hasConnectorFactory(
          velox::connector::hive::HiveConnectorFactory::kHiveConnectorName)) {
    velox::connector::registerConnectorFactory(
        std::make_shared<facebook::velox::cudf_velox::connector::hive::CudfHiveConnectorFactory>());
    velox::connector::registerConnectorFactory(
        std::make_shared<facebook::velox::cudf_velox::connector::hive::CudfHiveConnectorFactory>(
            kHiveHadoop2ConnectorName));
  }
#else
  if (!velox::connector::hasConnectorFactory(
          velox::connector::hive::HiveConnectorFactory::kHiveConnectorName)) {
    velox::connector::registerConnectorFactory(
        std::make_shared<velox::connector::hive::HiveConnectorFactory>());
    velox::connector::registerConnectorFactory(
        std::make_shared<velox::connector::hive::HiveConnectorFactory>(
            kHiveHadoop2ConnectorName));
  }
#endif

  if (!velox::connector::hasConnectorFactory(
          velox::connector::tpch::TpchConnectorFactory::kTpchConnectorName)) {
    velox::connector::registerConnectorFactory(
        std::make_shared<velox::connector::tpch::TpchConnectorFactory>());
  }

  // Register Velox connector factory for iceberg.
  // The iceberg catalog is handled by the hive connector factory.
  if (!velox::connector::hasConnectorFactory(kIcebergConnectorName)) {
    velox::connector::registerConnectorFactory(
        std::make_shared<velox::connector::hive::HiveConnectorFactory>(
            kIcebergConnectorName));
  }

#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
  if (!velox::connector::hasConnectorFactory(
          ArrowFlightConnectorFactory::kArrowFlightConnectorName)) {
    velox::connector::registerConnectorFactory(
        std::make_shared<ArrowFlightConnectorFactory>());
  }
#endif
}
} // namespace

void registerConnectors() {
  registerConnectorFactories();

  registerPrestoToVeloxConnector(std::make_unique<HivePrestoToVeloxConnector>(
      velox::connector::hive::HiveConnectorFactory::kHiveConnectorName));
  registerPrestoToVeloxConnector(
      std::make_unique<HivePrestoToVeloxConnector>(kHiveHadoop2ConnectorName));
  registerPrestoToVeloxConnector(
      std::make_unique<IcebergPrestoToVeloxConnector>(kIcebergConnectorName));
  registerPrestoToVeloxConnector(std::make_unique<TpchPrestoToVeloxConnector>(
      velox::connector::tpch::TpchConnectorFactory::kTpchConnectorName));

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

#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
  registerPrestoToVeloxConnector(std::make_unique<ArrowPrestoToVeloxConnector>(
      ArrowFlightConnectorFactory::kArrowFlightConnectorName));
#endif
}
} // namespace facebook::presto
