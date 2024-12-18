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
#include "presto_cpp/main/connectors/ConnectorRegistration.h"

#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowPrestoToVeloxConnector.h"
#endif

namespace facebook::presto::connector {

void registerAllPrestoConnectors() {
#ifdef PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR
  registerPrestoToVeloxConnector(
      std::make_unique<
          presto::connector::arrow_flight::ArrowPrestoToVeloxConnector>(
          "arrow-flight"));

  if (!velox::connector::hasConnectorFactory(
          presto::connector::arrow_flight::ArrowFlightConnectorFactory::
              kArrowFlightConnectorName)) {
    velox::connector::registerConnectorFactory(
        std::make_shared<
            presto::connector::arrow_flight::ArrowFlightConnectorFactory>());
  }
#endif
}

} // namespace facebook::presto::connector
