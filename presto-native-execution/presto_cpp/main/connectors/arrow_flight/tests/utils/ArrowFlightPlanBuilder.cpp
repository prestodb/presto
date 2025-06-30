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
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/ArrowFlightPlanBuilder.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"

namespace facebook::presto::test {
namespace {
const std::string kFlightConnectorId = "test-flight";
}

velox::exec::test::PlanBuilder& ArrowFlightPlanBuilder::flightTableScan(
    const velox::RowTypePtr& outputType,
    velox::connector::ColumnHandleMap assignments,
    bool createDefaultColumnHandles) {
  if (createDefaultColumnHandles) {
    for (const auto& name : outputType->names()) {
      // Provide unaliased defaults for unmapped columns.
      // `emplace` won't modify the map if the key already exists,
      // so existing aliases are kept.
      assignments.emplace(
          name, std::make_shared<ArrowFlightColumnHandle>(name));
    }
  }

  return startTableScan()
      .tableHandle(std::make_shared<ArrowFlightTableHandle>(kFlightConnectorId))
      .outputType(outputType)
      .assignments(std::move(assignments))
      .endTableScan();
}

} // namespace facebook::presto::test
