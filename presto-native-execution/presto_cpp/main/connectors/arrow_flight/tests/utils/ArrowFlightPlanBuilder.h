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
#pragma once

#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::presto::test {

class ArrowFlightPlanBuilder : public velox::exec::test::PlanBuilder {
 public:
  /// @brief Add a table scan node to the Plan, using the Flight connector
  /// @param outputType The output type of the table scan node
  /// @param assignments mapping from the column aliases to real column handles
  /// @param createDefaultColumnHandles If true, generate column handles for
  /// for the columns which don't have an entry in assignments
  velox::exec::test::PlanBuilder& flightTableScan(
      const velox::RowTypePtr& outputType,
      velox::connector::ColumnHandleMap assignments = {},
      bool createDefaultColumnHandles = true);
};

} // namespace facebook::presto::test
