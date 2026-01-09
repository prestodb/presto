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

#include "presto_cpp/main/tvf/tests/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::core;

namespace facebook::presto::tvf {

std::function<
    velox::core::PlanNodePtr(std::string nodeId, velox::core::PlanNodePtr)>
addTvfNode(
    const std::string& name,
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args,
    const std::vector<velox::core::FieldAccessTypedExprPtr>& partitionKeys,
    const std::vector<velox::core::FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<velox::core::SortOrder>& sortingOrders) {
  return [&name, &args, &partitionKeys, &sortingKeys, &sortingOrders](
             PlanNodeId nodeId, PlanNodePtr source) -> PlanNodePtr {
    // Validate the user has provided all required arguments.
    auto argsSpecList = getTableFunctionArgumentSpecs(name);
    bool pruneWhenEmpty;
    for (const auto argSpec : argsSpecList) {
      if (argSpec->required()) {
        VELOX_CHECK_GT(args.count(argSpec->name()), 0);
      }
      if (auto tableArgSpec =
              std::dynamic_pointer_cast<TableArgumentSpecification>(argSpec)) {
        if (!pruneWhenEmpty) {
          pruneWhenEmpty = tableArgSpec->pruneWhenEmpty();
        }
      }
    }

    auto analysis = TableFunction::analyze(name, args);
    VELOX_CHECK(analysis);
    VELOX_CHECK(analysis->tableFunctionHandle());

    RowTypePtr outputType;
    auto returnTypeSpec = getTableFunctionReturnType(name);
    if (returnTypeSpec->returnType() ==
        ReturnTypeSpecification::ReturnType::kGenericTable) {
      VELOX_CHECK(analysis->returnType());

      auto names = analysis->returnType()->names();
      auto types = analysis->returnType()->types();
      outputType = velox::ROW(std::move(names), std::move(types));
    } else {
      auto describedTableSpec =
          std::dynamic_pointer_cast<DescribedTableReturnType>(returnTypeSpec);
      auto names = describedTableSpec->descriptor()->names();
      auto types = describedTableSpec->descriptor()->types();
      outputType = velox::ROW(std::move(names), std::move(types));
    }

    std::vector<PlanNodePtr> sources;
    if (source == nullptr) {
      sources.clear();
    } else {
      sources.push_back(source);
    }

    // Populate requiredColumns from analysis->requiredColumns()
    std::vector<std::vector<velox::column_index_t>> requiredColumns;
    for (const auto& [key, columnIndices] : analysis->requiredColumns()) {
      requiredColumns.push_back(columnIndices);
    }

    // Using an empty marker channels map for tests.
    std::unordered_map<velox::column_index_t, velox::column_index_t>
        markerChannels;

    std::vector<TableFunctionProcessorNode::PassThroughColumnSpecification>
        passThroughColumns;

    return std::make_shared<TableFunctionProcessorNode>(
        nodeId,
        name,
        analysis->tableFunctionHandle(),
        partitionKeys,
        sortingKeys,
        sortingOrders,
        pruneWhenEmpty,
        outputType,
        requiredColumns,
        markerChannels,
        passThroughColumns,
        sources);
  };
}
} // namespace facebook::presto::tvf
