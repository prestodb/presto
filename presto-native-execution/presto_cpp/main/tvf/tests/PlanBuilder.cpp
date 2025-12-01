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
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  return [&name, &args](PlanNodeId nodeId, PlanNodePtr source) -> PlanNodePtr {
    // Validate the user has provided all required arguments.
    auto argsList = getTableFunctionArgumentSpecs(name);
    for (const auto arg : argsList) {
      if (arg->required()) {
        VELOX_CHECK_GT(args.count(arg->name()), 0);
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

    std::vector<velox::core::FieldAccessTypedExprPtr> partitionKeys = {};
    std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys = {};
    std::vector<velox::core::SortOrder> sortingOrders = {};
    // This is flattening all the requiredColumns into one list..but that is not
    // how it is done during analysis. Need to fix this when we have multiple sources.
    std::vector<velox::column_index_t> requiredColumns = {};
    for (const auto& [tableName, columns] :
         analysis->requiredColumns()) {
      for (const auto& colIndex : columns) {
        requiredColumns.push_back(colIndex);
      }
    }

    return std::make_shared<TableFunctionProcessorNode>(
        nodeId,
        name,
        analysis->tableFunctionHandle(),
        partitionKeys,
        sortingKeys,
        sortingOrders,
        outputType,
        requiredColumns,
        sources);
  };
}
} // namespace facebook::presto::tvf
