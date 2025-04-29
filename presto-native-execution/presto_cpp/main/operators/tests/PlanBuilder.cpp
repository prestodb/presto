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
#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"

using namespace facebook::velox;
using namespace facebook::velox::core;

namespace facebook::presto::operators {

namespace {
std::pair<
    std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>,
    std::vector<core::SortOrder>>
parseSortByClause(
    const std::vector<std::string>& sortExpr,
    const RowTypePtr& inputType,
    memory::MemoryPool* pool) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  for (const auto& expr : sortExpr) {
    const auto [untypedExpr, sortOrder] = parse::parseOrderByExpr(expr);
    const auto typedExpr =
        core::Expressions::inferTypes(untypedExpr, inputType, pool);

    const auto sortingKey =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(typedExpr);
    VELOX_CHECK_NOT_NULL(
        sortingKey,
        "sort by clause must use a column name, not an expression: {}",
        expr);
    sortingKeys.emplace_back(sortingKey);
    sortingOrders.emplace_back(sortOrder);
  }

  return {sortingKeys, sortingOrders};
}
} // namespace

std::function<PlanNodePtr(std::string nodeId, PlanNodePtr)>
addPartitionAndSerializeNode(
    uint32_t numPartitions,
    bool replicateNullsAndAny,
    const std::vector<core::TypedExprPtr>& partitionKeys,
    const std::vector<std::string>& serializedColumns,
    const std::optional<std::vector<velox::core::SortOrder>>& sortOrders,
    const std::optional<std::vector<velox::core::FieldAccessTypedExprPtr>>&
        sortKeys) {
  return [numPartitions,
          replicateNullsAndAny,
          &partitionKeys,
          &serializedColumns,
          &sortOrders,
          &sortKeys](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    const auto inputType = source->outputType();

    std::vector<std::string> names = serializedColumns;
    std::vector<TypePtr> types(serializedColumns.size());
    for (auto i = 0; i < serializedColumns.size(); ++i) {
      types[i] = inputType->findChild(serializedColumns[i]);
    }

    auto serializedType = serializedColumns.empty()
        ? inputType
        : ROW(std::move(names), std::move(types));

    return std::make_shared<PartitionAndSerializeNode>(
        nodeId,
        partitionKeys,
        numPartitions,
        serializedType,
        std::move(source),
        replicateNullsAndAny,
        std::make_shared<exec::HashPartitionFunctionSpec>(
            inputType, exec::toChannels(inputType, partitionKeys)),
        sortOrders,
        sortKeys);
  };
}

std::function<PlanNodePtr(std::string nodeId, PlanNodePtr)>
addPartitionAndSerializeNode(
    uint32_t numPartitions,
    const std::vector<std::string>& partitionKeys,
    const std::optional<std::vector<std::string>>& sortExpr,
    memory::MemoryPool* pool) {
  return [numPartitions, &partitionKeys, &sortExpr, pool](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    VELOX_CHECK_NOT_NULL(
        source, "partitionAndSerialize cannot be the source node");
    const auto outputType = source->outputType();

    std::vector<core::TypedExprPtr> partitionKeyExprs;
    for (auto& expr : partitionKeys) {
      const auto typedExpression = core::Expressions::inferTypes(
          parse::parseExpr(expr, parse::ParseOptions{}), outputType, pool);

      if (dynamic_cast<const core::FieldAccessTypedExpr*>(
              typedExpression.get())) {
        partitionKeyExprs.push_back(typedExpression);
      } else if (dynamic_cast<const core::ConstantTypedExpr*>(
                     typedExpression.get())) {
        partitionKeyExprs.push_back(typedExpression);
      } else {
        VELOX_FAIL("Expected field name or constant: {}", expr);
      }
    }

    std::optional<std::vector<core::FieldAccessTypedExprPtr>> sortingKeys;
    std::optional<std::vector<core::SortOrder>> sortingOrders;
    if (sortExpr.has_value()) {
      std::tie(sortingKeys, sortingOrders) =
          parseSortByClause(sortExpr.value(), outputType, pool);
    }
    return addPartitionAndSerializeNode(
        numPartitions,
        false, // replicateNullsAndAny
        partitionKeyExprs,
        {}, // serializedColumns
        sortingOrders,
        sortingKeys)(nodeId, source);
  };
}

std::function<PlanNodePtr(std::string nodeId, PlanNodePtr)> addShuffleReadNode(
    const velox::RowTypePtr& outputType) {
  return [&outputType](
             PlanNodeId nodeId, PlanNodePtr /* source */) -> PlanNodePtr {
    return std::make_shared<ShuffleReadNode>(nodeId, outputType);
  };
}

std::function<PlanNodePtr(std::string nodeId, PlanNodePtr)> addShuffleWriteNode(
    uint32_t numPartitions,
    const std::string& shuffleName,
    const std::string& serializedWriteInfo) {
  return [=](PlanNodeId nodeId, PlanNodePtr source) -> PlanNodePtr {
    return std::make_shared<ShuffleWriteNode>(
        nodeId,
        numPartitions,
        shuffleName,
        serializedWriteInfo,
        std::move(source));
  };
}

std::function<PlanNodePtr(std::string, PlanNodePtr)> addBroadcastWriteNode(
    const std::string& basePath,
    const std::optional<std::vector<std::string>>& outputLayout) {
  return [=](core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    auto outputType = source->outputType();

    if (outputLayout.has_value()) {
      std::vector<std::string> names = outputLayout.value();
      std::vector<TypePtr> types;
      for (const auto& name : names) {
        types.push_back(source->outputType()->findChild(name));
      }
      outputType = ROW(std::move(names), std::move(types));
    }

    return std::make_shared<BroadcastWriteNode>(
        nodeId, basePath, outputType, std::move(source));
  };
}
} // namespace facebook::presto::operators
