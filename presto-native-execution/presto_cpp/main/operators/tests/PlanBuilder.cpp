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

using namespace facebook::velox;
using namespace facebook::velox::core;

namespace facebook::presto::operators {

std::function<PlanNodePtr(std::string nodeId, PlanNodePtr)>
addPartitionAndSerializeNode(
    uint32_t numPartitions,
    bool replicateNullsAndAny,
    const std::vector<std::string>& serializedColumns) {
  return [numPartitions, &serializedColumns, replicateNullsAndAny](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    std::vector<core::TypedExprPtr> keys{
        std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0")};
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
        keys,
        numPartitions,
        serializedType,
        std::move(source),
        replicateNullsAndAny,
        std::make_shared<exec::HashPartitionFunctionSpec>(
            inputType, exec::toChannels(inputType, keys)));
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
    const std::string& shuffleName,
    const std::string& serializedWriteInfo) {
  return [&shuffleName, &serializedWriteInfo](
             PlanNodeId nodeId, PlanNodePtr source) -> PlanNodePtr {
    return std::make_shared<ShuffleWriteNode>(
        nodeId, shuffleName, serializedWriteInfo, std::move(source));
  };
}

std::function<PlanNodePtr(std::string, PlanNodePtr)> addBroadcastWriteNode(
    const std::string& basePath) {
  return [&basePath](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    return std::make_shared<BroadcastWriteNode>(
        nodeId, basePath, std::move(source));
  };
}
} // namespace facebook::presto::operators
