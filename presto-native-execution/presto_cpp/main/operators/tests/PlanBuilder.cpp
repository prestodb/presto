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
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "velox/exec/HashPartitionFunction.h"

using namespace facebook::velox;
using namespace facebook::velox::core;

namespace facebook::presto::operators {

std::function<PlanNodePtr(std::string nodeId, PlanNodePtr)>
addPartitionAndSerializeNode(uint32_t numPartitions) {
  return [numPartitions](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    std::vector<core::TypedExprPtr> keys;
    keys.push_back(
        std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"));
    const auto inputType = source->outputType();
    return std::make_shared<PartitionAndSerializeNode>(
        nodeId,
        keys,
        numPartitions,
        ROW({"p", "d"}, {INTEGER(), VARBINARY()}),
        std::move(source),
        std::make_shared<exec::HashPartitionFunctionSpec>(
            inputType, exec::toChannels(inputType, keys)));
  };
}

std::function<PlanNodePtr(std::string nodeId, PlanNodePtr)> addShuffleReadNode(
    velox::RowTypePtr& outputType) {
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
} // namespace facebook::presto::operators
