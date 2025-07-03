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

#include "presto_cpp/main/tvf/core/TableFunctionNode.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;
using namespace facebook::velox::core;

TableFunctionNode::TableFunctionNode(
    PlanNodeId id,
    const std::string& name,
    TableFunctionHandlePtr handle,
    velox::RowTypePtr outputType,
    RequiredColumnsMap requiredColumns,
    std::vector<PlanNodePtr> sources)
    : PlanNode(std::move(id)),
      functionName_(name),
      handle_(std::move(handle)),
      outputType_(outputType),
      requiredColumns_(std::move(requiredColumns)),
      sources_{std::move(sources)} {}

void TableFunctionNode::addDetails(std::stringstream& stream) const {}

folly::dynamic TableFunctionNode::serialize() const {
  auto obj = PlanNode::serialize();
  if (handle_) {
    obj["handle"] = handle_->serialize();
  }
  obj["functionName"] = functionName_.data();
  obj["outputType"] = outputType_->serialize();

  folly::dynamic requiredColumns = folly::dynamic::array;
  for (const auto& [tableName, columns] : requiredColumns_) {
    folly::dynamic pair = folly::dynamic::object;
    pair["tableName"] = tableName;
    pair["columnNames"] = ISerializable::serialize<column_index_t>(columns);
    requiredColumns.push_back(std::move(pair));
  }
  obj["requiredColumns"] = std::move(requiredColumns);

  return obj;
}

namespace {
std::vector<PlanNodePtr> deserializeSources(
    const folly::dynamic& obj,
    void* context) {
  if (obj.count("sources")) {
    return ISerializable::deserialize<std::vector<PlanNode>>(
        obj["sources"], context);
  }

  return {};
}

PlanNodePtr deserializeSingleSource(const folly::dynamic& obj, void* context) {
  auto sources = deserializeSources(obj, context);
  VELOX_CHECK_EQ(1, sources.size());

  return sources[0];
}

PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

RowTypePtr deserializeRowType(const folly::dynamic& obj) {
  return ISerializable::deserialize<RowType>(obj);
}
} // namespace

// static
PlanNodePtr TableFunctionNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto sources = deserializeSources(obj, context);
  auto outputType = deserializeRowType(obj["outputType"]);
  auto handle = ISerializable::deserialize<TableFunctionHandle>(obj["handle"]);
  VELOX_CHECK(handle);

  auto name = obj["functionName"].asString();

  std::unordered_map<std::string, std::vector<column_index_t>> requiredColumns;
  for (const auto& pair : obj["requiredColumns"]) {
    auto tableName = pair["tableName"].asString();
    auto columnNames = ISerializable::deserialize<std::vector<column_index_t>>(
        pair["columnNames"], context);
    requiredColumns[tableName] = columnNames;
  }

  return std::make_shared<TableFunctionNode>(
      deserializePlanNodeId(obj),
      name,
      handle,
      outputType,
      requiredColumns,
      sources);
}

} // namespace facebook::presto::tvf
