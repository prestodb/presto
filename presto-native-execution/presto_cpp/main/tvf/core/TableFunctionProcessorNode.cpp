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

#include "presto_cpp/main/tvf/core/TableFunctionProcessorNode.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;
using namespace facebook::velox::core;

TableFunctionProcessorNode::TableFunctionProcessorNode(
    PlanNodeId id,
    std::string name,
    TableFunctionHandlePtr handle,
    std::vector<velox::core::FieldAccessTypedExprPtr> partitionKeys,
    std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys,
    std::vector<velox::core::SortOrder> sortingOrders,
    bool pruneWhenEmpty,
    velox::RowTypePtr outputType,
    std::vector<std::vector<column_index_t>> requiredColumns,
    std::unordered_map<velox::column_index_t, velox::column_index_t>
        markerChannels,
    std::vector<PassThroughColumnSpecification> passThroughColumns,
    std::vector<PlanNodePtr> sources)
    : PlanNode(std::move(id)),
      functionName_(std::move(name)),
      handle_(std::move(handle)),
      partitionKeys_(std::move(partitionKeys)),
      sortingKeys_(std::move(sortingKeys)),
      sortingOrders_(std::move(sortingOrders)),
      pruneWhenEmpty_(pruneWhenEmpty),
      outputType_(std::move(outputType)),
      requiredColumns_(std::move(requiredColumns)),
      markerChannels_(std::move(markerChannels)),
      passThroughColumns_(std::move(passThroughColumns)),
      sources_{std::move(sources)} {
  VELOX_CHECK_EQ(
      sortingKeys_.size(),
      sortingOrders_.size(),
      "Number of sorting keys must be equal to the number of sorting orders");

  std::unordered_set<std::string> keyNames;
  for (const auto& key : partitionKeys_) {
    VELOX_USER_CHECK(
        keyNames.insert(key->name()).second,
        "Partitioning keys must be unique. Found duplicate key: {}",
        key->name());
  }

  for (const auto& key : sortingKeys_) {
    VELOX_USER_CHECK(
        keyNames.insert(key->name()).second,
        "Sorting keys must be unique and not overlap with partitioning keys. Found duplicate key: {}",
        key->name());
  }

  VELOX_CHECK_LE(
      sources_.size(), 1, "Number of sources must be equal to 0 or 1");
}

namespace {
void appendComma(int32_t i, std::stringstream& sql) {
  if (i > 0) {
    sql << ", ";
  }
}

void addFields(
    std::stringstream& stream,
    const std::vector<FieldAccessTypedExprPtr>& keys) {
  for (auto i = 0; i < keys.size(); ++i) {
    appendComma(i, stream);
    stream << keys[i]->name();
  }
}

void addKeys(std::stringstream& stream, const std::vector<TypedExprPtr>& keys) {
  for (auto i = 0; i < keys.size(); ++i) {
    const auto& expr = keys[i];
    appendComma(i, stream);
    if (auto field = TypedExprs::asFieldAccess(expr)) {
      stream << field->name();
    } else if (auto constant = TypedExprs::asConstant(expr)) {
      stream << constant->toString();
    } else {
      stream << expr->toString();
    }
  }
}

void addSortingKeys(
    const std::vector<FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<SortOrder>& sortingOrders,
    std::stringstream& stream) {
  for (auto i = 0; i < sortingKeys.size(); ++i) {
    appendComma(i, stream);
    stream << sortingKeys[i]->name() << " " << sortingOrders[i].toString();
  }
}

} // namespace

void TableFunctionProcessorNode::addDetails(std::stringstream& stream) const {
  if (!partitionKeys_.empty()) {
    stream << "partition by [";
    addFields(stream, partitionKeys_);
    stream << "] ";
  }

  if (!sortingKeys_.empty()) {
    stream << "order by [";
    addSortingKeys(sortingKeys_, sortingOrders_, stream);
    stream << "] ";
  }
}

namespace {
folly::dynamic serializeSortingOrders(
    const std::vector<SortOrder>& sortingOrders) {
  auto array = folly::dynamic::array();
  for (const auto& order : sortingOrders) {
    array.push_back(order.serialize());
  }

  return array;
}

std::vector<SortOrder> deserializeSortingOrders(const folly::dynamic& array) {
  std::vector<SortOrder> sortingOrders;
  sortingOrders.reserve(array.size());
  for (const auto& order : array) {
    sortingOrders.push_back(SortOrder::deserialize(order));
  }
  return sortingOrders;
}

folly::dynamic serializePassThroughColumns(
    const std::vector<
        TableFunctionProcessorNode::PassThroughColumnSpecification>&
        passThroughSpecs) {
  auto array = folly::dynamic::array();
  for (const auto& passThroughSpec : passThroughSpecs) {
    array.push_back(passThroughSpec.serialize());
  }

  return array;
}

} // namespace

folly::dynamic TableFunctionProcessorNode::serialize() const {
  auto obj = PlanNode::serialize();
  if (handle_) {
    obj["handle"] = handle_->serialize();
  }

  obj["partitionKeys"] = ISerializable::serialize(partitionKeys_);
  obj["sortingKeys"] = ISerializable::serialize(sortingKeys_);
  obj["sortingOrders"] = serializeSortingOrders(sortingOrders_);
  obj["pruneWhenEmpty"] = pruneWhenEmpty_;

  obj["functionName"] = functionName_.data();
  obj["outputType"] = outputType_->serialize();

  obj["requiredColumns"] = ISerializable::serialize(requiredColumns_);
  obj["markerChannels"] = ISerializable::serialize(markerChannels_);
  obj["passThroughColumns"] = serializePassThroughColumns(passThroughColumns_);

  return obj;
}

namespace {

std::vector<TableFunctionProcessorNode::PassThroughColumnSpecification>
deserializePassthroughColumnSpecification(
    const folly::dynamic& obj,
    void* context) {
  if (obj.count("passThroughColumns")) {
    VELOX_CHECK(obj["passThroughColumns"].isArray());
    folly::dynamic array = obj["passThroughColumns"];
    std::vector<TableFunctionProcessorNode::PassThroughColumnSpecification>
        passThroughColumns;

    for (const auto& item : array) {
      passThroughColumns.push_back(
          TableFunctionProcessorNode::PassThroughColumnSpecification::
              deserialize(item));
    }
    return passThroughColumns;
  }

  return {};
}

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

std::vector<FieldAccessTypedExprPtr> deserializeFields(
    const folly::dynamic& array,
    void* context) {
  return ISerializable::deserialize<std::vector<FieldAccessTypedExpr>>(
      array, context);
}

} // namespace

// static
PlanNodePtr TableFunctionProcessorNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto sources = deserializeSources(obj, context);
  auto outputType = deserializeRowType(obj["outputType"]);
  auto handle = ISerializable::deserialize<TableFunctionHandle>(obj["handle"]);
  VELOX_CHECK(handle);

  auto partitionKeys = deserializeFields(obj["partitionKeys"], context);
  auto sortingKeys = deserializeFields(obj["sortingKeys"], context);

  auto sortingOrders = deserializeSortingOrders(obj["sortingOrders"]);

  auto name = obj["functionName"].asString();

  auto requiredColumns = deserialize<std::vector<std::vector<column_index_t>>>(
      obj["requiredColumns"]);

  // There isn't an option for std::unordered_map in the deserialization so
  // transforming from an std::map to std::unordered_map.
  auto markerChannels = deserialize<std::map<column_index_t, column_index_t>>(
      obj["markerChannels"]);
  std::unordered_map<column_index_t, column_index_t> markerChannelsMap(
      markerChannels.begin(), markerChannels.end());

  return std::make_shared<TableFunctionProcessorNode>(
      deserializePlanNodeId(obj),
      name,
      handle,
      partitionKeys,
      sortingKeys,
      sortingOrders,
      obj["pruneWhenEmpty"].asBool(),
      outputType,
      requiredColumns,
      markerChannelsMap,
      deserializePassthroughColumnSpecification(obj, context),
      sources);
}

} // namespace facebook::presto::tvf
