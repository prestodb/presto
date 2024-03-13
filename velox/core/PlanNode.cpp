/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
#include <folly/container/F14Set.h>

#include "velox/common/encode/Base64.h"
#include "velox/core/PlanNode.h"
#include "velox/vector/VectorSaver.h"

namespace facebook::velox::core {

namespace {

void appendComma(int32_t i, std::stringstream& sql) {
  if (i > 0) {
    sql << ", ";
  }
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
} // namespace

const SortOrder kAscNullsFirst(true, true);
const SortOrder kAscNullsLast(true, false);
const SortOrder kDescNullsFirst(false, true);
const SortOrder kDescNullsLast(false, false);

folly::dynamic SortOrder::serialize() const {
  folly::dynamic obj = folly::dynamic::object();
  obj["ascending"] = ascending_;
  obj["nullsFirst"] = nullsFirst_;
  return obj;
}

// static
SortOrder SortOrder::deserialize(const folly::dynamic& obj) {
  return SortOrder(obj["ascending"].asBool(), obj["nullsFirst"].asBool());
}

namespace {
const std::vector<PlanNodePtr> kEmptySources;

RowTypePtr getAggregationOutputType(
    const std::vector<FieldAccessTypedExprPtr>& groupingKeys,
    const std::vector<std::string>& aggregateNames,
    const std::vector<AggregationNode::Aggregate>& aggregates) {
  VELOX_CHECK_EQ(
      aggregateNames.size(),
      aggregates.size(),
      "Number of aggregate names must be equal to number of aggregates");

  std::vector<std::string> names;
  std::vector<TypePtr> types;

  for (auto& key : groupingKeys) {
    auto field = TypedExprs::asFieldAccess(key);
    VELOX_CHECK(field, "Grouping key must be a field reference");
    names.push_back(field->name());
    types.push_back(field->type());
  }

  for (int32_t i = 0; i < aggregateNames.size(); i++) {
    names.push_back(aggregateNames[i]);
    types.push_back(aggregates[i].call->type());
  }

  return std::make_shared<RowType>(std::move(names), std::move(types));
}

} // namespace

AggregationNode::AggregationNode(
    const PlanNodeId& id,
    Step step,
    const std::vector<FieldAccessTypedExprPtr>& groupingKeys,
    const std::vector<FieldAccessTypedExprPtr>& preGroupedKeys,
    const std::vector<std::string>& aggregateNames,
    const std::vector<Aggregate>& aggregates,
    const std::vector<vector_size_t>& globalGroupingSets,
    const std::optional<FieldAccessTypedExprPtr>& groupId,
    bool ignoreNullKeys,
    PlanNodePtr source)
    : PlanNode(id),
      step_(step),
      groupingKeys_(groupingKeys),
      preGroupedKeys_(preGroupedKeys),
      aggregateNames_(aggregateNames),
      aggregates_(aggregates),
      ignoreNullKeys_(ignoreNullKeys),
      groupId_(groupId),
      globalGroupingSets_(globalGroupingSets),
      sources_{source},
      outputType_(getAggregationOutputType(
          groupingKeys_,
          aggregateNames_,
          aggregates_)) {
  // Empty grouping keys are used in global aggregation:
  //    SELECT sum(c) FROM t
  // Empty aggregates are used in distinct:
  //    SELECT distinct(b, c) FROM t GROUP BY a
  // Sometimes there are no grouping keys and no aggregations:
  //    WITH a AS (SELECT sum(x) from t)
  //    SELECT y FROM a, UNNEST(array[1, 2,3]) as u(y)

  std::unordered_set<std::string> groupingKeyNames;
  groupingKeyNames.reserve(groupingKeys.size());
  for (const auto& key : groupingKeys) {
    groupingKeyNames.insert(key->name());
  }

  for (const auto& key : preGroupedKeys) {
    VELOX_CHECK_EQ(
        1,
        groupingKeyNames.count(key->name()),
        "Pre-grouped key must be one of the grouping keys: {}.",
        key->name());
  }

  if (groupId_.has_value()) {
    VELOX_USER_CHECK_GT(
        groupingKeyNames.count(groupId_.value()->name()),
        0,
        "GroupId key {} must be one of the grouping keys",
        groupId_.value()->name());

    VELOX_USER_CHECK(
        !globalGroupingSets_.empty(),
        "GroupId key {} must have global grouping sets",
        groupId_.value()->name());
  }

  if (!globalGroupingSets_.empty()) {
    VELOX_USER_CHECK(
        groupId_.has_value(), "Global grouping sets require GroupId key");
  }
}

AggregationNode::AggregationNode(
    const PlanNodeId& id,
    Step step,
    const std::vector<FieldAccessTypedExprPtr>& groupingKeys,
    const std::vector<FieldAccessTypedExprPtr>& preGroupedKeys,
    const std::vector<std::string>& aggregateNames,
    const std::vector<Aggregate>& aggregates,
    bool ignoreNullKeys,
    PlanNodePtr source)
    : AggregationNode(
          id,
          step,
          groupingKeys,
          preGroupedKeys,
          aggregateNames,
          aggregates,
          {},
          std::nullopt,
          ignoreNullKeys,
          source) {}

namespace {
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

bool AggregationNode::canSpill(const QueryConfig& queryConfig) const {
  // TODO: Add spilling for aggregations over distinct inputs.
  // https://github.com/facebookincubator/velox/issues/7454
  for (const auto& aggregate : aggregates_) {
    if (aggregate.distinct) {
      return false;
    }
  }
  // TODO: add spilling for pre-grouped aggregation later:
  // https://github.com/facebookincubator/velox/issues/3264
  return (isFinal() || isSingle()) && preGroupedKeys().empty() &&
      queryConfig.aggregationSpillEnabled();
}

void AggregationNode::addDetails(std::stringstream& stream) const {
  stream << stepName(step_) << " ";

  if (isPreGrouped()) {
    stream << "STREAMING ";
  }

  if (!groupingKeys_.empty()) {
    stream << "[";
    addFields(stream, groupingKeys_);
    stream << "] ";
  }

  for (auto i = 0; i < aggregateNames_.size(); ++i) {
    appendComma(i, stream);
    const auto& aggregate = aggregates_[i];
    stream << aggregateNames_[i] << " := " << aggregate.call->toString();
    if (aggregate.distinct) {
      stream << " distinct";
    }

    if (aggregate.mask) {
      stream << " mask: " << aggregate.mask->name();
    }

    if (!aggregate.sortingKeys.empty()) {
      stream << " ORDER BY ";
      addSortingKeys(aggregate.sortingKeys, aggregate.sortingOrders, stream);
    }
  }

  if (!globalGroupingSets_.empty()) {
    stream << " global group IDs: [ " << folly::join(", ", globalGroupingSets_)
           << " ]";
  }

  if (groupId_.has_value()) {
    stream << " Group Id key: " << groupId_.value()->name();
  }
}

namespace {
std::unordered_map<AggregationNode::Step, std::string> stepNames() {
  return {
      {AggregationNode::Step::kPartial, "PARTIAL"},
      {AggregationNode::Step::kFinal, "FINAL"},
      {AggregationNode::Step::kIntermediate, "INTERMEDIATE"},
      {AggregationNode::Step::kSingle, "SINGLE"},
  };
}

template <typename K, typename V>
std::unordered_map<V, K> invertMap(const std::unordered_map<K, V>& mapping) {
  std::unordered_map<V, K> inverted;
  for (const auto& [key, value] : mapping) {
    inverted.emplace(value, key);
  }
  return inverted;
}
} // namespace

// static
const char* AggregationNode::stepName(AggregationNode::Step step) {
  static const auto kSteps = stepNames();
  return kSteps.at(step).c_str();
}

// static
AggregationNode::Step AggregationNode::stepFromName(const std::string& name) {
  static const auto kSteps = invertMap(stepNames());
  return kSteps.at(name);
}

folly::dynamic AggregationNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["step"] = stepName(step_);
  obj["groupingKeys"] = ISerializable::serialize(groupingKeys_);
  obj["preGroupedKeys"] = ISerializable::serialize(preGroupedKeys_);
  obj["aggregateNames"] = ISerializable::serialize(aggregateNames_);
  obj["aggregates"] = folly::dynamic::array;
  for (const auto& aggregate : aggregates_) {
    obj["aggregates"].push_back(aggregate.serialize());
  }

  obj["globalGroupingSets"] = folly::dynamic::array;
  for (const auto& globalGroup : globalGroupingSets_) {
    obj["globalGroupingSets"].push_back(globalGroup);
  }

  if (groupId_.has_value()) {
    obj["groupId"] = ISerializable::serialize(groupId_.value());
  }
  obj["ignoreNullKeys"] = ignoreNullKeys_;
  return obj;
}

namespace {
std::vector<FieldAccessTypedExprPtr> deserializeFields(
    const folly::dynamic& array,
    void* context) {
  return ISerializable::deserialize<std::vector<FieldAccessTypedExpr>>(
      array, context);
}

std::vector<std::string> deserializeStrings(const folly::dynamic& array) {
  return ISerializable::deserialize<std::vector<std::string>>(array);
}

RowTypePtr deserializeRowType(const folly::dynamic& obj) {
  return ISerializable::deserialize<RowType>(obj);
}

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
} // namespace

folly::dynamic AggregationNode::Aggregate::serialize() const {
  folly::dynamic obj = folly::dynamic::object();
  obj["call"] = call->serialize();
  obj["rawInputTypes"] = ISerializable::serialize(rawInputTypes);
  if (mask) {
    obj["mask"] = mask->serialize();
  }
  obj["sortingKeys"] = ISerializable::serialize(sortingKeys);
  obj["sortingOrders"] = serializeSortingOrders(sortingOrders);
  obj["distinct"] = distinct;
  return obj;
}

// static
AggregationNode::Aggregate AggregationNode::Aggregate::deserialize(
    const folly::dynamic& obj,
    void* context) {
  auto call = ISerializable::deserialize<CallTypedExpr>(obj["call"]);
  auto rawInputTypes =
      ISerializable::deserialize<std::vector<Type>>(obj["rawInputTypes"]);
  FieldAccessTypedExprPtr mask;
  if (obj.count("mask")) {
    mask = ISerializable::deserialize<FieldAccessTypedExpr>(obj["mask"]);
  }
  auto sortingKeys = deserializeFields(obj["sortingKeys"], context);
  auto sortingOrders = deserializeSortingOrders(obj["sortingOrders"]);
  bool distinct = obj["distinct"].asBool();
  return {
      call,
      rawInputTypes,
      mask,
      std::move(sortingKeys),
      std::move(sortingOrders),
      distinct};
}

// static
PlanNodePtr AggregationNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);

  auto groupingKeys = deserializeFields(obj["groupingKeys"], context);
  auto preGroupedKeys = deserializeFields(obj["preGroupedKeys"], context);
  auto aggregateNames = deserializeStrings(obj["aggregateNames"]);

  std::vector<Aggregate> aggregates;
  for (const auto& aggregate : obj["aggregates"]) {
    aggregates.push_back(Aggregate::deserialize(aggregate, context));
  }

  std::vector<vector_size_t> globalGroupingSets;
  for (const auto& globalSet : obj["globalGroupingSets"]) {
    globalGroupingSets.push_back(globalSet.asInt());
  }

  std::optional<FieldAccessTypedExprPtr> groupId;
  if (obj.count("groupId")) {
    groupId = ISerializable::deserialize<FieldAccessTypedExpr>(
        obj["groupId"], context);
  }

  return std::make_shared<AggregationNode>(
      deserializePlanNodeId(obj),
      stepFromName(obj["step"].asString()),
      groupingKeys,
      preGroupedKeys,
      aggregateNames,
      aggregates,
      globalGroupingSets,
      groupId,
      obj["ignoreNullKeys"].asBool(),
      deserializeSingleSource(obj, context));
}

namespace {
RowTypePtr getExpandOutputType(
    const std::vector<std::vector<TypedExprPtr>>& projections,
    std::vector<std::string> names) {
  VELOX_USER_CHECK(!names.empty());
  VELOX_USER_CHECK(!projections.empty());
  VELOX_USER_CHECK_GT(names.size(), 0);
  VELOX_USER_CHECK_GT(projections.size(), 0);

  for (int32_t i = 0; i < projections.size(); i++) {
    VELOX_USER_CHECK_EQ(names.size(), projections[i].size());
  }

  std::vector<TypePtr> types;
  types.reserve(names.size());
  for (const auto& projection : projections[0]) {
    types.push_back(projection->type());
  }

  folly::F14FastSet<std::string> uniqueNames;
  for (const auto& name : names) {
    auto result = uniqueNames.insert(name);
    VELOX_USER_CHECK(
        result.second,
        "Found duplicate column name in Expand plan node: {}.",
        name);
  }

  return ROW(std::move(names), std::move(types));
}
} // namespace

ExpandNode::ExpandNode(
    PlanNodeId id,
    std::vector<std::vector<TypedExprPtr>> projections,
    std::vector<std::string> names,
    PlanNodePtr source)
    : PlanNode(std::move(id)),
      sources_{source},
      outputType_(getExpandOutputType(projections, std::move(names))),
      projections_(std::move(projections)) {
  const auto& projectionNames = outputType_->names();
  const auto numColumns = projectionNames.size();
  const auto numRows = projections_.size();

  for (const auto& rowProjection : projections_) {
    for (const auto& columnProjection : rowProjection) {
      VELOX_USER_CHECK(
          TypedExprs::isFieldAccess(columnProjection) ||
              TypedExprs::isConstant(columnProjection),
          "Unsupported projection expression in Expand plan node. Expected field reference or constant. Got: {} ",
          columnProjection->toString());
    }
  }

  for (int i = 0; i < numColumns; ++i) {
    const auto& type = outputType_->childAt(i);
    for (int j = 1; j < numRows; ++j) {
      VELOX_USER_CHECK(
          projections_[j][i]->type()->equivalent(*type),
          "The projections type does not match across different rows in the same column. Got: {}, {}",
          projections_[j][i]->type()->toString(),
          type->toString());
    }
  }
}

void ExpandNode::addDetails(std::stringstream& stream) const {
  for (auto i = 0; i < projections_.size(); ++i) {
    appendComma(i, stream);
    stream << "[";
    addKeys(stream, projections_[i]);
    stream << "]";
  }
}

folly::dynamic ExpandNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["projections"] = ISerializable::serialize(projections_);
  obj["names"] = ISerializable::serialize(outputType_->names());

  return obj;
}

// static
PlanNodePtr ExpandNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);
  auto names =
      ISerializable::deserialize<std::vector<std::string>>(obj["names"]);
  auto projections =
      ISerializable::deserialize<std::vector<std::vector<ITypedExpr>>>(
          obj["projections"], context);
  return std::make_shared<ExpandNode>(
      deserializePlanNodeId(obj),
      std::move(projections),
      std::move(names),
      std::move(source));
}

namespace {
RowTypePtr getGroupIdOutputType(
    const std::vector<GroupIdNode::GroupingKeyInfo>& groupingKeyInfos,
    const std::vector<FieldAccessTypedExprPtr>& aggregationInputs,
    const std::string& groupIdName) {
  // Grouping keys come first, followed by aggregation inputs and groupId
  // column.

  auto numOutputs = groupingKeyInfos.size() + aggregationInputs.size() + 1;

  std::vector<std::string> names;
  std::vector<TypePtr> types;

  names.reserve(numOutputs);
  types.reserve(numOutputs);

  for (const auto& groupingKeyInfo : groupingKeyInfos) {
    names.push_back(groupingKeyInfo.output);
    types.push_back(groupingKeyInfo.input->type());
  }

  for (const auto& input : aggregationInputs) {
    names.push_back(input->name());
    types.push_back(input->type());
  }

  names.push_back(groupIdName);
  types.push_back(BIGINT());

  return ROW(std::move(names), std::move(types));
}

} // namespace

GroupIdNode::GroupIdNode(
    PlanNodeId id,
    std::vector<std::vector<std::string>> groupingSets,
    std::vector<GroupingKeyInfo> groupingKeyInfos,
    std::vector<FieldAccessTypedExprPtr> aggregationInputs,
    std::string groupIdName,
    PlanNodePtr source)
    : PlanNode(std::move(id)),
      sources_{source},
      outputType_(getGroupIdOutputType(
          groupingKeyInfos,
          aggregationInputs,
          groupIdName)),
      groupingSets_(std::move(groupingSets)),
      groupingKeyInfos_(std::move(groupingKeyInfos)),
      aggregationInputs_(std::move(aggregationInputs)),
      groupIdName_(std::move(groupIdName)) {
  VELOX_USER_CHECK_GE(
      groupingSets_.size(),
      2,
      "GroupIdNode requires two or more grouping sets.");
}

void GroupIdNode::addDetails(std::stringstream& stream) const {
  for (auto i = 0; i < groupingSets_.size(); ++i) {
    appendComma(i, stream);
    stream << "[";
    for (auto j = 0; j < groupingSets_[i].size(); j++) {
      appendComma(j, stream);
      stream << groupingSets_[i][j];
    }
    stream << "]";
  }
}

folly::dynamic GroupIdNode::GroupingKeyInfo::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["output"] = output;
  obj["input"] = input->serialize();
  return obj;
}

folly::dynamic GroupIdNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["groupingSets"] = ISerializable::serialize(groupingSets_);
  obj["aggregationInputs"] = ISerializable::serialize(aggregationInputs_);
  obj["groupIdName"] = groupIdName_;
  obj["groupingKeyInfos"] = folly::dynamic::array();
  for (const auto& info : groupingKeyInfos_) {
    obj["groupingKeyInfos"].push_back(info.serialize());
  }
  return obj;
}

// static
PlanNodePtr GroupIdNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);
  std::vector<GroupingKeyInfo> groupingKeyInfos;
  for (const auto& info : obj["groupingKeyInfos"]) {
    groupingKeyInfos.push_back(
        {info["output"].asString(),
         ISerializable::deserialize<FieldAccessTypedExpr>(info["input"])});
  }

  auto groupingSets =
      ISerializable::deserialize<std::vector<std::vector<std::string>>>(
          obj["groupingSets"]);
  return std::make_shared<GroupIdNode>(
      deserializePlanNodeId(obj),
      std::move(groupingSets),
      std::move(groupingKeyInfos),
      deserializeFields(obj["aggregationInputs"], context),
      obj["groupIdName"].asString(),
      std::move(source));
}

const std::vector<PlanNodePtr>& ValuesNode::sources() const {
  return kEmptySources;
}

void ValuesNode::addDetails(std::stringstream& stream) const {
  vector_size_t totalCount = 0;
  for (const auto& vector : values_) {
    totalCount += vector->size();
  }
  stream << totalCount << " rows in " << values_.size() << " vectors";

  // TODO Figure out how to include 'parallelizable_' flag without breaking
  // Prestissimo tests.

  if (repeatTimes_ > 1) {
    stream << ", repeat " << repeatTimes_ << " times";
  }
}

folly::dynamic ValuesNode::serialize() const {
  auto obj = PlanNode::serialize();

  // Serialize data using VectorSaver.
  std::ostringstream out;
  for (const auto& vector : values_) {
    saveVector(*vector, out);
  }

  auto serializedData = out.str();

  obj["data"] =
      encoding::Base64::encode(serializedData.data(), serializedData.size());
  obj["parallelizable"] = parallelizable_;
  obj["repeatTimes"] = repeatTimes_;
  return obj;
}

// static
PlanNodePtr ValuesNode::create(const folly::dynamic& obj, void* context) {
  auto sources = deserializeSources(obj, context);
  VELOX_CHECK_EQ(0, sources.size());

  auto encodedData = obj["data"].asString();
  auto serializedData = encoding::Base64::decode(encodedData);
  std::istringstream dataStream(serializedData);

  auto* pool = static_cast<memory::MemoryPool*>(context);

  std::vector<RowVectorPtr> values;
  while (dataStream.tellg() < serializedData.size()) {
    values.push_back(
        std::dynamic_pointer_cast<RowVector>(restoreVector(dataStream, pool)));
  }

  return std::make_shared<ValuesNode>(
      deserializePlanNodeId(obj),
      std::move(values),
      obj["parallelizable"].asBool(),
      obj["repeatTimes"].asInt());
}

void ProjectNode::addDetails(std::stringstream& stream) const {
  stream << "expressions: ";
  for (auto i = 0; i < projections_.size(); i++) {
    auto& projection = projections_[i];
    appendComma(i, stream);
    stream << "(" << names_[i] << ":" << projection->type()->toString() << ", "
           << projection->toString() << ")";
  }
}

folly::dynamic ProjectNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["names"] = ISerializable::serialize(names_);
  obj["projections"] = ISerializable::serialize(projections_);
  return obj;
}

// static
PlanNodePtr ProjectNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);

  auto names = deserializeStrings(obj["names"]);
  auto projections = ISerializable::deserialize<std::vector<ITypedExpr>>(
      obj["projections"], context);
  return std::make_shared<ProjectNode>(
      deserializePlanNodeId(obj),
      std::move(names),
      std::move(projections),
      std::move(source));
}

const std::vector<PlanNodePtr>& TableScanNode::sources() const {
  return kEmptySources;
}

void TableScanNode::addDetails(std::stringstream& stream) const {
  stream << tableHandle_->toString();
}

folly::dynamic TableScanNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["outputType"] = outputType_->serialize();
  obj["tableHandle"] = tableHandle_->serialize();
  folly::dynamic assignments = folly::dynamic::array;
  for (const auto& [assign, columnHandle] : assignments_) {
    folly::dynamic pair = folly::dynamic::object;
    pair["assign"] = assign;
    pair["columnHandle"] = columnHandle->serialize();
    assignments.push_back(std::move(pair));
  }
  obj["assignments"] = std::move(assignments);
  return obj;
}

// static
PlanNodePtr TableScanNode::create(const folly::dynamic& obj, void* context) {
  auto planNodeId = obj["id"].asString();
  auto outputType = deserializeRowType(obj["outputType"]);
  auto tableHandle = std::const_pointer_cast<connector::ConnectorTableHandle>(
      ISerializable::deserialize<connector::ConnectorTableHandle>(
          obj["tableHandle"], context));

  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (const auto& pair : obj["assignments"]) {
    auto assign = pair["assign"].asString();
    auto columnHandle = ISerializable::deserialize<connector::ColumnHandle>(
        pair["columnHandle"]);
    assignments[assign] =
        std::const_pointer_cast<connector::ColumnHandle>(columnHandle);
  }

  return std::make_shared<const TableScanNode>(
      planNodeId, outputType, tableHandle, assignments);
}

const std::vector<PlanNodePtr>& ArrowStreamNode::sources() const {
  return kEmptySources;
}

void ArrowStreamNode::addDetails(std::stringstream& stream) const {
  // Nothing to add.
}

const std::vector<PlanNodePtr>& ExchangeNode::sources() const {
  return kEmptySources;
}

void ExchangeNode::addDetails(std::stringstream& /* stream */) const {
  // Nothing to add.
}

folly::dynamic ExchangeNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["outputType"] = ExchangeNode::outputType()->serialize();
  return obj;
}

// static
PlanNodePtr ExchangeNode::create(const folly::dynamic& obj, void* context) {
  auto outputType = deserializeRowType(obj["outputType"]);
  return std::make_shared<ExchangeNode>(deserializePlanNodeId(obj), outputType);
}

UnnestNode::UnnestNode(
    const PlanNodeId& id,
    std::vector<FieldAccessTypedExprPtr> replicateVariables,
    std::vector<FieldAccessTypedExprPtr> unnestVariables,
    const std::vector<std::string>& unnestNames,
    const std::optional<std::string>& ordinalityName,
    const PlanNodePtr& source)
    : PlanNode(id),
      replicateVariables_{std::move(replicateVariables)},
      unnestVariables_{std::move(unnestVariables)},
      withOrdinality_{ordinalityName.has_value()},
      sources_{source} {
  // Calculate output type. First come "replicate" columns, followed by
  // "unnest" columns, followed by an optional ordinality column.
  std::vector<std::string> names;
  std::vector<TypePtr> types;

  for (const auto& variable : replicateVariables_) {
    names.emplace_back(variable->name());
    types.emplace_back(variable->type());
  }

  int unnestIndex = 0;
  for (const auto& variable : unnestVariables_) {
    if (variable->type()->isArray()) {
      names.emplace_back(unnestNames[unnestIndex++]);
      types.emplace_back(variable->type()->asArray().elementType());
    } else if (variable->type()->isMap()) {
      const auto& mapType = variable->type()->asMap();

      names.emplace_back(unnestNames[unnestIndex++]);
      types.emplace_back(mapType.keyType());

      names.emplace_back(unnestNames[unnestIndex++]);
      types.emplace_back(mapType.valueType());
    } else {
      VELOX_FAIL(
          "Unexpected type of unnest variable. Expected ARRAY or MAP, but got {}.",
          variable->type()->toString());
    }
  }

  if (ordinalityName.has_value()) {
    names.emplace_back(ordinalityName.value());
    types.emplace_back(BIGINT());
  }
  outputType_ = ROW(std::move(names), std::move(types));
}

void UnnestNode::addDetails(std::stringstream& stream) const {
  addFields(stream, unnestVariables_);
}

folly::dynamic UnnestNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["replicateVariables"] = ISerializable::serialize(replicateVariables_);
  obj["unnestVariables"] = ISerializable::serialize(unnestVariables_);

  obj["unnestNames"] = folly::dynamic::array();

  // Extract 'unnest' column names from the 'outputType'.
  // Output types contains 'replicated' column names, followed by 'unnest'
  // column names, followed by an optional 'ordinal' column name.
  for (auto i = replicateVariables_.size();
       i < outputType()->size() - (withOrdinality_ ? 1 : 0);
       ++i) {
    obj["unnestNames"].push_back(outputType()->names()[i]);
  }

  if (withOrdinality_) {
    obj["ordinalityName"] = outputType()->names().back();
  }
  return obj;
}

// static
PlanNodePtr UnnestNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);
  auto replicateVariables =
      deserializeFields(obj["replicateVariables"], context);
  auto unnestVariables = deserializeFields(obj["unnestVariables"], context);
  auto unnestNames = deserializeStrings(obj["unnestNames"]);
  std::optional<std::string> ordinalityName = std::nullopt;
  if (obj.count("ordinalityName")) {
    ordinalityName = obj["ordinalityName"].asString();
  }

  return std::make_shared<UnnestNode>(
      deserializePlanNodeId(obj),
      std::move(replicateVariables),
      std::move(unnestVariables),
      std::move(unnestNames),
      ordinalityName,
      std::move(source));
}

AbstractJoinNode::AbstractJoinNode(
    const PlanNodeId& id,
    JoinType joinType,
    const std::vector<FieldAccessTypedExprPtr>& leftKeys,
    const std::vector<FieldAccessTypedExprPtr>& rightKeys,
    TypedExprPtr filter,
    PlanNodePtr left,
    PlanNodePtr right,
    RowTypePtr outputType)
    : PlanNode(id),
      joinType_(joinType),
      leftKeys_(leftKeys),
      rightKeys_(rightKeys),
      filter_(std::move(filter)),
      sources_({std::move(left), std::move(right)}),
      outputType_(std::move(outputType)) {
  VELOX_CHECK(!leftKeys_.empty(), "JoinNode requires at least one join key");
  VELOX_CHECK_EQ(
      leftKeys_.size(),
      rightKeys_.size(),
      "JoinNode requires same number of join keys on left and right sides");
  auto leftType = sources_[0]->outputType();
  for (auto key : leftKeys_) {
    VELOX_CHECK(
        leftType->containsChild(key->name()),
        "Left side join key not found in left side output: {}",
        key->name());
  }
  auto rightType = sources_[1]->outputType();
  for (auto key : rightKeys_) {
    VELOX_CHECK(
        rightType->containsChild(key->name()),
        "Right side join key not found in right side output: {}",
        key->name());
  }
  for (auto i = 0; i < leftKeys_.size(); ++i) {
    VELOX_CHECK_EQ(
        leftKeys_[i]->type()->kind(),
        rightKeys_[i]->type()->kind(),
        "Join key types on the left and right sides must match");
  }

  auto numOutputColumms = outputType_->size();
  if (core::isLeftSemiProjectJoin(joinType) ||
      core::isRightSemiProjectJoin(joinType)) {
    // Last output column must be a boolean 'match'.
    --numOutputColumms;
    VELOX_CHECK_EQ(outputType_->childAt(numOutputColumms), BOOLEAN());

    // Verify that 'match' column name doesn't match any column from left or
    // right source.
    const auto& name = outputType_->nameOf(numOutputColumms);
    VELOX_CHECK(!leftType->containsChild(name));
    VELOX_CHECK(!rightType->containsChild(name));
  }

  // Output of right semi join cannot include columns from the left side.
  bool outputMayIncludeLeftColumns =
      !(core::isRightSemiFilterJoin(joinType) ||
        core::isRightSemiProjectJoin(joinType));

  // Output of left semi and anti joins cannot include columns from the right
  // side.
  bool outputMayIncludeRightColumns =
      !(core::isLeftSemiFilterJoin(joinType) ||
        core::isLeftSemiProjectJoin(joinType) || core::isAntiJoin(joinType));

  for (auto i = 0; i < numOutputColumms; ++i) {
    auto name = outputType_->nameOf(i);
    if (outputMayIncludeLeftColumns && leftType->containsChild(name)) {
      VELOX_CHECK(
          !rightType->containsChild(name),
          "Duplicate column name found on join's left and right sides: {}",
          name);
    } else if (outputMayIncludeRightColumns && rightType->containsChild(name)) {
      VELOX_CHECK(
          !leftType->containsChild(name),
          "Duplicate column name found on join's left and right sides: {}",
          name);
    } else {
      VELOX_FAIL(
          "Join's output column not found in either left or right sides: {}",
          name);
    }
  }
}

void AbstractJoinNode::addDetails(std::stringstream& stream) const {
  stream << joinTypeName(joinType_) << " ";

  for (auto i = 0; i < leftKeys_.size(); ++i) {
    if (i > 0) {
      stream << " AND ";
    }
    stream << leftKeys_[i]->name() << "=" << rightKeys_[i]->name();
  }

  if (filter_) {
    stream << ", filter: " << filter_->toString();
  }
}

folly::dynamic AbstractJoinNode::serializeBase() const {
  auto obj = PlanNode::serialize();
  obj["joinType"] = joinTypeName(joinType_);
  obj["leftKeys"] = ISerializable::serialize(leftKeys_);
  obj["rightKeys"] = ISerializable::serialize(rightKeys_);
  if (filter_) {
    obj["filter"] = filter_->serialize();
  }
  obj["outputType"] = outputType_->serialize();
  return obj;
}

namespace {
std::unordered_map<JoinType, std::string> joinTypeNames() {
  return {
      {JoinType::kInner, "INNER"},
      {JoinType::kLeft, "LEFT"},
      {JoinType::kRight, "RIGHT"},
      {JoinType::kFull, "FULL"},
      {JoinType::kLeftSemiFilter, "LEFT SEMI (FILTER)"},
      {JoinType::kRightSemiFilter, "RIGHT SEMI (FILTER)"},
      {JoinType::kLeftSemiProject, "LEFT SEMI (PROJECT)"},
      {JoinType::kRightSemiProject, "RIGHT SEMI (PROJECT)"},
      {JoinType::kAnti, "ANTI"},
  };
}
} // namespace

const char* joinTypeName(JoinType joinType) {
  static const auto kJoinTypes = joinTypeNames();
  return kJoinTypes.at(joinType).c_str();
}

JoinType joinTypeFromName(const std::string& name) {
  static const auto kJoinTypes = invertMap(joinTypeNames());
  return kJoinTypes.at(name);
}

void HashJoinNode::addDetails(std::stringstream& stream) const {
  AbstractJoinNode::addDetails(stream);
  if (nullAware_) {
    stream << ", null aware";
  }
}

folly::dynamic HashJoinNode::serialize() const {
  auto obj = serializeBase();
  obj["nullAware"] = nullAware_;
  return obj;
}

// static
PlanNodePtr HashJoinNode::create(const folly::dynamic& obj, void* context) {
  auto sources = deserializeSources(obj, context);
  VELOX_CHECK_EQ(2, sources.size());

  auto nullAware = obj["nullAware"].asBool();
  auto leftKeys = deserializeFields(obj["leftKeys"], context);
  auto rightKeys = deserializeFields(obj["rightKeys"], context);

  TypedExprPtr filter;
  if (obj.count("filter")) {
    filter = ISerializable::deserialize<ITypedExpr>(obj["filter"]);
  }

  auto outputType = deserializeRowType(obj["outputType"]);

  return std::make_shared<HashJoinNode>(
      deserializePlanNodeId(obj),
      joinTypeFromName(obj["joinType"].asString()),
      nullAware,
      std::move(leftKeys),
      std::move(rightKeys),
      filter,
      sources[0],
      sources[1],
      outputType);
}

folly::dynamic MergeJoinNode::serialize() const {
  return serializeBase();
}

// static
PlanNodePtr MergeJoinNode::create(const folly::dynamic& obj, void* context) {
  auto sources = deserializeSources(obj, context);
  VELOX_CHECK_EQ(2, sources.size());

  auto leftKeys = deserializeFields(obj["leftKeys"], context);
  auto rightKeys = deserializeFields(obj["rightKeys"], context);

  TypedExprPtr filter;
  if (obj.count("filter")) {
    filter = ISerializable::deserialize<ITypedExpr>(obj["filter"]);
  }

  auto outputType = deserializeRowType(obj["outputType"]);

  return std::make_shared<MergeJoinNode>(
      deserializePlanNodeId(obj),
      joinTypeFromName(obj["joinType"].asString()),
      std::move(leftKeys),
      std::move(rightKeys),
      filter,
      sources[0],
      sources[1],
      outputType);
}

NestedLoopJoinNode::NestedLoopJoinNode(
    const PlanNodeId& id,
    JoinType joinType,
    TypedExprPtr joinCondition,
    PlanNodePtr left,
    PlanNodePtr right,
    RowTypePtr outputType)
    : PlanNode(id),
      joinType_(joinType),
      joinCondition_(std::move(joinCondition)),
      sources_({std::move(left), std::move(right)}),
      outputType_(std::move(outputType)) {
  VELOX_USER_CHECK(
      core::isInnerJoin(joinType_) || core::isLeftJoin(joinType_) ||
          core::isRightJoin(joinType_) || core::isFullJoin(joinType_),
      "{} unsupported, NestedLoopJoin only supports inner and outer join",
      joinTypeName(joinType_));

  auto leftType = sources_[0]->outputType();
  auto rightType = sources_[1]->outputType();
  for (const auto& name : outputType_->names()) {
    const bool leftContains = leftType->containsChild(name);
    const bool rightContains = rightType->containsChild(name);
    VELOX_USER_CHECK(
        !(leftContains && rightContains),
        "Duplicate column name found on join's left and right sides: {}",
        name);
    VELOX_USER_CHECK(
        leftContains || rightContains,
        "Join's output column not found in either left or right sides: {}",
        name);
  }
}

NestedLoopJoinNode::NestedLoopJoinNode(
    const PlanNodeId& id,
    PlanNodePtr left,
    PlanNodePtr right,
    RowTypePtr outputType)
    : NestedLoopJoinNode(
          id,
          JoinType::kInner,
          nullptr,
          left,
          right,
          outputType) {}

void NestedLoopJoinNode::addDetails(std::stringstream& stream) const {
  stream << joinTypeName(joinType_);
  if (joinCondition_) {
    stream << ", joinCondition: " << joinCondition_->toString();
  }
}

folly::dynamic NestedLoopJoinNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["joinType"] = joinTypeName(joinType_);
  if (joinCondition_) {
    obj["joinCondition"] = joinCondition_->serialize();
  }
  obj["outputType"] = outputType_->serialize();
  return obj;
}

PlanNodePtr NestedLoopJoinNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto sources = deserializeSources(obj, context);
  VELOX_CHECK_EQ(2, sources.size());

  TypedExprPtr joinCondition;
  if (obj.count("joinCondition")) {
    joinCondition =
        ISerializable::deserialize<ITypedExpr>(obj["joinCondition"], context);
  }

  auto outputType = deserializeRowType(obj["outputType"]);

  return std::make_shared<NestedLoopJoinNode>(
      deserializePlanNodeId(obj),
      joinTypeFromName(obj["joinType"].asString()),
      joinCondition,
      sources[0],
      sources[1],
      outputType);
}

AssignUniqueIdNode::AssignUniqueIdNode(
    const PlanNodeId& id,
    const std::string& idName,
    const int32_t taskUniqueId,
    PlanNodePtr source)
    : PlanNode(id), taskUniqueId_(taskUniqueId), sources_{std::move(source)} {
  std::vector<std::string> names(sources_[0]->outputType()->names());
  std::vector<TypePtr> types(sources_[0]->outputType()->children());

  names.emplace_back(idName);
  types.emplace_back(BIGINT());
  outputType_ = ROW(std::move(names), std::move(types));
  uniqueIdCounter_ = std::make_shared<std::atomic_int64_t>();
}

void AssignUniqueIdNode::addDetails(std::stringstream& /* stream */) const {
  // Nothing to add.
}

folly::dynamic AssignUniqueIdNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["idName"] = outputType_->names().back();
  obj["taskUniqueId"] = taskUniqueId_;
  return obj;
}

// static
PlanNodePtr AssignUniqueIdNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto source = deserializeSingleSource(obj, context);

  return std::make_shared<AssignUniqueIdNode>(
      deserializePlanNodeId(obj),
      obj["idName"].asString(),
      obj["taskUniqueId"].asInt(),
      std::move(source));
}

namespace {
RowTypePtr getWindowOutputType(
    const RowTypePtr& inputType,
    const std::vector<std::string>& windowColumnNames,
    const std::vector<WindowNode::Function>& windowFunctions) {
  VELOX_CHECK_EQ(
      windowColumnNames.size(),
      windowFunctions.size(),
      "Number of window column names must be equal to number of window functions");

  std::vector<std::string> names = inputType->names();
  std::vector<TypePtr> types = inputType->children();

  for (int32_t i = 0; i < windowColumnNames.size(); i++) {
    names.push_back(windowColumnNames[i]);
    types.push_back(windowFunctions[i].functionCall->type());
  }
  return ROW(std::move(names), std::move(types));
}

void addWindowFunction(
    std::stringstream& stream,
    const WindowNode::Function& windowFunction) {
  stream << windowFunction.functionCall->toString() << " ";
  if (windowFunction.ignoreNulls) {
    stream << "IGNORE NULLS ";
  }
  auto frame = windowFunction.frame;
  if (frame.startType == WindowNode::BoundType::kUnboundedFollowing) {
    VELOX_USER_FAIL("Window frame start cannot be UNBOUNDED FOLLOWING");
  }
  if (frame.endType == WindowNode::BoundType::kUnboundedPreceding) {
    VELOX_USER_FAIL("Window frame end cannot be UNBOUNDED PRECEDING");
  }

  stream << WindowNode::windowTypeName(frame.type) << " between ";
  if (frame.startValue) {
    addKeys(stream, {frame.startValue});
    stream << " ";
  }
  stream << WindowNode::boundTypeName(frame.startType) << " and ";
  if (frame.endValue) {
    addKeys(stream, {frame.endValue});
    stream << " ";
  }
  stream << WindowNode::boundTypeName(frame.endType);
}

} // namespace

WindowNode::WindowNode(
    PlanNodeId id,
    std::vector<FieldAccessTypedExprPtr> partitionKeys,
    std::vector<FieldAccessTypedExprPtr> sortingKeys,
    std::vector<SortOrder> sortingOrders,
    std::vector<std::string> windowColumnNames,
    std::vector<Function> windowFunctions,
    bool inputsSorted,
    PlanNodePtr source)
    : PlanNode(std::move(id)),
      partitionKeys_(std::move(partitionKeys)),
      sortingKeys_(std::move(sortingKeys)),
      sortingOrders_(std::move(sortingOrders)),
      windowFunctions_(std::move(windowFunctions)),
      inputsSorted_(inputsSorted),
      sources_{std::move(source)},
      outputType_(getWindowOutputType(
          sources_[0]->outputType(),
          windowColumnNames,
          windowFunctions_)) {
  VELOX_CHECK_GT(
      windowFunctions_.size(),
      0,
      "Window node must have at least one window function");
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

  for (const auto& windowFunction : windowFunctions_) {
    if (windowFunction.frame.type == WindowType::kRange) {
      if (windowFunction.frame.startValue || windowFunction.frame.endValue) {
        // This is RANGE frame with a k limit bound like
        // RANGE BETWEEN 5 PRECEDING AND CURRENT ROW.
        // Such frames require that the ORDER BY have a single sorting key
        // for comparison.
        VELOX_USER_CHECK_EQ(
            sortingKeys_.size(),
            1,
            "Window frame of type RANGE PRECEDING or FOLLOWING requires single sorting key in ORDER BY.");
      }
    }
  }
}

void WindowNode::addDetails(std::stringstream& stream) const {
  if (inputsSorted_) {
    stream << "STREAMING ";
  }

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

  const auto numInputs = inputType()->size();
  for (auto i = 0; i < windowFunctions_.size(); i++) {
    appendComma(i, stream);
    stream << outputType_->names()[i + numInputs] << " := ";
    addWindowFunction(stream, windowFunctions_[i]);
  }
}

namespace {
std::unordered_map<WindowNode::BoundType, std::string> boundTypeNames() {
  return {
      {WindowNode::BoundType::kCurrentRow, "CURRENT ROW"},
      {WindowNode::BoundType::kPreceding, "PRECEDING"},
      {WindowNode::BoundType::kFollowing, "FOLLOWING"},
      {WindowNode::BoundType::kUnboundedPreceding, "UNBOUNDED PRECEDING"},
      {WindowNode::BoundType::kUnboundedFollowing, "UNBOUNDED FOLLOWING"},
  };
}
} // namespace

// static
const char* WindowNode::boundTypeName(WindowNode::BoundType type) {
  static const auto kTypes = boundTypeNames();
  return kTypes.at(type).c_str();
}

// static
WindowNode::BoundType WindowNode::boundTypeFromName(const std::string& name) {
  static const auto kTypes = invertMap(boundTypeNames());
  return kTypes.at(name);
}

namespace {
std::unordered_map<WindowNode::WindowType, std::string> windowTypeNames() {
  return {
      {WindowNode::WindowType::kRows, "ROWS"},
      {WindowNode::WindowType::kRange, "RANGE"},
  };
}
} // namespace

// static
const char* WindowNode::windowTypeName(WindowNode::WindowType type) {
  static const auto kTypes = windowTypeNames();
  return kTypes.at(type).c_str();
}

// static
WindowNode::WindowType WindowNode::windowTypeFromName(const std::string& name) {
  static const auto kTypes = invertMap(windowTypeNames());
  return kTypes.at(name);
}

folly::dynamic WindowNode::Frame::serialize() const {
  folly::dynamic obj = folly::dynamic::object();
  obj["type"] = windowTypeName(type);
  obj["startType"] = boundTypeName(startType);
  if (startValue) {
    obj["startValue"] = startValue->serialize();
  }
  obj["endType"] = boundTypeName(endType);
  if (endValue) {
    obj["endValue"] = endValue->serialize();
  }
  return obj;
}

// static
WindowNode::Frame WindowNode::Frame::deserialize(const folly::dynamic& obj) {
  TypedExprPtr startValue;
  if (obj.count("startValue")) {
    startValue = ISerializable::deserialize<ITypedExpr>(obj["startValue"]);
  }

  TypedExprPtr endValue;
  if (obj.count("endValue")) {
    endValue = ISerializable::deserialize<ITypedExpr>(obj["endValue"]);
  }

  return {
      windowTypeFromName(obj["type"].asString()),
      boundTypeFromName(obj["startType"].asString()),
      startValue,
      boundTypeFromName(obj["endType"].asString()),
      endValue};
}

folly::dynamic WindowNode::Function::serialize() const {
  folly::dynamic obj = folly::dynamic::object();
  obj["functionCall"] = functionCall->serialize();
  obj["frame"] = frame.serialize();
  obj["ignoreNulls"] = ignoreNulls;
  return obj;
}

// static
WindowNode::Function WindowNode::Function::deserialize(
    const folly::dynamic& obj) {
  return {
      ISerializable::deserialize<CallTypedExpr>(obj["functionCall"]),
      WindowNode::Frame::deserialize(obj["frame"]),
      obj["ignoreNulls"].asBool()};
}

folly::dynamic WindowNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["partitionKeys"] = ISerializable::serialize(partitionKeys_);
  obj["sortingKeys"] = ISerializable::serialize(sortingKeys_);
  obj["sortingOrders"] = serializeSortingOrders(sortingOrders_);

  obj["functions"] = folly::dynamic::array();
  for (const auto& function : windowFunctions_) {
    obj["functions"].push_back(function.serialize());
  }

  auto numInputs = sources()[0]->outputType()->size();
  auto numOutputs = outputType()->size();
  std::vector<std::string> windowNames;
  for (auto i = numInputs; i < numOutputs; ++i) {
    windowNames.push_back(outputType_->nameOf(i));
  }
  obj["names"] = ISerializable::serialize(windowNames);
  obj["inputsSorted"] = inputsSorted_;

  return obj;
}

// static
PlanNodePtr WindowNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);
  auto partitionKeys = deserializeFields(obj["partitionKeys"], context);
  auto sortingKeys = deserializeFields(obj["sortingKeys"], context);

  auto sortingOrders = deserializeSortingOrders(obj["sortingOrders"]);

  std::vector<Function> functions;
  for (const auto& function : obj["functions"]) {
    functions.push_back(Function::deserialize(function));
  }

  auto windowNames = deserializeStrings(obj["names"]);

  auto inputsSorted = obj["inputsSorted"].asBool();

  return std::make_shared<WindowNode>(
      deserializePlanNodeId(obj),
      partitionKeys,
      sortingKeys,
      sortingOrders,
      windowNames,
      functions,
      inputsSorted,
      source);
}

RowTypePtr getMarkDistinctOutputType(
    const RowTypePtr& inputType,
    const std::string& markerName) {
  std::vector<std::string> names = inputType->names();
  std::vector<TypePtr> types = inputType->children();

  names.emplace_back(markerName);
  types.emplace_back(BOOLEAN());
  return ROW(std::move(names), std::move(types));
}

MarkDistinctNode::MarkDistinctNode(
    PlanNodeId id,
    std::string markerName,
    std::vector<FieldAccessTypedExprPtr> distinctKeys,
    PlanNodePtr source)
    : PlanNode(std::move(id)),
      markerName_(std::move(markerName)),
      distinctKeys_(std::move(distinctKeys)),
      sources_{std::move(source)},
      outputType_(
          getMarkDistinctOutputType(sources_[0]->outputType(), markerName_)) {
  VELOX_USER_CHECK_GT(markerName_.size(), 0)
  VELOX_USER_CHECK_GT(distinctKeys_.size(), 0);
}

folly::dynamic MarkDistinctNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["distinctKeys"] = ISerializable::serialize(this->distinctKeys_);
  obj["markerName"] = this->markerName_;
  return obj;
}

// static
PlanNodePtr MarkDistinctNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);
  auto distinctKeys = deserializeFields(obj["distinctKeys"], context);
  auto markerName = obj["markerName"].asString();

  return std::make_shared<MarkDistinctNode>(
      deserializePlanNodeId(obj), markerName, distinctKeys, source);
}

namespace {
RowTypePtr getRowNumberOutputType(
    const RowTypePtr& inputType,
    const std::string& rowNumberColumnName) {
  std::vector<std::string> names = inputType->names();
  std::vector<TypePtr> types = inputType->children();

  names.push_back(rowNumberColumnName);
  types.push_back(BIGINT());

  return ROW(std::move(names), std::move(types));
}

RowTypePtr getOptionalRowNumberOutputType(
    const RowTypePtr& inputType,
    const std::optional<std::string>& rowNumberColumnName) {
  if (rowNumberColumnName) {
    return getRowNumberOutputType(inputType, rowNumberColumnName.value());
  }

  return inputType;
}
} // namespace

RowNumberNode::RowNumberNode(
    PlanNodeId id,
    std::vector<FieldAccessTypedExprPtr> partitionKeys,
    const std::optional<std::string>& rowNumberColumnName,
    std::optional<int32_t> limit,
    PlanNodePtr source)
    : PlanNode(std::move(id)),
      partitionKeys_{std::move(partitionKeys)},
      limit_{limit},
      sources_{std::move(source)},
      outputType_(getOptionalRowNumberOutputType(
          sources_[0]->outputType(),
          rowNumberColumnName)) {}

void RowNumberNode::addDetails(std::stringstream& stream) const {
  if (!partitionKeys_.empty()) {
    stream << "partition by (";
    addFields(stream, partitionKeys_);
    stream << ")";
  }

  if (limit_) {
    if (!partitionKeys_.empty()) {
      stream << " ";
    }
    stream << "limit " << limit_.value();
  }
}

folly::dynamic RowNumberNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["partitionKeys"] = ISerializable::serialize(partitionKeys_);
  if (generateRowNumber()) {
    obj["rowNumberColumnName"] = outputType_->names().back();
  }
  if (limit_) {
    obj["limit"] = limit_.value();
  }

  return obj;
}

// static
PlanNodePtr RowNumberNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);
  auto partitionKeys = deserializeFields(obj["partitionKeys"], context);

  std::optional<int32_t> limit;
  if (obj.count("limit")) {
    limit = obj["limit"].asInt();
  }

  std::optional<std::string> rowNumberColumnName;
  if (obj.count("rowNumberColumnName")) {
    rowNumberColumnName = obj["rowNumberColumnName"].asString();
  }

  return std::make_shared<RowNumberNode>(
      deserializePlanNodeId(obj),
      partitionKeys,
      rowNumberColumnName,
      limit,
      source);
}

TopNRowNumberNode::TopNRowNumberNode(
    PlanNodeId id,
    std::vector<FieldAccessTypedExprPtr> partitionKeys,
    std::vector<FieldAccessTypedExprPtr> sortingKeys,
    std::vector<SortOrder> sortingOrders,
    const std::optional<std::string>& rowNumberColumnName,
    int32_t limit,
    PlanNodePtr source)
    : PlanNode(std::move(id)),
      partitionKeys_{std::move(partitionKeys)},
      sortingKeys_{std::move(sortingKeys)},
      sortingOrders_{std::move(sortingOrders)},
      limit_{limit},
      sources_{std::move(source)},
      outputType_{getOptionalRowNumberOutputType(
          sources_[0]->outputType(),
          rowNumberColumnName)} {
  VELOX_USER_CHECK_EQ(
      sortingKeys_.size(),
      sortingOrders_.size(),
      "Number of sorting keys must be equal to the number of sorting orders");

  VELOX_USER_CHECK_GT(
      sortingKeys_.size(),
      0,
      "Number of sorting keys must be greater than zero");

  VELOX_USER_CHECK_GT(limit, 0, "Limit must be greater than zero");

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
}

void TopNRowNumberNode::addDetails(std::stringstream& stream) const {
  if (!partitionKeys_.empty()) {
    stream << "partition by (";
    addFields(stream, partitionKeys_);
    stream << ") ";
  }

  stream << "order by (";
  addSortingKeys(sortingKeys_, sortingOrders_, stream);
  stream << ") ";

  stream << "limit " << limit_;
}

folly::dynamic TopNRowNumberNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["partitionKeys"] = ISerializable::serialize(partitionKeys_);
  obj["sortingKeys"] = ISerializable::serialize(sortingKeys_);
  obj["sortingOrders"] = serializeSortingOrders(sortingOrders_);
  if (generateRowNumber()) {
    obj["rowNumberColumnName"] = outputType_->names().back();
  }
  obj["limit"] = limit_;
  return obj;
}

// static
PlanNodePtr TopNRowNumberNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto source = deserializeSingleSource(obj, context);
  auto partitionKeys = deserializeFields(obj["partitionKeys"], context);
  auto sortingKeys = deserializeFields(obj["sortingKeys"], context);

  auto sortingOrders = deserializeSortingOrders(obj["sortingOrders"]);

  std::optional<std::string> rowNumberColumnName;
  if (obj.count("rowNumberColumnName")) {
    rowNumberColumnName = obj["rowNumberColumnName"].asString();
  }

  return std::make_shared<TopNRowNumberNode>(
      deserializePlanNodeId(obj),
      partitionKeys,
      sortingKeys,
      sortingOrders,
      rowNumberColumnName,
      obj["limit"].asInt(),
      source);
}

void LocalMergeNode::addDetails(std::stringstream& stream) const {
  addSortingKeys(sortingKeys_, sortingOrders_, stream);
}

folly::dynamic LocalMergeNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["sortingKeys"] = ISerializable::serialize(sortingKeys_);
  obj["sortingOrders"] = serializeSortingOrders(sortingOrders_);
  return obj;
}

// static
PlanNodePtr LocalMergeNode::create(const folly::dynamic& obj, void* context) {
  auto sources = deserializeSources(obj, context);
  auto sortingKeys = deserializeFields(obj["sortingKeys"], context);
  auto sortingOrders = deserializeSortingOrders(obj["sortingOrders"]);

  return std::make_shared<LocalMergeNode>(
      deserializePlanNodeId(obj),
      std::move(sortingKeys),
      std::move(sortingOrders),
      std::move(sources));
}

void TableWriteNode::addDetails(std::stringstream& /*unused*/) const {}

folly::dynamic TableWriteNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["sources"] = sources_.front()->serialize();
  obj["columns"] = columns_->serialize();
  obj["columnNames"] = ISerializable::serialize(columnNames_);
  if (aggregationNode_ != nullptr) {
    obj["aggregationNode"] = aggregationNode_->serialize();
  }
  obj["connectorId"] = insertTableHandle_->connectorId();
  obj["connectorInsertTableHandle"] =
      insertTableHandle_->connectorInsertTableHandle()->serialize();
  obj["hasPartitioningScheme"] = hasPartitioningScheme_;
  obj["outputType"] = outputType_->serialize();
  obj["commitStrategy"] = connector::commitStrategyToString(commitStrategy_);
  return obj;
}

// static
PlanNodePtr TableWriteNode::create(const folly::dynamic& obj, void* context) {
  auto id = obj["id"].asString();
  auto columns = deserializeRowType(obj["columns"]);
  auto columnNames =
      ISerializable::deserialize<std::vector<std::string>>(obj["columnNames"]);
  std::shared_ptr<AggregationNode> aggregationNode;
  if (obj.count("aggregationNode") != 0) {
    aggregationNode = std::const_pointer_cast<AggregationNode>(
        ISerializable::deserialize<AggregationNode>(obj["aggregationNode"]));
  }
  auto connectorId = obj["connectorId"].asString();
  auto connectorInsertTableHandle =
      std::const_pointer_cast<connector::ConnectorInsertTableHandle>(
          ISerializable::deserialize<connector::ConnectorInsertTableHandle>(
              obj["connectorInsertTableHandle"]));
  const bool hasPartitioningScheme = obj["hasPartitioningScheme"].asBool();
  auto outputType = deserializeRowType(obj["outputType"]);
  auto commitStrategy =
      connector::stringToCommitStrategy(obj["commitStrategy"].asString());
  auto source = ISerializable::deserialize<PlanNode>(obj["sources"]);
  return std::make_shared<TableWriteNode>(
      id,
      columns,
      columnNames,
      std::move(aggregationNode),
      std::make_shared<InsertTableHandle>(
          connectorId, connectorInsertTableHandle),
      hasPartitioningScheme,
      outputType,
      commitStrategy,
      source);
}

void TableWriteMergeNode::addDetails(std::stringstream& /* stream */) const {}

folly::dynamic TableWriteMergeNode::serialize() const {
  auto obj = PlanNode::serialize();
  VELOX_CHECK_EQ(
      sources_.size(), 1, "TableWriteMergeNode can only have one source");
  obj["sources"] = sources_.front()->serialize();
  if (aggregationNode_ != nullptr) {
    obj["aggregationNode"] = aggregationNode_->serialize();
  }
  obj["outputType"] = outputType_->serialize();
  return obj;
}

// static
PlanNodePtr TableWriteMergeNode::create(
    const folly::dynamic& obj,
    void* /*unused*/) {
  auto id = obj["id"].asString();
  auto outputType = deserializeRowType(obj["outputType"]);
  std::shared_ptr<AggregationNode> aggregationNode;
  if (obj.count("aggregationNode") != 0) {
    aggregationNode = std::const_pointer_cast<AggregationNode>(
        ISerializable::deserialize<AggregationNode>(obj["aggregationNode"]));
  }
  auto source = ISerializable::deserialize<PlanNode>(obj["sources"]);
  return std::make_shared<TableWriteMergeNode>(
      id, outputType, aggregationNode, source);
}

void MergeExchangeNode::addDetails(std::stringstream& stream) const {
  addSortingKeys(sortingKeys_, sortingOrders_, stream);
}

folly::dynamic MergeExchangeNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["outputType"] = ExchangeNode::outputType()->serialize();
  obj["sortingKeys"] = ISerializable::serialize(sortingKeys_);
  obj["sortingOrders"] = serializeSortingOrders(sortingOrders_);
  return obj;
}

// static
PlanNodePtr MergeExchangeNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto outputType = deserializeRowType(obj["outputType"]);
  auto sortingKeys = deserializeFields(obj["sortingKeys"], context);
  auto sortingOrders = deserializeSortingOrders(obj["sortingOrders"]);
  return std::make_shared<MergeExchangeNode>(
      deserializePlanNodeId(obj), outputType, sortingKeys, sortingOrders);
}

void LocalPartitionNode::addDetails(std::stringstream& stream) const {
  stream << typeName(type_);
  if (type_ != Type::kGather) {
    stream << " " << partitionFunctionSpec_->toString();
  }
}

folly::dynamic LocalPartitionNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["type"] = typeName(type_);
  obj["partitionFunctionSpec"] = partitionFunctionSpec_->serialize();
  return obj;
}

// static
PlanNodePtr LocalPartitionNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<LocalPartitionNode>(
      deserializePlanNodeId(obj),
      typeFromName(obj["type"].asString()),
      ISerializable::deserialize<PartitionFunctionSpec>(
          obj["partitionFunctionSpec"]),
      deserializeSources(obj, context));
}

// static
const char* LocalPartitionNode::typeName(Type type) {
  switch (type) {
    case Type::kGather:
      return "GATHER";
    case Type::kRepartition:
      return "REPARTITION";
  }
  VELOX_UNREACHABLE();
}

namespace {
std::unordered_map<LocalPartitionNode::Type, std::string>
localPartitionTypeNames() {
  return {
      {LocalPartitionNode::Type::kGather, "GATHER"},
      {LocalPartitionNode::Type::kRepartition, "REPARTITION"},
  };
}
} // namespace

// static
LocalPartitionNode::Type LocalPartitionNode::typeFromName(
    const std::string& name) {
  static const auto kTypes = invertMap(localPartitionTypeNames());

  return kTypes.at(name);
}

void EnforceSingleRowNode::addDetails(std::stringstream& /* stream */) const {
  // Nothing to add.
}

folly::dynamic EnforceSingleRowNode::serialize() const {
  return PlanNode::serialize();
}

// static
PlanNodePtr EnforceSingleRowNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<EnforceSingleRowNode>(
      deserializePlanNodeId(obj), deserializeSingleSource(obj, context));
}

// static
std::string PartitionedOutputNode::kindString(Kind kind) {
  switch (kind) {
    case Kind::kPartitioned:
      return "PARTITIONED";
    case Kind::kBroadcast:
      return "BROADCAST";
    case Kind::kArbitrary:
      return "ARBITRARY";
    default:
      return fmt::format("INVALID OUTPUT KIND {}", static_cast<int>(kind));
  }
}

// static
PartitionedOutputNode::Kind PartitionedOutputNode::stringToKind(
    std::string str) {
  if (str == "PARTITIONED") {
    return Kind::kPartitioned;
  } else if (str == "BROADCAST") {
    return Kind::kBroadcast;
  } else if (str == "ARBITRARY") {
    return Kind::kArbitrary;
  } else {
    VELOX_FAIL("Unknown output buffer type: {}", str);
  }
}

void PartitionedOutputNode::addDetails(std::stringstream& stream) const {
  if (kind_ == Kind::kBroadcast) {
    stream << "BROADCAST";
  } else if (kind_ == Kind::kPartitioned) {
    if (numPartitions_ == 1) {
      stream << "SINGLE";
    } else {
      stream << fmt::format(
          "partitionFunction: {} with {} partitions",
          partitionFunctionSpec_->toString(),
          numPartitions_);
    }
  } else {
    stream << "ARBITRARY";
  }

  if (replicateNullsAndAny_) {
    stream << " replicate nulls and any";
  }
}

folly::dynamic PartitionedOutputNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["kind"] = kindString(kind_);
  obj["numPartitions"] = numPartitions_;
  obj["keys"] = ISerializable::serialize(keys_);
  obj["replicateNullsAndAny"] = replicateNullsAndAny_;
  obj["partitionFunctionSpec"] = partitionFunctionSpec_->serialize();
  obj["outputType"] = outputType_->serialize();
  return obj;
}

// static
PlanNodePtr PartitionedOutputNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<PartitionedOutputNode>(
      deserializePlanNodeId(obj),
      stringToKind(obj["kind"].asString()),
      ISerializable::deserialize<std::vector<ITypedExpr>>(obj["keys"], context),
      obj["numPartitions"].asInt(),
      obj["replicateNullsAndAny"].asBool(),
      ISerializable::deserialize<PartitionFunctionSpec>(
          obj["partitionFunctionSpec"], context),
      deserializeRowType(obj["outputType"]),
      deserializeSingleSource(obj, context));
}

TopNNode::TopNNode(
    const PlanNodeId& id,
    const std::vector<FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<SortOrder>& sortingOrders,
    int32_t count,
    bool isPartial,
    const PlanNodePtr& source)
    : PlanNode(id),
      sortingKeys_(sortingKeys),
      sortingOrders_(sortingOrders),
      count_(count),
      isPartial_(isPartial),
      sources_{source} {
  VELOX_USER_CHECK(!sortingKeys.empty(), "TopN must specify sorting keys");
  VELOX_USER_CHECK_EQ(
      sortingKeys.size(),
      sortingOrders.size(),
      "Number of sorting keys and sorting orders in TopN must be the same");
  VELOX_USER_CHECK_GT(
      count, 0, "TopN must specify greater than zero number of rows to keep");
  folly::F14FastSet<std::string> sortingKeyNames;
  for (const auto& sortingKey : sortingKeys_) {
    auto result = sortingKeyNames.insert(sortingKey->name());
    VELOX_USER_CHECK(
        result.second,
        "TopN must specify unique sorting keys. Found duplicate key: {}",
        *result.first);
  }
}

void TopNNode::addDetails(std::stringstream& stream) const {
  if (isPartial_) {
    stream << "PARTIAL ";
  }
  stream << count_ << " ";

  addSortingKeys(sortingKeys_, sortingOrders_, stream);
}

folly::dynamic TopNNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["sortingKeys"] = ISerializable::serialize(sortingKeys_);
  obj["sortingOrders"] = serializeSortingOrders(sortingOrders_);
  obj["count"] = count_;
  obj["partial"] = isPartial_;
  return obj;
}

// static
PlanNodePtr TopNNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);
  auto sortingKeys = deserializeFields(obj["sortingKeys"], context);
  auto sortingOrders = deserializeSortingOrders(obj["sortingOrders"]);

  return std::make_shared<TopNNode>(
      deserializePlanNodeId(obj),
      std::move(sortingKeys),
      std::move(sortingOrders),
      obj["count"].asInt(),
      obj["partial"].asBool(),
      std::move(source));
}

void LimitNode::addDetails(std::stringstream& stream) const {
  if (isPartial_) {
    stream << "PARTIAL ";
  }
  stream << count_;
  if (offset_) {
    stream << " offset " << offset_;
  }
}

folly::dynamic LimitNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["partial"] = isPartial_;
  obj["count"] = count_;
  obj["offset"] = offset_;
  return obj;
}

// static
PlanNodePtr LimitNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);

  return std::make_shared<LimitNode>(
      deserializePlanNodeId(obj),
      obj["offset"].asInt(),
      obj["count"].asInt(),
      obj["partial"].asBool(),
      std::move(source));
}

void OrderByNode::addDetails(std::stringstream& stream) const {
  if (isPartial_) {
    stream << "PARTIAL ";
  }
  addSortingKeys(sortingKeys_, sortingOrders_, stream);
}

folly::dynamic OrderByNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["sortingKeys"] = ISerializable::serialize(sortingKeys_);
  obj["sortingOrders"] = serializeSortingOrders(sortingOrders_);
  obj["partial"] = isPartial_;
  return obj;
}

// static
PlanNodePtr OrderByNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);
  auto sortingKeys = deserializeFields(obj["sortingKeys"], context);
  auto sortingOrders = deserializeSortingOrders(obj["sortingOrders"]);

  return std::make_shared<OrderByNode>(
      deserializePlanNodeId(obj),
      std::move(sortingKeys),
      std::move(sortingOrders),
      obj["partial"].asBool(),
      std::move(source));
}

void MarkDistinctNode::addDetails(std::stringstream& stream) const {
  addFields(stream, distinctKeys_);
}

void PlanNode::toString(
    std::stringstream& stream,
    bool detailed,
    bool recursive,
    size_t indentationSize,
    std::function<void(
        const PlanNodeId& planNodeId,
        const std::string& indentation,
        std::stringstream& stream)> addContext) const {
  const std::string indentation(indentationSize, ' ');

  stream << indentation << "-- " << name();

  if (detailed) {
    stream << "[";
    addDetails(stream);
    stream << "]";
    stream << " -> ";
    outputType()->printChildren(stream, ", ");
  }
  stream << std::endl;

  if (addContext) {
    auto contextIndentation = indentation + "   ";
    stream << contextIndentation;
    addContext(id_, contextIndentation, stream);
    stream << std::endl;
  }

  if (recursive) {
    for (auto& source : sources()) {
      source->toString(stream, detailed, true, indentationSize + 2, addContext);
    }
  }
}

namespace {
void collectLeafPlanNodeIds(
    const core::PlanNode& planNode,
    std::unordered_set<core::PlanNodeId>& leafIds) {
  if (planNode.sources().empty()) {
    leafIds.insert(planNode.id());
    return;
  }

  for (const auto& child : planNode.sources()) {
    collectLeafPlanNodeIds(*child, leafIds);
  }
}
} // namespace

std::unordered_set<core::PlanNodeId> PlanNode::leafPlanNodeIds() const {
  std::unordered_set<core::PlanNodeId> leafIds;
  collectLeafPlanNodeIds(*this, leafIds);
  return leafIds;
}

// static
void PlanNode::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();

  registry.Register("AggregationNode", AggregationNode::create);
  registry.Register("AssignUniqueIdNode", AssignUniqueIdNode::create);
  registry.Register("EnforceSingleRowNode", EnforceSingleRowNode::create);
  registry.Register("ExchangeNode", ExchangeNode::create);
  registry.Register("ExpandNode", ExpandNode::create);
  registry.Register("FilterNode", FilterNode::create);
  registry.Register("GroupIdNode", GroupIdNode::create);
  registry.Register("HashJoinNode", HashJoinNode::create);
  registry.Register("MergeExchangeNode", MergeExchangeNode::create);
  registry.Register("MergeJoinNode", MergeJoinNode::create);
  registry.Register("NestedLoopJoinNode", NestedLoopJoinNode::create);
  registry.Register("LimitNode", LimitNode::create);
  registry.Register("LocalMergeNode", LocalMergeNode::create);
  registry.Register("LocalPartitionNode", LocalPartitionNode::create);
  registry.Register("OrderByNode", OrderByNode::create);
  registry.Register("PartitionedOutputNode", PartitionedOutputNode::create);
  registry.Register("ProjectNode", ProjectNode::create);
  registry.Register("RowNumberNode", RowNumberNode::create);
  registry.Register("TableScanNode", TableScanNode::create);
  registry.Register("TableWriteNode", TableWriteNode::create);
  registry.Register("TableWriteMergeNode", TableWriteMergeNode::create);
  registry.Register("TopNNode", TopNNode::create);
  registry.Register("TopNRowNumberNode", TopNRowNumberNode::create);
  registry.Register("UnnestNode", UnnestNode::create);
  registry.Register("ValuesNode", ValuesNode::create);
  registry.Register("WindowNode", WindowNode::create);
  registry.Register("MarkDistinctNode", MarkDistinctNode::create);
  registry.Register(
      "GatherPartitionFunctionSpec", GatherPartitionFunctionSpec::deserialize);
}

folly::dynamic PlanNode::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = fmt::format("{}Node", name());
  obj["id"] = id_;

  if (!sources().empty()) {
    folly::dynamic serializedSources = folly::dynamic::array;
    for (const auto& source : sources()) {
      serializedSources.push_back(source->serialize());
    }

    obj["sources"] = serializedSources;
  }

  return obj;
}

folly::dynamic FilterNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["filter"] = filter_->serialize();
  return obj;
}

// static
PlanNodePtr FilterNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);

  auto filter = ISerializable::deserialize<ITypedExpr>(obj["filter"]);
  return std::make_shared<FilterNode>(
      deserializePlanNodeId(obj), filter, std::move(source));
}

} // namespace facebook::velox::core
