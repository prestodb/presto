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

#include "velox/common/Casts.h"
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

namespace {
IndexLookupConditionPtr createIndexJoinCondition(
    const folly::dynamic& obj,
    void* context) {
  VELOX_USER_CHECK_EQ(obj.count("type"), 1);
  if (obj["type"] == "in") {
    return InIndexLookupCondition::create(obj, context);
  }
  if (obj["type"] == "between") {
    return BetweenIndexLookupCondition::create(obj, context);
  }
  if (obj["type"] == "equal") {
    return EqualIndexLookupCondition::create(obj, context);
  }
  VELOX_USER_FAIL(
      "Unknown index join condition type {}", obj["type"].asString());
}
} // namespace

/// Deserializes lookup conditions from dynamic object for index lookup joins.
/// These conditions are more complex than simple equality join conditions and
/// can include IN, BETWEEN, and EQUAL conditions that involve both left and
/// right side columns.
std::vector<IndexLookupConditionPtr> deserializejoinConditions(
    const folly::dynamic& obj,
    void* context) {
  if (obj.count("joinConditions") == 0) {
    return {};
  }

  std::vector<IndexLookupConditionPtr> joinConditions;
  joinConditions.reserve(obj.count("joinConditions"));
  for (const auto& joinCondition : obj["joinConditions"]) {
    joinConditions.push_back(createIndexJoinCondition(joinCondition, context));
  }
  return joinConditions;
}

PlanNodePtr deserializeSingleSource(const folly::dynamic& obj, void* context) {
  auto sources = deserializeSources(obj, context);
  VELOX_CHECK_EQ(1, sources.size());

  return sources[0];
}

PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
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

// static
const std::vector<vector_size_t> AggregationNode::kDefaultGlobalGroupingSets =
    {};

// static
const std::optional<FieldAccessTypedExprPtr> AggregationNode::kDefaultGroupId =
    std::nullopt;

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
          kDefaultGlobalGroupingSets,
          kDefaultGroupId,
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

void addVectorSerdeKind(VectorSerde::Kind kind, std::stringstream& stream) {
  stream << VectorSerde::kindName(kind);
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
  return (isFinal() || isSingle()) && !groupingKeys().empty() &&
      preGroupedKeys().empty() && queryConfig.aggregationSpillEnabled();
}

void AggregationNode::addDetails(std::stringstream& stream) const {
  stream << toName(step_) << " ";

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
const auto& stepNames() {
  static const folly::F14FastMap<AggregationNode::Step, std::string_view>
      kNames = {
          {AggregationNode::Step::kPartial, "PARTIAL"},
          {AggregationNode::Step::kFinal, "FINAL"},
          {AggregationNode::Step::kIntermediate, "INTERMEDIATE"},
          {AggregationNode::Step::kSingle, "SINGLE"},
      };
  return kNames;
}

} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(AggregationNode, Step, stepNames)

folly::dynamic AggregationNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["step"] = toName(step_);
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

void AggregationNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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
  auto call = ISerializable::deserialize<CallTypedExpr>(obj["call"], context);
  auto rawInputTypes =
      ISerializable::deserialize<std::vector<Type>>(obj["rawInputTypes"]);
  FieldAccessTypedExprPtr mask;
  if (obj.count("mask")) {
    mask =
        ISerializable::deserialize<FieldAccessTypedExpr>(obj["mask"], context);
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
      toStep(obj["step"].asString()),
      groupingKeys,
      preGroupedKeys,
      aggregateNames,
      aggregates,
      globalGroupingSets,
      groupId,
      obj["ignoreNullKeys"].asBool(),
      deserializeSingleSource(obj, context));
}

PlanNodePtr AggregationNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
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

void ExpandNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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

PlanNodePtr ExpandNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
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

void GroupIdNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr GroupIdNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);
  std::vector<GroupingKeyInfo> groupingKeyInfos;
  for (const auto& info : obj["groupingKeyInfos"]) {
    groupingKeyInfos.push_back(
        {info["output"].asString(),
         ISerializable::deserialize<FieldAccessTypedExpr>(
             info["input"], context)});
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

PlanNodePtr GroupIdNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
}

const std::vector<PlanNodePtr>& ValuesNode::sources() const {
  return kEmptySources;
}

void ValuesNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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

// static
RowTypePtr AbstractProjectNode::makeOutputType(
    const std::vector<std::string>& names,
    const std::vector<TypedExprPtr>& projections) {
  VELOX_USER_CHECK_EQ(names.size(), projections.size());

  std::vector<TypePtr> types;
  types.reserve(projections.size());
  for (const auto& projection : projections) {
    types.push_back(projection->type());
  }

  auto namesCopy = names;
  return ROW(std::move(namesCopy), std::move(types));
}

void AbstractProjectNode::addDetails(std::stringstream& stream) const {
  stream << "expressions: ";
  for (auto i = 0; i < projections_.size(); i++) {
    auto& projection = projections_[i];
    appendComma(i, stream);
    stream << "(" << names_[i] << ":" << projection->type()->toString() << ", "
           << projection->toString() << ")";
  }
}
//
// void AbstractProjectNode::updateNewTypes(
//    const std::map<std::string, std::pair<std::string, TypePtr>>& newTypes) {
//  // Update output type first as it may contain new column names.
//  PlanNode::updateNewTypes(newTypes);
//
//  // Update projections
//  for (auto& projection : projections_) {
//    if (auto field = TypedExprs::asFieldAccess(projection)) {
//      auto it = newTypes.find(field->name());
//      if (it != newTypes.end()) {
//        auto mutableField =
//            std::const_pointer_cast<FieldAccessTypedExpr>(field);
//        mutableField->setName(it->second.first);
//        mutableField->setType(it->second.second);
//      }
//    }
//  }
//
//  // Update names
//  for (auto i = 0; i < names_.size(); i++) {
//    auto& name = names_[i];
//    auto it = newTypes.find(name);
//    if (it != newTypes.end()) {
//      setName(i, it->second.first);
//    }
//  }
//}

namespace {

class SummarizeExprVisitor : public ITypedExprVisitor {
 public:
  class Context : public ITypedExprVisitorContext {
   public:
    std::unordered_map<std::string, int64_t>& functionCounts() {
      return functionCounts_;
    }

    std::unordered_map<std::string, int64_t>& expressionCounts() {
      return expressionCounts_;
    }

    std::unordered_map<velox::TypePtr, int64_t>& constantCounts() {
      return constantCounts_;
    }

   private:
    std::unordered_map<std::string, int64_t> functionCounts_;
    std::unordered_map<std::string, int64_t> expressionCounts_;
    std::unordered_map<velox::TypePtr, int64_t> constantCounts_;
  };

  void visit(const CallTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    const auto& name = expr.name();

    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["call"]++;

    auto& counts = myCtx.functionCounts();
    counts[name]++;

    visitInputs(expr, ctx);
  }

  void visit(const CastTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["cast"]++;
    visitInputs(expr, ctx);
  }

  void visit(const ConcatTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["concat"]++;
    visitInputs(expr, ctx);
  }

  void visit(const ConstantTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["constant"]++;
    myCtx.constantCounts()[expr.type()]++;

    visitInputs(expr, ctx);
  }

  void visit(const DereferenceTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["dereference"]++;
    visitInputs(expr, ctx);
  }

  void visit(const FieldAccessTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    auto& myCtx = static_cast<Context&>(ctx);
    if (expr.isInputColumn()) {
      myCtx.expressionCounts()["field"]++;
    } else {
      myCtx.expressionCounts()["dereference"]++;
    }
    visitInputs(expr, ctx);
  }

  void visit(const InputTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const LambdaTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["lambda"]++;
    expr.body()->accept(*this, ctx);
  }
};

void appendCounts(
    const std::unordered_map<std::string, int64_t>& counts,
    std::stringstream& stream) {
  // Sort map entries by key.
  std::vector<std::string> sortedKeys;
  sortedKeys.reserve(counts.size());
  for (const auto& [name, _] : counts) {
    sortedKeys.push_back(name);
  }
  std::sort(sortedKeys.begin(), sortedKeys.end());

  bool first = true;
  for (const auto& key : sortedKeys) {
    if (first) {
      first = false;
    } else {
      stream << ", ";
    }
    stream << key << ": " << counts.at(key);
  }
}

std::string truncate(const std::string& str, size_t maxLength = 50) {
  if (str.size() > maxLength) {
    return str.substr(0, maxLength) + "...";
  }
  return str;
}

void appendProjections(
    const std::string& indentation,
    const AbstractProjectNode& op,
    const std::vector<size_t>& projections,
    size_t cnt,
    std::stringstream& stream,
    size_t maxLength) {
  if (cnt == 0) {
    return;
  }

  for (auto i = 0; i < cnt; ++i) {
    const auto index = projections[i];
    const auto& expr = op.projections()[index];
    stream << indentation << op.outputType()->nameOf(index) << ": "
           << truncate(expr->toString(), maxLength) << std::endl;
  }

  if (cnt < projections.size()) {
    stream << indentation << "... " << (projections.size() - cnt) << " more"
           << std::endl;
  }
}

void appendAggregations(
    const std::string& indentation,
    const AggregationNode& op,
    size_t cnt,
    std::stringstream& stream,
    size_t maxLength) {
  if (cnt == 0) {
    return;
  }

  for (auto i = 0; i < cnt; ++i) {
    const auto& expr = op.aggregates().at(i).call;
    stream << indentation << i << ": "
           << (op.aggregates().at(i).distinct ? "DISTINCT" : "")
           << truncate(expr->toString(), maxLength) << std::endl;
  }

  if (cnt < op.aggregates().size()) {
    stream << indentation << "... " << (op.aggregates().size() - cnt) << " more"
           << std::endl;
  }
}

void appendExprSummary(
    const std::string& indentation,
    const PlanSummaryOptions& options,
    SummarizeExprVisitor::Context& exprCtx,
    std::stringstream& stream) {
  stream << indentation << "expressions: ";
  appendCounts(exprCtx.expressionCounts(), stream);
  stream << std::endl;

  if (!exprCtx.functionCounts().empty()) {
    stream << indentation << "functions: ";
    appendCounts(exprCtx.functionCounts(), stream);
    stream << std::endl;
  }

  if (!exprCtx.constantCounts().empty()) {
    stream << indentation << "constants: ";
    std::unordered_map<std::string, int64_t> counts;
    for (const auto& [type, count] : exprCtx.constantCounts()) {
      counts[type->toSummaryString(
          {.maxChildren = (uint32_t)options.maxChildTypes})] += count;
    }
    appendCounts(counts, stream);
    stream << std::endl;
  }
}

} // namespace

void AbstractProjectNode::addSummaryDetails(
    const std::string& indentation,
    const PlanSummaryOptions& options,
    std::stringstream& stream) const {
  SummarizeExprVisitor::Context exprCtx;
  SummarizeExprVisitor visitor;
  for (const auto& projection : projections_) {
    // Skip identity projections.
    if (projection->isFieldAccessKind() &&
        projection->asUnchecked<FieldAccessTypedExpr>()->isInputColumn()) {
      continue;
    }

    projection->accept(visitor, exprCtx);
  }

  appendExprSummary(indentation, options, exprCtx, stream);

  // Collect non-identity projections.
  const size_t numFields = outputType()->size();

  std::vector<size_t> projections;
  projections.reserve(numFields);

  std::vector<size_t> dereferences;
  dereferences.reserve(numFields);

  std::vector<size_t> constants;
  constants.reserve(numFields);

  for (auto i = 0; i < numFields; ++i) {
    const auto& expr = projections_[i];
    if (expr->isDereferenceKind()) {
      dereferences.push_back(i);
    } else if (expr->isConstantKind()) {
      constants.push_back(i);
    } else {
      auto fae = expr->asUnchecked<FieldAccessTypedExpr>();
      if (fae == nullptr) {
        projections.push_back(i);
      } else if (!fae->isInputColumn()) {
        dereferences.push_back(i);
      }
    }
  }

  // projections: 4 out of 10
  if (!projections.empty()) {
    stream << indentation << "projections: " << projections.size() << " out of "
           << numFields << std::endl;
    {
      const auto cnt =
          std::min(options.project.maxProjections, projections.size());
      appendProjections(
          indentation + "   ",
          *this,
          projections,
          cnt,
          stream,
          options.maxLength);
    }
  }

  // dereferences: 2 out of 10
  if (!dereferences.empty()) {
    stream << indentation << "dereferences: " << dereferences.size()
           << " out of " << numFields << std::endl;
    {
      const auto cnt =
          std::min(options.project.maxDereferences, dereferences.size());
      appendProjections(
          indentation + "   ",
          *this,
          dereferences,
          cnt,
          stream,
          options.maxLength);
    }
  }

  // constants: 1 out of 10
  if (!constants.empty()) {
    stream << indentation << "constant projections: " << constants.size()
           << " out of " << numFields << std::endl;
    {
      const auto cnt = std::min(options.project.maxConstants, constants.size());
      appendProjections(
          indentation + "   ",
          *this,
          constants,
          cnt,
          stream,
          options.maxLength);
    }
  }
}

folly::dynamic ProjectNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["names"] = ISerializable::serialize(names_);
  obj["projections"] = ISerializable::serialize(projections_);
  return obj;
}

void ProjectNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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

PlanNodePtr ProjectNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
}

namespace {
// makes a list of all names for use in the ProjectNode.
std::vector<std::string> allNames(
    const std::vector<std::string>& names,
    const std::vector<std::string>& moreNames) {
  auto result = names;
  result.insert(result.end(), moreNames.begin(), moreNames.end());
  return result;
}

// Flattens out projection exprs and adds dummy  exprs for noLoadIdentities.
// Used to fill in ProjectNode members for use in the summary functions.
std::vector<TypedExprPtr> flattenExprs(
    const std::vector<std::vector<TypedExprPtr>>& exprs,
    const std::vector<std::string>& moreNames,
    const PlanNodePtr& input) {
  std::vector<TypedExprPtr> result;
  for (auto& group : exprs) {
    result.insert(result.end(), group.begin(), group.end());
  }

  const auto& sourceType = input->outputType();
  for (auto& name : moreNames) {
    result.push_back(
        std::make_shared<FieldAccessTypedExpr>(
            sourceType->findChild(name), name));
  }
  return result;
}
} // namespace

ParallelProjectNode::ParallelProjectNode(
    const PlanNodeId& id,
    std::vector<std::string> names,
    std::vector<std::vector<TypedExprPtr>> exprGroups,
    std::vector<std::string> noLoadIdentities,
    PlanNodePtr input)
    : AbstractProjectNode(
          id,
          allNames(names, noLoadIdentities),
          flattenExprs(exprGroups, noLoadIdentities, input),
          input),
      exprNames_(std::move(names)),
      exprGroups_(std::move(exprGroups)),
      noLoadIdentities_(std::move(noLoadIdentities)) {
  VELOX_USER_CHECK(!exprNames_.empty());
  VELOX_USER_CHECK(!exprGroups_.empty());

  for (const auto& group : exprGroups_) {
    VELOX_USER_CHECK(!group.empty());
  }
}

void ParallelProjectNode::addDetails(std::stringstream& stream) const {
  AbstractProjectNode::addDetails(stream);
  stream << " Parallel expr groups: ";
  int32_t start = 0;
  for (auto i = 0; i < exprGroups_.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << fmt::format("[{}-{}]", start, start + exprGroups_[i].size() - 1);
    start += exprGroups_[i].size();
  }
  stream << std::endl;
}

folly::dynamic ParallelProjectNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["names"] = ISerializable::serialize(exprNames_);
  obj["projections"] = ISerializable::serialize(exprGroups_);
  obj["noLoadIdentities"] = ISerializable::serialize(noLoadIdentities_);
  return obj;
}

void ParallelProjectNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr ParallelProjectNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto source = deserializeSingleSource(obj, context);

  auto names = deserializeStrings(obj["names"]);
  auto projections =
      ISerializable::deserialize<std::vector<std::vector<ITypedExpr>>>(
          obj["projections"], context);
  auto noLoadIdentities = deserializeStrings(obj["noLoadIdentities"]);
  return std::make_shared<ParallelProjectNode>(
      deserializePlanNodeId(obj),
      std::move(names),
      std::move(projections),
      std::move(noLoadIdentities),
      std::move(source));
}

PlanNodePtr ParallelProjectNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1, "LazyDereferenceNode is unary");

  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
}

// static
PlanNodePtr LazyDereferenceNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto source = deserializeSingleSource(obj, context);

  auto names = deserializeStrings(obj["names"]);
  auto projections = ISerializable::deserialize<std::vector<ITypedExpr>>(
      obj["projections"], context);
  return std::make_shared<LazyDereferenceNode>(
      deserializePlanNodeId(obj),
      std::move(names),
      std::move(projections),
      std::move(source));
}

PlanNodePtr LazyDereferenceNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1, "LazyDereferenceNode is unary");

  ProjectNode::Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
}

const std::vector<PlanNodePtr>& TableScanNode::sources() const {
  return kEmptySources;
}

void TableScanNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

void TableScanNode::addDetails(std::stringstream& stream) const {
  stream << tableHandle_->toString();
}

void TableScanNode::addSummaryDetails(
    const std::string& indentation,
    const PlanSummaryOptions& /* options */,
    std::stringstream& stream) const {
  stream << indentation << tableHandle_->toString() << std::endl;
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
  auto tableHandle =
      ISerializable::deserialize<connector::ConnectorTableHandle>(
          obj["tableHandle"], context);

  connector::ColumnHandleMap assignments;
  for (const auto& pair : obj["assignments"]) {
    auto assign = pair["assign"].asString();
    auto columnHandle = ISerializable::deserialize<connector::ColumnHandle>(
        pair["columnHandle"], context);
    assignments[assign] = std::move(columnHandle);
  }

  return std::make_shared<const TableScanNode>(
      planNodeId, outputType, tableHandle, assignments);
}

// PlanNodePtr TableScanNode::copyWithNewSources(
//     std::vector<PlanNodePtr> newSources) const {
//   VELOX_CHECK_EQ(newSources.size(), 1, "LazyDereferenceNode is unary");
//
//   Builder builder(*this);
//   return builder.build();
// }

const std::vector<PlanNodePtr>& ArrowStreamNode::sources() const {
  return kEmptySources;
}

void ArrowStreamNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

void ArrowStreamNode::addDetails(std::stringstream& stream) const {
  // Nothing to add.
}

const std::vector<PlanNodePtr>& ExchangeNode::sources() const {
  return kEmptySources;
}

void ExchangeNode::addDetails(std::stringstream& stream) const {
  addVectorSerdeKind(serdeKind_, stream);
}

folly::dynamic ExchangeNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["outputType"] = ExchangeNode::outputType()->serialize();
  obj["serdeKind"] = VectorSerde::kindName(serdeKind_);
  return obj;
}

void ExchangeNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr ExchangeNode::create(const folly::dynamic& obj, void* context) {
  return std::make_shared<ExchangeNode>(
      deserializePlanNodeId(obj),
      deserializeRowType(obj["outputType"]),
      VectorSerde::kindByName(obj["serdeKind"].asString()));
}

UnnestNode::UnnestNode(
    const PlanNodeId& id,
    std::vector<FieldAccessTypedExprPtr> replicateVariables,
    std::vector<FieldAccessTypedExprPtr> unnestVariables,
    std::vector<std::string> unnestNames,
    std::optional<std::string> ordinalityName,
    std::optional<std::string> markerName,
    const PlanNodePtr& source)
    : PlanNode(id),
      replicateVariables_{std::move(replicateVariables)},
      unnestVariables_{std::move(unnestVariables)},
      unnestNames_{std::move(unnestNames)},
      ordinalityName_{std::move(ordinalityName)},
      markerName_(std::move(markerName)),
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
      names.emplace_back(unnestNames_[unnestIndex++]);
      types.emplace_back(variable->type()->asArray().elementType());
    } else if (variable->type()->isMap()) {
      const auto& mapType = variable->type()->asMap();

      names.emplace_back(unnestNames_[unnestIndex++]);
      types.emplace_back(mapType.keyType());

      names.emplace_back(unnestNames_[unnestIndex++]);
      types.emplace_back(mapType.valueType());
    } else {
      VELOX_FAIL(
          "Unexpected type of unnest variable. Expected ARRAY or MAP, but got {}.",
          variable->type()->toString());
    }
  }

  if (ordinalityName_.has_value()) {
    names.emplace_back(ordinalityName_.value());
    types.emplace_back(BIGINT());
  }

  if (markerName_.has_value()) {
    names.emplace_back(markerName_.value());
    types.emplace_back(BOOLEAN());
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
  obj["unnestNames"] = ISerializable::serialize(unnestNames_);

  if (ordinalityName_.has_value()) {
    obj["ordinalityName"] = ordinalityName_.value();
  }
  if (markerName_.has_value()) {
    obj["markerName"] = markerName_.value();
  }
  return obj;
}

void UnnestNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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
  std::optional<std::string> markerName = std::nullopt;
  if (obj.count("markerName")) {
    markerName = obj["markerName"].asString();
  }
  return std::make_shared<UnnestNode>(
      deserializePlanNodeId(obj),
      std::move(replicateVariables),
      std::move(unnestVariables),
      std::move(unnestNames),
      std::move(ordinalityName),
      std::move(markerName),
      std::move(source));
}

PlanNodePtr UnnestNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
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
      outputType_(std::move(outputType)) {}

namespace {
bool isIntegral(const TypePtr& type) {
  return type->isBigint() || type->isInteger() || type->isSmallint() ||
      type->isTinyint();
}
} // namespace

void AbstractJoinNode::validate() const {
  VELOX_CHECK(!leftKeys_.empty(), "JoinNode requires at least one join key");
  VELOX_CHECK_EQ(
      leftKeys_.size(),
      rightKeys_.size(),
      "JoinNode requires same number of join keys on left and right sides");
  auto leftType = sources_[0]->outputType();
  for (const auto& key : leftKeys_) {
    VELOX_CHECK(
        leftType->containsChild(key->name()),
        "Left side join key not found in left side output: {}",
        key->name());
  }
  auto rightType = sources_[1]->outputType();
  for (const auto& key : rightKeys_) {
    VELOX_CHECK(
        rightType->containsChild(key->name()),
        "Right side join key not found in right side output: {}",
        key->name());
  }
  for (auto i = 0; i < leftKeys_.size(); ++i) {
    auto leftType = leftKeys_[i]->type();
    auto rightType = rightKeys_[i]->type();
    if (isIntegral(leftType) && isIntegral(rightType)) {
      continue;
    }

    VELOX_CHECK_EQ(
        leftType,
        rightType,
        "Join key types on the left and right sides must match");
  }

  auto numOutputColumns = outputType_->size();
  if (isLeftSemiProjectJoin() || isRightSemiProjectJoin()) {
    // Last output column must be a boolean 'match'.
    --numOutputColumns;
    VELOX_CHECK_EQ(outputType_->childAt(numOutputColumns), BOOLEAN());

    // Verify that 'match' column name doesn't match any column from left or
    // right source.
    const auto& name = outputType_->nameOf(numOutputColumns);
    VELOX_CHECK(!leftType->containsChild(name));
    VELOX_CHECK(!rightType->containsChild(name));
  }

  // Output of right semi join cannot include columns from the left side.
  bool outputMayIncludeLeftColumns =
      !(isRightSemiFilterJoin() || isRightSemiProjectJoin());

  // Output of left semi and anti joins cannot include columns from the right
  // side.
  bool outputMayIncludeRightColumns =
      !(isLeftSemiFilterJoin() || isLeftSemiProjectJoin() || isAntiJoin());

  for (auto i = 0; i < numOutputColumns; ++i) {
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

// void AbstractJoinNode::updateNewTypes(
//     const std::map<std::string, std::pair<std::string, TypePtr>>& newTypes) {
//   // Update output type first as it may contain new column names.
//   PlanNode::updateNewTypes(newTypes);
//
//   // Update join keys.
//   for (auto& key : leftKeys_) {
//     auto iter = newTypes.find(key->name());
//     if (iter != newTypes.end()) {
//       auto mutableKey = std::const_pointer_cast<FieldAccessTypedExpr>(key);
//       mutableKey->updateNewType(iter->second.first, iter->second.second);
//     }
//   }
//   for (auto& key : rightKeys_) {
//     auto iter = newTypes.find(key->name());
//     if (iter != newTypes.end()) {
//       auto mutableKey = std::const_pointer_cast<FieldAccessTypedExpr>(key);
//       mutableKey->updateNewType(iter->second.first, iter->second.second);
//     }
//   }
// }

void AbstractJoinNode::addDetails(std::stringstream& stream) const {
  stream << JoinTypeName::toName(joinType_) << " ";

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
  obj["joinType"] = JoinTypeName::toName(joinType_);
  obj["leftKeys"] = ISerializable::serialize(leftKeys_);
  obj["rightKeys"] = ISerializable::serialize(rightKeys_);
  if (filter_) {
    obj["filter"] = filter_->serialize();
  }
  obj["outputType"] = outputType_->serialize();
  return obj;
}

namespace {
const auto& joinTypeNames() {
  static const folly::F14FastMap<JoinType, std::string_view> kNames = {
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
  return kNames;
}

// Check that each output of the join is in exactly one of the inputs.
void checkJoinColumnNames(
    const RowTypePtr& leftType,
    const RowTypePtr& rightType,
    const RowTypePtr& outputType,
    uint32_t numColumnsToCheck) {
  for (auto i = 0; i < numColumnsToCheck; ++i) {
    const auto name = outputType->nameOf(i);
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

} // namespace

VELOX_DEFINE_ENUM_NAME(JoinType, joinTypeNames)

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

void HashJoinNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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
    filter = ISerializable::deserialize<ITypedExpr>(obj["filter"], context);
  }

  auto outputType = deserializeRowType(obj["outputType"]);

  return std::make_shared<HashJoinNode>(
      deserializePlanNodeId(obj),
      JoinTypeName::toJoinType(obj["joinType"].asString()),
      nullAware,
      std::move(leftKeys),
      std::move(rightKeys),
      filter,
      sources[0],
      sources[1],
      outputType);
}

namespace {
std::vector<core::FieldAccessTypedExprPtr> getKeysFromSource(
    const std::vector<core::FieldAccessTypedExprPtr>& keys,
    const PlanNodePtr& source) {
  std::vector<core::FieldAccessTypedExprPtr> result;
  auto sourceType = source->outputType();
  for (const auto& key : keys) {
    auto keyName = key->name();
    auto keyType = sourceType->findChild(keyName);
    VELOX_CHECK_NOT_NULL(
        keyType, "Join key not found in source output: {}", keyName);
    result.push_back(std::make_shared<FieldAccessTypedExpr>(keyType, keyName));
  }
  return result;
}
} // namespace

PlanNodePtr HashJoinNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 2);

  std::vector<core::FieldAccessTypedExprPtr> leftKeys =
      getKeysFromSource(leftKeys_, newSources[0]);
  std::vector<core::FieldAccessTypedExprPtr> rightKeys =
      getKeysFromSource(rightKeys_, newSources[1]);

  Builder builder(*this);
  builder.left(std::move(newSources[0]))
      .right(std::move(newSources[1]))
      .leftKeys(std::move(leftKeys))
      .rightKeys(std::move(rightKeys));
  return builder.build();
}

MergeJoinNode::MergeJoinNode(
    const PlanNodeId& id,
    JoinType joinType,
    const std::vector<FieldAccessTypedExprPtr>& leftKeys,
    const std::vector<FieldAccessTypedExprPtr>& rightKeys,
    TypedExprPtr filter,
    PlanNodePtr left,
    PlanNodePtr right,
    RowTypePtr outputType)
    : AbstractJoinNode(
          id,
          joinType,
          leftKeys,
          rightKeys,
          std::move(filter),
          std::move(left),
          std::move(right),
          std::move(outputType)) {
  validate();
  VELOX_USER_CHECK(
      isSupported(joinType_),
      "The join type is not supported by merge join: {}",
      JoinTypeName::toName(joinType_));
}

folly::dynamic MergeJoinNode::serialize() const {
  return serializeBase();
}

// static
bool MergeJoinNode::isSupported(JoinType joinType) {
  switch (joinType) {
    case JoinType::kInner:
    case JoinType::kLeft:
    case JoinType::kRight:
    case JoinType::kLeftSemiFilter:
    case JoinType::kRightSemiFilter:
    case JoinType::kAnti:
    case JoinType::kFull:
      return true;

    default:
      return false;
  }
}

void MergeJoinNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr MergeJoinNode::create(const folly::dynamic& obj, void* context) {
  auto sources = deserializeSources(obj, context);
  VELOX_CHECK_EQ(2, sources.size());

  auto leftKeys = deserializeFields(obj["leftKeys"], context);
  auto rightKeys = deserializeFields(obj["rightKeys"], context);

  TypedExprPtr filter;
  if (obj.count("filter")) {
    filter = ISerializable::deserialize<ITypedExpr>(obj["filter"], context);
  }

  auto outputType = deserializeRowType(obj["outputType"]);

  return std::make_shared<MergeJoinNode>(
      deserializePlanNodeId(obj),
      JoinTypeName::toJoinType(obj["joinType"].asString()),
      std::move(leftKeys),
      std::move(rightKeys),
      filter,
      sources[0],
      sources[1],
      outputType);
}

PlanNodePtr MergeJoinNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 2);

  std::vector<core::FieldAccessTypedExprPtr> leftKeys =
      getKeysFromSource(leftKeys_, newSources[0]);
  std::vector<core::FieldAccessTypedExprPtr> rightKeys =
      getKeysFromSource(rightKeys_, newSources[1]);

  Builder builder(*this);
  builder.left(std::move(newSources[0]))
      .right(std::move(newSources[1]))
      .leftKeys(std::move(leftKeys))
      .rightKeys(std::move(rightKeys));
  return builder.build();
}

IndexLookupJoinNode::IndexLookupJoinNode(
    const PlanNodeId& id,
    JoinType joinType,
    const std::vector<FieldAccessTypedExprPtr>& leftKeys,
    const std::vector<FieldAccessTypedExprPtr>& rightKeys,
    const std::vector<IndexLookupConditionPtr>& joinConditions,
    TypedExprPtr filter,
    bool hasMarker,
    PlanNodePtr left,
    TableScanNodePtr right,
    RowTypePtr outputType)
    : AbstractJoinNode(
          id,
          joinType,
          leftKeys,
          rightKeys,
          std::move(filter),
          std::move(left),
          right,
          outputType),
      lookupSourceNode_(std::move(right)),
      joinConditions_(joinConditions),
      hasMarker_(hasMarker) {
  VELOX_USER_CHECK(
      !leftKeys.empty(),
      "The index lookup join node requires at least one join key");
  VELOX_USER_CHECK_EQ(
      leftKeys_.size(),
      rightKeys_.size(),
      "The index lookup join node requires same number of join keys on left and right sides");

  // TODO: add check that (1) 'rightKeys_' form an index prefix. each of
  // 'joinConditions_' uses columns from both sides and uses exactly one index
  // column from the right side.
  VELOX_USER_CHECK(
      lookupSourceNode_->tableHandle()->supportsIndexLookup(),
      "The lookup table handle {} from connector {} doesn't support index lookup",
      lookupSourceNode_->tableHandle()->name(),
      lookupSourceNode_->tableHandle()->connectorId());
  VELOX_USER_CHECK(
      isSupported(joinType_),
      "Unsupported index lookup join type {}",
      JoinTypeName::toName(joinType_));

  auto leftType = sources_[0]->outputType();
  for (const auto& key : leftKeys_) {
    VELOX_USER_CHECK(
        leftType->containsChild(key->name()),
        "Left side join key not found in left side output: {}",
        key->name());
  }
  auto rightType = sources_[1]->outputType();
  for (const auto& key : rightKeys_) {
    VELOX_USER_CHECK(
        rightType->containsChild(key->name()),
        "Right side join key not found in right side output: {}",
        key->name());
  }
  for (auto i = 0; i < leftKeys_.size(); ++i) {
    VELOX_USER_CHECK_EQ(
        leftKeys_[i]->type()->kind(),
        rightKeys_[i]->type()->kind(),
        "Index lookup koin key types on the left and right sides must match");
  }

  auto numOutputColumns = outputType_->size();
  if (hasMarker_) {
    VELOX_USER_CHECK(
        isLeftJoin(),
        "Index join match column can only present for {} but not {}",
        JoinTypeName::toName(JoinType::kLeft),
        JoinTypeName::toName(joinType_));
    // Last output column must be a boolean 'match'.
    --numOutputColumns;
    VELOX_USER_CHECK_EQ(
        outputType_->childAt(numOutputColumns),
        BOOLEAN(),
        "The last output column must be boolean type if match column is present");

    // Verify that 'match' column name doesn't match any column from left or
    // right source.
    const auto& name = outputType_->nameOf(numOutputColumns);
    VELOX_USER_CHECK(!leftType->containsChild(name));
    VELOX_USER_CHECK(!rightType->containsChild(name));
  }

  checkJoinColumnNames(leftType, rightType, outputType_, numOutputColumns);
}

PlanNodePtr IndexLookupJoinNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto sources = deserializeSources(obj, context);
  VELOX_CHECK_EQ(2, sources.size());
  TableScanNodePtr lookupSource =
      checked_pointer_cast<const TableScanNode>(sources[1]);

  auto leftKeys = deserializeFields(obj["leftKeys"], context);
  auto rightKeys = deserializeFields(obj["rightKeys"], context);

  TypedExprPtr filter;
  if (obj.count("filter")) {
    filter = ISerializable::deserialize<ITypedExpr>(obj["filter"], context);
  }

  auto joinConditions = deserializejoinConditions(obj, context);

  const bool hasMarker = obj["hasMarker"].asBool();

  auto outputType = deserializeRowType(obj["outputType"]);

  return std::make_shared<IndexLookupJoinNode>(
      deserializePlanNodeId(obj),
      JoinTypeName::toJoinType(obj["joinType"].asString()),
      std::move(leftKeys),
      std::move(rightKeys),
      std::move(joinConditions),
      filter,
      hasMarker,
      sources[0],
      std::move(lookupSource),
      std::move(outputType));
}

PlanNodePtr IndexLookupJoinNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 2);

  std::vector<core::FieldAccessTypedExprPtr> leftKeys =
      getKeysFromSource(leftKeys_, newSources[0]);
  std::vector<core::FieldAccessTypedExprPtr> rightKeys =
      getKeysFromSource(rightKeys_, newSources[1]);

  Builder builder(*this);
  builder.left(std::move(newSources[0]))
      .right(std::move(newSources[1]))
      .leftKeys(std::move(leftKeys))
      .rightKeys(std::move(rightKeys));
  return builder.build();
}

folly::dynamic IndexLookupJoinNode::serialize() const {
  auto obj = serializeBase();
  if (!joinConditions_.empty()) {
    folly::dynamic serializedJoins = folly::dynamic::array();
    for (const auto& joinCondition : joinConditions_) {
      serializedJoins.push_back(joinCondition->serialize());
    }
    obj["joinConditions"] = std::move(serializedJoins);
  }
  if (filter_) {
    obj["filter"] = filter_->serialize();
  }
  obj["hasMarker"] = hasMarker_;
  return obj;
}

void IndexLookupJoinNode::addDetails(std::stringstream& stream) const {
  AbstractJoinNode::addDetails(stream);
  if (joinConditions_.empty()) {
    return;
  }

  std::vector<std::string> joinConditionstrs;
  joinConditionstrs.reserve(joinConditions_.size());
  for (const auto& joinCondition : joinConditions_) {
    joinConditionstrs.push_back(joinCondition->toString());
  }
  stream << ", joinConditions: [" << folly::join(", ", joinConditionstrs)
         << " ], filter: ["
         << (filter_ == nullptr ? "null" : filter_->toString())
         << "], hasMarker: [" << (hasMarker_ ? "true" : "false") << "]";
}

void IndexLookupJoinNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
bool IndexLookupJoinNode::isSupported(JoinType joinType) {
  switch (joinType) {
    case JoinType::kInner:
      [[fallthrough]];
    case JoinType::kLeft:
      return true;
    default:
      return false;
  }
}

bool isIndexLookupJoin(const PlanNode* planNode) {
  return is_instance_of<IndexLookupJoinNode>(planNode);
}

// static
const JoinType NestedLoopJoinNode::kDefaultJoinType = JoinType::kInner;
// static
const TypedExprPtr NestedLoopJoinNode::kDefaultJoinCondition = nullptr;

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
      isSupported(joinType_),
      "The join type is not supported by nested loop join: {}",
      JoinTypeName::toName(joinType_));

  auto leftType = sources_[0]->outputType();
  auto rightType = sources_[1]->outputType();
  auto numOutputColumns = outputType_->size();
  if (isLeftSemiProjectJoin(joinType)) {
    --numOutputColumns;
    VELOX_CHECK_EQ(outputType_->childAt(numOutputColumns), BOOLEAN());
    const auto& name = outputType_->nameOf(numOutputColumns);
    VELOX_CHECK(
        !leftType->containsChild(name),
        "Match column '{}' cannot be present in left source.",
        name);
    VELOX_CHECK(
        !rightType->containsChild(name),
        "Match column '{}' cannot be present in right source.",
        name);
  }

  checkJoinColumnNames(leftType, rightType, outputType_, numOutputColumns);
}

NestedLoopJoinNode::NestedLoopJoinNode(
    const PlanNodeId& id,
    PlanNodePtr left,
    PlanNodePtr right,
    RowTypePtr outputType)
    : NestedLoopJoinNode(
          id,
          kDefaultJoinType,
          kDefaultJoinCondition,
          std::move(left),
          std::move(right),
          std::move(outputType)) {}

// static
bool NestedLoopJoinNode::isSupported(JoinType joinType) {
  switch (joinType) {
    case JoinType::kInner:
    case JoinType::kLeft:
    case JoinType::kRight:
    case JoinType::kFull:
    case JoinType::kLeftSemiProject:
      return true;

    default:
      return false;
  }
}

void NestedLoopJoinNode::addDetails(std::stringstream& stream) const {
  stream << JoinTypeName::toName(joinType_);
  if (joinCondition_) {
    stream << ", joinCondition: " << joinCondition_->toString();
  }
}

folly::dynamic NestedLoopJoinNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["joinType"] = JoinTypeName::toName(joinType_);
  if (joinCondition_) {
    obj["joinCondition"] = joinCondition_->serialize();
  }
  obj["outputType"] = outputType_->serialize();
  return obj;
}

void NestedLoopJoinNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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
      JoinTypeName::toJoinType(obj["joinType"].asString()),
      joinCondition,
      sources[0],
      sources[1],
      outputType);
}

PlanNodePtr NestedLoopJoinNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 2);

  Builder builder(*this);
  builder.left(std::move(newSources[0])).right(std::move(newSources[1]));
  return builder.build();
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

void AssignUniqueIdNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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

PlanNodePtr AssignUniqueIdNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
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

  stream << WindowNode::toName(frame.type) << " between ";
  if (frame.startValue) {
    addKeys(stream, {frame.startValue});
    stream << " ";
  }
  stream << WindowNode::toName(frame.startType) << " and ";
  if (frame.endValue) {
    addKeys(stream, {frame.endValue});
    stream << " ";
  }
  stream << WindowNode::toName(frame.endType);
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
      windowColumnNames_(std::move(windowColumnNames)),
      windowFunctions_(std::move(windowFunctions)),
      inputsSorted_(inputsSorted),
      sources_{std::move(source)},
      outputType_(getWindowOutputType(
          sources_[0]->outputType(),
          windowColumnNames_,
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
const auto& boundTypeNames() {
  static const folly::F14FastMap<WindowNode::BoundType, std::string_view>
      kNames = {
          {WindowNode::BoundType::kCurrentRow, "CURRENT ROW"},
          {WindowNode::BoundType::kPreceding, "PRECEDING"},
          {WindowNode::BoundType::kFollowing, "FOLLOWING"},
          {WindowNode::BoundType::kUnboundedPreceding, "UNBOUNDED PRECEDING"},
          {WindowNode::BoundType::kUnboundedFollowing, "UNBOUNDED FOLLOWING"},
      };
  return kNames;
}
} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(WindowNode, BoundType, boundTypeNames)

namespace {
const auto& windowTypeNames() {
  static const folly::F14FastMap<WindowNode::WindowType, std::string_view>
      kNames = {
          {WindowNode::WindowType::kRows, "ROWS"},
          {WindowNode::WindowType::kRange, "RANGE"},
      };
  return kNames;
}
} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(WindowNode, WindowType, windowTypeNames)

folly::dynamic WindowNode::Frame::serialize() const {
  folly::dynamic obj = folly::dynamic::object();
  obj["type"] = toName(type);
  obj["startType"] = toName(startType);
  if (startValue) {
    obj["startValue"] = startValue->serialize();
  }
  obj["endType"] = toName(endType);
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
      toWindowType(obj["type"].asString()),
      toBoundType(obj["startType"].asString()),
      startValue,
      toBoundType(obj["endType"].asString()),
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

  obj["names"] = ISerializable::serialize(windowColumnNames_);
  obj["inputsSorted"] = inputsSorted_;

  return obj;
}

void WindowNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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

PlanNodePtr WindowNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
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
  VELOX_USER_CHECK_GT(markerName_.size(), 0);
  VELOX_USER_CHECK_GT(distinctKeys_.size(), 0);
}

folly::dynamic MarkDistinctNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["distinctKeys"] = ISerializable::serialize(this->distinctKeys_);
  obj["markerName"] = this->markerName_;
  return obj;
}

void MarkDistinctNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr MarkDistinctNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);
  auto distinctKeys = deserializeFields(obj["distinctKeys"], context);
  auto markerName = obj["markerName"].asString();

  return std::make_shared<MarkDistinctNode>(
      deserializePlanNodeId(obj), markerName, distinctKeys, source);
}

PlanNodePtr MarkDistinctNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
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

void RowNumberNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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

PlanNodePtr RowNumberNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
}

namespace {
std::unordered_map<TopNRowNumberNode::RankFunction, std::string>
rankFunctionNames() {
  return {
      {TopNRowNumberNode::RankFunction::kRowNumber, "row_number"},
      {TopNRowNumberNode::RankFunction::kRank, "rank"},
      {TopNRowNumberNode::RankFunction::kDenseRank, "dense_rank"},
  };
}
} // namespace

// static
const char* TopNRowNumberNode::rankFunctionName(
    TopNRowNumberNode::RankFunction function) {
  static const auto kFunctionNames = rankFunctionNames();
  auto it = kFunctionNames.find(function);
  VELOX_CHECK(
      it != kFunctionNames.end(),
      "Invalid rank function {}",
      static_cast<int>(function));
  return it->second.c_str();
}

// static
TopNRowNumberNode::RankFunction TopNRowNumberNode::rankFunctionFromName(
    std::string_view name) {
  static const auto kFunctionNames = invertMap(rankFunctionNames());
  auto it = kFunctionNames.find(name.data());
  VELOX_CHECK(it != kFunctionNames.end(), "Invalid rank function {}", name);
  return it->second;
}

TopNRowNumberNode::TopNRowNumberNode(
    PlanNodeId id,
    RankFunction function,
    std::vector<FieldAccessTypedExprPtr> partitionKeys,
    std::vector<FieldAccessTypedExprPtr> sortingKeys,
    std::vector<SortOrder> sortingOrders,
    const std::optional<std::string>& rowNumberColumnName,
    int32_t limit,
    PlanNodePtr source)
    : PlanNode(std::move(id)),
      function_(function),
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
  stream << rankFunctionName(function_) << " ";

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
  obj["function"] = rankFunctionName(function_);
  obj["partitionKeys"] = ISerializable::serialize(partitionKeys_);
  obj["sortingKeys"] = ISerializable::serialize(sortingKeys_);
  obj["sortingOrders"] = serializeSortingOrders(sortingOrders_);
  if (generateRowNumber()) {
    obj["rowNumberColumnName"] = outputType_->names().back();
  }
  obj["limit"] = limit_;
  return obj;
}

void TopNRowNumberNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr TopNRowNumberNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto source = deserializeSingleSource(obj, context);
  auto function = rankFunctionFromName(obj["function"].asString());
  auto partitionKeys = deserializeFields(obj["partitionKeys"], context);
  auto sortingKeys = deserializeFields(obj["sortingKeys"], context);

  auto sortingOrders = deserializeSortingOrders(obj["sortingOrders"]);

  std::optional<std::string> rowNumberColumnName;
  if (obj.count("rowNumberColumnName")) {
    rowNumberColumnName = obj["rowNumberColumnName"].asString();
  }

  return std::make_shared<TopNRowNumberNode>(
      deserializePlanNodeId(obj),
      function,
      partitionKeys,
      sortingKeys,
      sortingOrders,
      rowNumberColumnName,
      obj["limit"].asInt(),
      source);
}

PlanNodePtr TopNRowNumberNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
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

void LocalMergeNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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

PlanNodePtr LocalMergeNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.sources(std::move(newSources));
  return builder.build();
}

void TableWriteNode::addDetails(std::stringstream& stream) const {
  stream << insertTableHandle_->connectorInsertTableHandle()->toString();
}

folly::dynamic ColumnStatsSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["groupingKeys"] = ISerializable::serialize(groupingKeys);
  obj["aggregationStep"] = AggregationNode::toName(aggregationStep);
  obj["aggregateNames"] = ISerializable::serialize(aggregateNames);
  obj["aggregates"] = folly::dynamic::array;
  for (const auto& aggregate : aggregates) {
    obj["aggregates"].push_back(aggregate.serialize());
  }
  return obj;
}

// static
ColumnStatsSpec ColumnStatsSpec::create(
    const folly::dynamic& obj,
    void* context) {
  auto groupingKeys = deserializeFields(obj["groupingKeys"], context);
  const auto aggregationStep =
      AggregationNode::toStep(obj["aggregationStep"].asString());
  auto aggregateNames = ISerializable::deserialize<std::vector<std::string>>(
      obj["aggregateNames"]);

  std::vector<AggregationNode::Aggregate> aggregates;
  aggregates.reserve(obj["aggregates"].size());
  for (const auto& aggregate : obj["aggregates"]) {
    aggregates.push_back(
        AggregationNode::Aggregate::deserialize(aggregate, context));
  }

  return ColumnStatsSpec{
      std::move(groupingKeys),
      aggregationStep,
      std::move(aggregateNames),
      std::move(aggregates)};
}

folly::dynamic TableWriteNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["columns"] = columns_->serialize();
  obj["columnNames"] = ISerializable::serialize(columnNames_);
  if (columnStatsSpec_.has_value()) {
    obj["columnStatsSpec"] = columnStatsSpec_->serialize();
  }
  obj["connectorId"] = insertTableHandle_->connectorId();
  obj["connectorInsertTableHandle"] =
      insertTableHandle_->connectorInsertTableHandle()->serialize();
  obj["hasPartitioningScheme"] = hasPartitioningScheme_;
  obj["outputType"] = outputType_->serialize();
  obj["commitStrategy"] =
      std::string(connector::CommitStrategyName::toName(commitStrategy_));
  return obj;
}

void TableWriteNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr TableWriteNode::create(const folly::dynamic& obj, void* context) {
  auto id = obj["id"].asString();
  auto columns = deserializeRowType(obj["columns"]);
  auto columnNames =
      ISerializable::deserialize<std::vector<std::string>>(obj["columnNames"]);
  auto connectorId = obj["connectorId"].asString();
  auto connectorInsertTableHandle =
      ISerializable::deserialize<connector::ConnectorInsertTableHandle>(
          obj["connectorInsertTableHandle"]);
  const bool hasPartitioningScheme = obj["hasPartitioningScheme"].asBool();
  auto outputType = deserializeRowType(obj["outputType"]);
  auto commitStrategy = connector::CommitStrategyName::toCommitStrategy(
      obj["commitStrategy"].asString());
  std::optional<ColumnStatsSpec> columnStatsSpec;
  if (obj.count("columnStatsSpec") != 0) {
    columnStatsSpec = ColumnStatsSpec::create(obj["columnStatsSpec"], context);
  }
  return std::make_shared<TableWriteNode>(
      id,
      columns,
      columnNames,
      std::move(columnStatsSpec),
      std::make_shared<InsertTableHandle>(
          connectorId, connectorInsertTableHandle),
      hasPartitioningScheme,
      outputType,
      commitStrategy,
      deserializeSingleSource(obj, context));
}

PlanNodePtr TableWriteNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
}

void TableWriteMergeNode::addDetails(std::stringstream& /* stream */) const {}

folly::dynamic TableWriteMergeNode::serialize() const {
  auto obj = PlanNode::serialize();
  VELOX_CHECK_EQ(
      sources_.size(), 1, "TableWriteMergeNode can only have one source");
  if (columnStatsSpec_.has_value()) {
    obj["columnStatsSpec"] = columnStatsSpec_->serialize();
  }
  obj["outputType"] = outputType_->serialize();
  return obj;
}

void TableWriteMergeNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr TableWriteMergeNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto id = obj["id"].asString();
  auto outputType = deserializeRowType(obj["outputType"]);
  std::optional<ColumnStatsSpec> columnStatsSpec;
  if (obj.count("columnStatsSpec") != 0) {
    columnStatsSpec = ColumnStatsSpec::create(obj["columnStatsSpec"], context);
  }
  return std::make_shared<TableWriteMergeNode>(
      id,
      outputType,
      std::move(columnStatsSpec),
      deserializeSingleSource(obj, context));
}

PlanNodePtr TableWriteMergeNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
}

MergeExchangeNode::MergeExchangeNode(
    const PlanNodeId& id,
    const RowTypePtr& type,
    const std::vector<FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<SortOrder>& sortingOrders,
    VectorSerde::Kind serdeKind)
    : ExchangeNode(id, type, serdeKind),
      sortingKeys_(sortingKeys),
      sortingOrders_(sortingOrders) {}

void MergeExchangeNode::addDetails(std::stringstream& stream) const {
  addSortingKeys(sortingKeys_, sortingOrders_, stream);
  stream << ", ";
  addVectorSerdeKind(serdeKind(), stream);
}

folly::dynamic MergeExchangeNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["outputType"] = ExchangeNode::outputType()->serialize();
  obj["sortingKeys"] = ISerializable::serialize(sortingKeys_);
  obj["sortingOrders"] = serializeSortingOrders(sortingOrders_);
  obj["serdeKind"] = VectorSerde::kindName(serdeKind());
  return obj;
}

void MergeExchangeNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr MergeExchangeNode::create(
    const folly::dynamic& obj,
    void* context) {
  const auto outputType = deserializeRowType(obj["outputType"]);
  const auto sortingKeys = deserializeFields(obj["sortingKeys"], context);
  const auto sortingOrders = deserializeSortingOrders(obj["sortingOrders"]);
  const auto serdeKind = VectorSerde::kindByName(obj["serdeKind"].asString());
  return std::make_shared<MergeExchangeNode>(
      deserializePlanNodeId(obj),
      outputType,
      sortingKeys,
      sortingOrders,
      serdeKind);
}

PlanNodePtr MergeExchangeNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  //  VELOX_CHECK_EQ(newSources.size(), 1);

  // TODO: sortingKeys_ and outputType_ need to be constructed from newSources.
  Builder builder(*this);
  //  builder.source(newSources[0]);
  return builder.build();
}

void LocalPartitionNode::addDetails(std::stringstream& stream) const {
  stream << toName(type_);
  if (type_ != Type::kGather) {
    stream << " " << partitionFunctionSpec_->toString();
  }
  if (scaleWriter_) {
    stream << " scaleWriter";
  }
}

folly::dynamic LocalPartitionNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["type"] = toName(type_);
  obj["scaleWriter"] = scaleWriter_;
  obj["partitionFunctionSpec"] = partitionFunctionSpec_->serialize();
  return obj;
}

void LocalPartitionNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr LocalPartitionNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<LocalPartitionNode>(
      deserializePlanNodeId(obj),
      toType(obj["type"].asString()),
      obj["scaleWriter"].asBool(),
      ISerializable::deserialize<PartitionFunctionSpec>(
          obj["partitionFunctionSpec"], context),
      deserializeSources(obj, context));
}

PlanNodePtr LocalPartitionNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  //  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.sources(std::move(newSources));
  return builder.build();
}

namespace {
const auto& localPartitionTypeNames() {
  static const folly::F14FastMap<LocalPartitionNode::Type, std::string_view>
      kNames = {
          {LocalPartitionNode::Type::kGather, "GATHER"},
          {LocalPartitionNode::Type::kRepartition, "REPARTITION"},
      };
  return kNames;
}
} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(
    LocalPartitionNode,
    Type,
    localPartitionTypeNames)

PartitionedOutputNode::PartitionedOutputNode(
    const PlanNodeId& id,
    Kind kind,
    const std::vector<TypedExprPtr>& keys,
    int numPartitions,
    bool replicateNullsAndAny,
    PartitionFunctionSpecPtr partitionFunctionSpec,
    RowTypePtr outputType,
    VectorSerde::Kind serdeKind,
    PlanNodePtr source)
    : PlanNode(id),
      kind_(kind),
      sources_{{std::move(source)}},
      keys_(keys),
      numPartitions_(numPartitions),
      replicateNullsAndAny_(replicateNullsAndAny),
      partitionFunctionSpec_(std::move(partitionFunctionSpec)),
      serdeKind_(serdeKind),
      outputType_(std::move(outputType)) {
  VELOX_USER_CHECK_GT(numPartitions_, 0);
  if (numPartitions_ == 1) {
    VELOX_USER_CHECK(
        keys_.empty(),
        "Non-empty partitioning keys require more than one partition");
  } else {
    VELOX_USER_CHECK_NOT_NULL(
        partitionFunctionSpec_,
        "Partition function spec must be specified when the number of destinations is more than 1.");
  }
  if (!isPartitioned()) {
    VELOX_USER_CHECK(
        keys_.empty(),
        "{} partitioning doesn't allow for partitioning keys",
        toName(kind_));
  }
}

// static
std::shared_ptr<PartitionedOutputNode> PartitionedOutputNode::broadcast(
    const PlanNodeId& id,
    int numPartitions,
    RowTypePtr outputType,
    VectorSerde::Kind serdeKind,
    PlanNodePtr source) {
  std::vector<TypedExprPtr> noKeys;
  return std::make_shared<PartitionedOutputNode>(
      id,
      Kind::kBroadcast,
      noKeys,
      numPartitions,
      false,
      std::make_shared<GatherPartitionFunctionSpec>(),
      std::move(outputType),
      serdeKind,
      std::move(source));
}

// static
std::shared_ptr<PartitionedOutputNode> PartitionedOutputNode::arbitrary(
    const PlanNodeId& id,
    RowTypePtr outputType,
    VectorSerde::Kind serdeKind,
    PlanNodePtr source) {
  std::vector<TypedExprPtr> noKeys;
  return std::make_shared<PartitionedOutputNode>(
      id,
      Kind::kArbitrary,
      noKeys,
      1,
      false,
      std::make_shared<GatherPartitionFunctionSpec>(),
      std::move(outputType),
      serdeKind,
      std::move(source));
}

// static
std::shared_ptr<PartitionedOutputNode> PartitionedOutputNode::single(
    const PlanNodeId& id,
    RowTypePtr outputType,
    VectorSerde::Kind serdeKind,
    PlanNodePtr source) {
  std::vector<TypedExprPtr> noKeys;
  return std::make_shared<PartitionedOutputNode>(
      id,
      Kind::kPartitioned,
      noKeys,
      1,
      false,
      std::make_shared<GatherPartitionFunctionSpec>(),
      std::move(outputType),
      serdeKind,
      std::move(source));
}

void EnforceSingleRowNode::addDetails(std::stringstream& /* stream */) const {
  // Nothing to add.
}

folly::dynamic EnforceSingleRowNode::serialize() const {
  return PlanNode::serialize();
}

void EnforceSingleRowNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr EnforceSingleRowNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<EnforceSingleRowNode>(
      deserializePlanNodeId(obj), deserializeSingleSource(obj, context));
}

PlanNodePtr EnforceSingleRowNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
}

namespace {
const auto& partitionKindNames() {
  static const folly::F14FastMap<PartitionedOutputNode::Kind, std::string_view>
      kNames = {
          {PartitionedOutputNode::Kind::kPartitioned, "PARTITIONED"},
          {PartitionedOutputNode::Kind::kBroadcast, "BROADCAST"},
          {PartitionedOutputNode::Kind::kArbitrary, "ARBITRARY"},
      };
  return kNames;
}
} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(PartitionedOutputNode, Kind, partitionKindNames)

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

  stream << " ";
  addVectorSerdeKind(serdeKind_, stream);
}

folly::dynamic PartitionedOutputNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["kind"] = toName(kind_);
  obj["numPartitions"] = numPartitions_;
  obj["keys"] = ISerializable::serialize(keys_);
  obj["replicateNullsAndAny"] = replicateNullsAndAny_;
  obj["partitionFunctionSpec"] = partitionFunctionSpec_->serialize();
  obj["serdeKind"] = VectorSerde::kindName(serdeKind_);
  obj["outputType"] = outputType_->serialize();
  return obj;
}

void PartitionedOutputNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
PlanNodePtr PartitionedOutputNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<PartitionedOutputNode>(
      deserializePlanNodeId(obj),
      toKind(obj["kind"].asString()),
      ISerializable::deserialize<std::vector<ITypedExpr>>(obj["keys"], context),
      obj["numPartitions"].asInt(),
      obj["replicateNullsAndAny"].asBool(),
      ISerializable::deserialize<PartitionFunctionSpec>(
          obj["partitionFunctionSpec"], context),
      deserializeRowType(obj["outputType"]),
      VectorSerde::kindByName(obj["serdeKind"].asString()),
      deserializeSingleSource(obj, context));
}

PlanNodePtr PartitionedOutputNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
}

SpatialJoinNode::SpatialJoinNode(
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
      isSupported(joinType_),
      "The join type is not supported by spatial join: {}",
      JoinTypeName::toName(joinType_));
  VELOX_USER_CHECK(
      joinCondition_ != nullptr,
      "The join condition must not be null for spatial join");
  VELOX_USER_CHECK_EQ(
      sources_.size(), 2, "Must have 2 sources for spatial joins");
  VELOX_USER_CHECK(
      sources_[0] != nullptr, "Left source must not be null for spatial joins");
  VELOX_USER_CHECK(
      sources_[1] != nullptr,
      "Right source must not be null for spatial joins");

  checkJoinColumnNames(
      sources_[0]->outputType(),
      sources_[1]->outputType(),
      outputType_,
      outputType_->size());
}

bool SpatialJoinNode::isSupported(JoinType joinType) {
  switch (joinType) {
    case JoinType::kInner:
      [[fallthrough]];
    case JoinType::kLeft:
      return true;
    default:
      return false;
  }
}

void SpatialJoinNode::addDetails(std::stringstream& stream) const {
  stream << JoinTypeName::toName(joinType_);
  if (joinCondition_) {
    stream << ", joinCondition: " << joinCondition_->toString();
  }
}

folly::dynamic SpatialJoinNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["joinType"] = JoinTypeName::toName(joinType_);
  if (joinCondition_) {
    obj["joinCondition"] = joinCondition_->serialize();
  }
  obj["outputType"] = outputType_->serialize();
  return obj;
}

void SpatialJoinNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

PlanNodePtr SpatialJoinNode::create(const folly::dynamic& obj, void* context) {
  auto sources = deserializeSources(obj, context);
  VELOX_CHECK_EQ(2, sources.size());

  TypedExprPtr joinCondition;
  if (obj.count("joinCondition")) {
    joinCondition =
        ISerializable::deserialize<ITypedExpr>(obj["joinCondition"], context);
  }

  auto outputType = deserializeRowType(obj["outputType"]);

  return std::make_shared<SpatialJoinNode>(
      deserializePlanNodeId(obj),
      JoinTypeName::toJoinType(obj["joinType"].asString()),
      joinCondition,
      sources[0],
      sources[1],
      outputType);
}

PlanNodePtr SpatialJoinNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 2);

  Builder builder(*this);
  builder.left(std::move(newSources[0])).right(std::move(newSources[1]));
  return builder.build();
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

void TopNNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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

PlanNodePtr TopNNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
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

void LimitNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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

PlanNodePtr LimitNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
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

void OrderByNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
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

PlanNodePtr OrderByNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder(*this);
  builder.source(newSources[0]);
  return builder.build();
}

void MarkDistinctNode::addDetails(std::stringstream& stream) const {
  addFields(stream, distinctKeys_);
}

void PlanNode::toString(
    std::stringstream& stream,
    bool detailed,
    bool recursive,
    size_t indentationSize,
    const AddContextFunc& addContext) const {
  const std::string indentation(indentationSize, ' ');

  stream << indentation << "-- " << name() << "[" << id() << "]";

  if (detailed) {
    stream << "[";
    addDetails(stream);
    stream << "]";
    stream << " -> ";
    outputType()->printChildren(stream, ", ");
  }
  stream << std::endl;

  if (addContext) {
    const auto contextIndentation = indentation + "   ";
    addContext(id(), contextIndentation, stream);
  }

  if (recursive) {
    for (auto& source : sources()) {
      source->toString(stream, detailed, true, indentationSize + 2, addContext);
    }
  }

//  stream << serialize();
}

namespace {

std::string summarizeOutputType(
    const velox::RowTypePtr& type,
    const PlanSummaryOptions& options) {
  std::ostringstream out;
  out << type->size() << " fields";

  // Include names and types for the first few fields.
  const auto cnt = std::min<size_t>(options.maxOutputFields, type->size());
  if (cnt > 0) {
    out << ": ";
    for (auto i = 0; i < cnt; ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << type->nameOf(i) << " "
          << type->childAt(i)->toSummaryString(
                 {.maxChildren = (uint32_t)options.maxChildTypes});
    }

    if (cnt < type->size()) {
      out << ", ...";

      // TODO Include counts of fields by type kind.
    }
  }

  return out.str();
}

} // namespace

void PlanNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

void PlanNode::toSummaryString(
    const PlanSummaryOptions& options,
    std::stringstream& stream,
    size_t indentationSize,
    const AddContextFunc& addContext) const {
  const std::string indentation(indentationSize, ' ');

  const auto detailsIndentation = indentation + std::string(6, ' ');

  stream << indentation << "-- " << name() << "[" << id()
         << "]: " << summarizeOutputType(outputType(), options) << std::endl;
  addSummaryDetails(detailsIndentation, options, stream);

  if (addContext != nullptr) {
    addContext(id(), detailsIndentation, stream);
  }

  for (auto& source : sources()) {
    source->toSummaryString(options, stream, indentationSize + 2, addContext);
  }
}

void PlanNode::addSummaryDetails(
    const std::string& indentation,
    const PlanSummaryOptions& options,
    std::stringstream& stream) const {
  std::stringstream out;
  addDetails(out);

  stream << indentation << truncate(out.str(), options.maxLength) << std::endl;
}

void PlanNode::toSkeletonString(
    std::stringstream& stream,
    size_t indentationSize) const {
  // Skip Project nodes.
  if (const auto* project = dynamic_cast<const ProjectNode*>(this)) {
    project->sources().at(0)->toSkeletonString(stream, indentationSize);
    return;
  }

  const std::string indentation(indentationSize, ' ');

  stream << indentation << "-- " << name() << "[" << id()
         << "]: " << outputType()->size() << " fields" << std::endl;

  // Include table scan details.
  if (const auto* scan = dynamic_cast<const TableScanNode*>(this)) {
    stream << indentation << std::string(6, ' ')
           << scan->tableHandle()->toString() << std::endl;
  }

  for (const auto& source : sources()) {
    source->toSkeletonString(stream, indentationSize + 2);
  }
}

namespace {
void collectLeafPlanNodeIds(
    const PlanNode& planNode,
    std::unordered_set<PlanNodeId>& leafIds) {
  if (planNode.sources().empty()) {
    leafIds.insert(planNode.id());
    return;
  }

  const auto& sources = planNode.sources();
  const auto numSources = isIndexLookupJoin(&planNode) ? 1 : sources.size();
  for (int i = 0; i < numSources; ++i) {
    collectLeafPlanNodeIds(*sources[i], leafIds);
  }
}
} // namespace

std::unordered_set<PlanNodeId> PlanNode::leafPlanNodeIds() const {
  std::unordered_set<PlanNodeId> leafIds;
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
  registry.Register("IndexLookupJoinNode", IndexLookupJoinNode::create);
  registry.Register("NestedLoopJoinNode", NestedLoopJoinNode::create);
  registry.Register("LimitNode", LimitNode::create);
  registry.Register("LocalMergeNode", LocalMergeNode::create);
  registry.Register("LocalPartitionNode", LocalPartitionNode::create);
  registry.Register("OrderByNode", OrderByNode::create);
  registry.Register("PartitionedOutputNode", PartitionedOutputNode::create);
  registry.Register("ProjectNode", ProjectNode::create);
  registry.Register("ParallelProjectNode", ParallelProjectNode::create);
  registry.Register("RowNumberNode", RowNumberNode::create);
  registry.Register("SpatialJoinNode", SpatialJoinNode::create);
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

PlanNodePtr PlanNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_UNSUPPORTED("copyWithNewSources is not implemented for {}", name());
}
//
// void PlanNode::updateNewTypes(
//    const std::map<std::string, std::pair<std::string, TypePtr>>&
//        newOutputTypes) {
//  RowTypePtr originalOutputType = outputType();
//  std::map<std::string, std::pair<std::string, TypePtr>> updatedOutputTypes;
//  for (auto i = 0; i < originalOutputType->size(); i++) {
//    auto originalName = originalOutputType->nameOf(i);
//    auto newOutTypeIter = newOutputTypes.find(originalName);
//    if (newOutTypeIter != newOutputTypes.end()) {
//      // If the field was updated by upstream PlanNodes before, it must be a
//      // FieldAccessTypedExpr that directly references the same field name in
//      // this current ProjectionNode. Otherwise it would have a different
//      symbol
//      // name. In this case we only need to update the output type of this
//      // node, because FieldAccessTypedExpr doesn't specify the input type.
//      auto newOutputType = newOutTypeIter->second.second;
//      auto newName = newOutTypeIter->second.first;
//      updateOutputNameAndType(i, newName, newOutputType);
//    }
//  }
//  originalOutputType->ensureNameToIndex();
//}

const std::vector<PlanNodePtr>& TraceScanNode::sources() const {
  return kEmptySources;
}

void TraceScanNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

std::string TraceScanNode::traceDir() const {
  return traceDir_;
}

void TraceScanNode::addDetails(std::stringstream& stream) const {
  stream << "Trace dir: " << traceDir_;
}

void FilterNode::addSummaryDetails(
    const std::string& indentation,
    const PlanSummaryOptions& options,
    std::stringstream& stream) const {
  SummarizeExprVisitor::Context exprCtx;
  SummarizeExprVisitor visitor;
  filter_->accept(visitor, exprCtx);

  appendExprSummary(indentation, options, exprCtx, stream);

  stream << indentation
         << "filter: " << truncate(filter_->toString(), options.maxLength)
         << std::endl;
}

folly::dynamic FilterNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["filter"] = filter_->serialize();
  return obj;
}

void FilterNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

void AggregationNode::addSummaryDetails(
    const std::string& indentation,
    const PlanSummaryOptions& options,
    std::stringstream& stream) const {
  std::stringstream out;
  SummarizeExprVisitor::Context exprCtx;
  SummarizeExprVisitor visitor;
  for (const auto& aggregate : aggregates()) {
    aggregate.call->accept(visitor, exprCtx);
  }

  appendExprSummary(indentation, options, exprCtx, stream);

  stream << indentation << "aggregations: " << aggregates().size() << std::endl;

  {
    const auto cnt =
        std::min(options.aggregate.maxAggregations, aggregates().size());
    appendAggregations(
        indentation + "   ", *this, cnt, stream, options.maxLength);
  }
}

// static
PlanNodePtr FilterNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);

  auto filter = ISerializable::deserialize<ITypedExpr>(obj["filter"], context);
  return std::make_shared<FilterNode>(
      deserializePlanNodeId(obj), filter, std::move(source));
}

PlanNodePtr FilterNode::copyWithNewSources(
    std::vector<PlanNodePtr> newSources) const {
  VELOX_CHECK_EQ(newSources.size(), 1);
  Builder builder;
  builder.id(id()).filter(filter_).source(newSources[0]);
  return builder.build();
}

folly::dynamic IndexLookupCondition::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["key"] = key->serialize();
  return obj;
}

bool InIndexLookupCondition::isFilter() const {
  return list->isConstantKind();
}

folly::dynamic InIndexLookupCondition::serialize() const {
  folly::dynamic obj = IndexLookupCondition::serialize();
  obj["type"] = "in";
  obj["in"] = list->serialize();
  return obj;
}

std::string InIndexLookupCondition::toString() const {
  return fmt::format("{} IN {}", key->toString(), list->toString());
}

void InIndexLookupCondition::validate() const {
  VELOX_CHECK_NOT_NULL(key);
  VELOX_CHECK_NOT_NULL(list);
  VELOX_CHECK(
      list->isFieldAccessKind() || list->isConstantKind(),
      "Invalid condition list {}",
      list->toString());
  const auto& listType = list->type()->asArray();
  VELOX_CHECK_EQ(
      key->type()->kind(),
      listType.elementType()->kind(),
      "In condition key and list condition element must have the same type");
}

IndexLookupConditionPtr InIndexLookupCondition::create(
    const folly::dynamic& obj,
    void* context) {
  TypedExprPtr list =
      ISerializable::deserialize<FieldAccessTypedExpr>(obj["in"], context);
  if (list == nullptr) {
    list = ISerializable::deserialize<ConstantTypedExpr>(obj["in"], context);
  }
  return std::make_shared<InIndexLookupCondition>(
      ISerializable::deserialize<FieldAccessTypedExpr>(obj["key"], context),
      std::move(list));
}

bool BetweenIndexLookupCondition::isFilter() const {
  return lower->isConstantKind() && upper->isConstantKind();
}

folly::dynamic BetweenIndexLookupCondition::serialize() const {
  folly::dynamic obj = IndexLookupCondition::serialize();
  obj["type"] = "between";
  obj["lower"] = lower->serialize();
  obj["upper"] = upper->serialize();
  return obj;
}

std::string BetweenIndexLookupCondition::toString() const {
  return fmt::format(
      "{} BETWEEN {} AND {}",
      key->toString(),
      lower->toString(),
      upper->toString());
}

IndexLookupConditionPtr BetweenIndexLookupCondition::create(
    const folly::dynamic& obj,
    void* context) {
  auto key =
      ISerializable::deserialize<FieldAccessTypedExpr>(obj["key"], context);
  return std::make_shared<BetweenIndexLookupCondition>(
      key,
      ISerializable::deserialize<ITypedExpr>(obj["lower"], context),
      ISerializable::deserialize<ITypedExpr>(obj["upper"], context));
}

void BetweenIndexLookupCondition::validate() const {
  VELOX_CHECK_NOT_NULL(key);
  VELOX_CHECK_NOT_NULL(lower);
  VELOX_CHECK_NOT_NULL(upper);
  VELOX_CHECK(
      lower->isFieldAccessKind() || lower->isConstantKind(),
      "Invalid lower between condition {}",
      lower->toString());

  VELOX_CHECK(
      upper->isFieldAccessKind() || upper->isConstantKind(),
      "Invalid upper between condition {}",
      upper->toString());

  VELOX_CHECK_EQ(
      key->type()->kind(),
      lower->type()->kind(),
      "Index key and lower condition must have the same type");

  VELOX_CHECK_EQ(
      key->type()->kind(),
      upper->type()->kind(),
      "Index key and upper condition must have the same type");
}

bool EqualIndexLookupCondition::isFilter() const {
  return value->isConstantKind();
}

folly::dynamic EqualIndexLookupCondition::serialize() const {
  folly::dynamic obj = IndexLookupCondition::serialize();
  obj["type"] = "equal";
  obj["value"] = value->serialize();
  return obj;
}

std::string EqualIndexLookupCondition::toString() const {
  return fmt::format("{} = {}", key->toString(), value->toString());
}

IndexLookupConditionPtr EqualIndexLookupCondition::create(
    const folly::dynamic& obj,
    void* context) {
  auto key =
      ISerializable::deserialize<FieldAccessTypedExpr>(obj["key"], context);
  return std::make_shared<EqualIndexLookupCondition>(
      key, ISerializable::deserialize<ITypedExpr>(obj["value"], context));
}

void EqualIndexLookupCondition::validate() const {
  VELOX_CHECK_NOT_NULL(key);
  VELOX_CHECK_NOT_NULL(value);
  VELOX_CHECK_NOT_NULL(
      checked_pointer_cast<const ConstantTypedExpr>(value),
      "Equal condition value must be a constant expression: {}",
      value->toString());

  VELOX_CHECK_EQ(
      key->type()->kind(),
      value->type()->kind(),
      "Equal condition key and value must have compatible types: {} vs {}",
      key->type()->toString(),
      value->type()->toString());
}
} // namespace facebook::velox::core
