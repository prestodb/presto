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
#include "velox/core/PlanNode.h"
#include "velox/common/encode/Base64.h"
#include "velox/vector/VectorSaver.h"

namespace facebook::velox::core {

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
    const std::vector<CallTypedExprPtr>& aggregates) {
  VELOX_CHECK_EQ(
      aggregateNames.size(),
      aggregates.size(),
      "Number of aggregate names must be equal to number of aggregates");

  std::vector<std::string> names;
  std::vector<TypePtr> types;

  for (auto& key : groupingKeys) {
    auto field =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(key);
    VELOX_CHECK(field, "Grouping key must be a field reference");
    names.push_back(field->name());
    types.push_back(field->type());
  }

  for (int32_t i = 0; i < aggregateNames.size(); i++) {
    names.push_back(aggregateNames[i]);
    types.push_back(aggregates[i]->type());
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
    const std::vector<CallTypedExprPtr>& aggregates,
    const std::vector<FieldAccessTypedExprPtr>& aggregateMasks,
    bool ignoreNullKeys,
    PlanNodePtr source)
    : PlanNode(id),
      step_(step),
      groupingKeys_(groupingKeys),
      preGroupedKeys_(preGroupedKeys),
      aggregateNames_(aggregateNames),
      aggregates_(aggregates),
      aggregateMasks_(aggregateMasks),
      ignoreNullKeys_(ignoreNullKeys),
      sources_{source},
      outputType_(getAggregationOutputType(
          groupingKeys_,
          aggregateNames_,
          aggregates_)) {
  // Empty grouping keys are used in global aggregation:
  //    SELECT sum(c) FROM t
  // Empty aggregates are used in distinct:
  //    SELECT distinct(b, c) FROM t GROUP BY a
  VELOX_CHECK(
      !groupingKeys_.empty() || !aggregates_.empty(),
      "Aggregation must specify either grouping keys or aggregates");

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
}

namespace {
void addFields(
    std::stringstream& stream,
    const std::vector<FieldAccessTypedExprPtr>& keys) {
  for (auto i = 0; i < keys.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << keys[i]->name();
  }
}

void addKeys(std::stringstream& stream, const std::vector<TypedExprPtr>& keys) {
  for (auto i = 0; i < keys.size(); ++i) {
    const auto& expr = keys[i];
    if (i > 0) {
      stream << ", ";
    }
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
      stream << field->name();
    } else if (
        auto constant =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
      stream << constant->toString();
    } else {
      stream << expr->toString();
    }
  }
}
} // namespace

void AggregationNode::addDetails(std::stringstream& stream) const {
  stream << stepName(step_) << " ";

  if (!groupingKeys_.empty()) {
    stream << "[";
    addFields(stream, groupingKeys_);
    stream << "] ";
  }

  for (auto i = 0; i < aggregateNames_.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << aggregateNames_[i] << " := " << aggregates_[i]->toString();
    if (aggregateMasks_.size() > i && aggregateMasks_[i]) {
      stream << " mask: " << aggregateMasks_[i]->name();
    }
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
  obj["aggregates"] = ISerializable::serialize(aggregates_);

  obj["masks"] = folly::dynamic::array;
  for (const auto& mask : aggregateMasks_) {
    if (mask) {
      obj["masks"].push_back(mask->serialize());
    } else {
      obj["masks"].push_back(nullptr);
    }
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
} // namespace

// static
PlanNodePtr AggregationNode::create(const folly::dynamic& obj, void* context) {
  auto source = deserializeSingleSource(obj, context);

  auto groupingKeys = deserializeFields(obj["groupingKeys"], context);
  auto preGroupedKeys = deserializeFields(obj["preGroupedKeys"], context);
  auto aggregateNames = deserializeStrings(obj["aggregateNames"]);
  auto aggregates = ISerializable::deserialize<std::vector<CallTypedExpr>>(
      obj["aggregates"], context);

  std::vector<FieldAccessTypedExprPtr> masks;
  for (const auto& mask : obj["masks"]) {
    if (mask.isNull()) {
      masks.push_back(nullptr);
    } else {
      masks.push_back(
          ISerializable::deserialize<FieldAccessTypedExpr>(mask, context));
    }
  }

  return std::make_shared<AggregationNode>(
      deserializePlanNodeId(obj),
      stepFromName(obj["step"].asString()),
      groupingKeys,
      preGroupedKeys,
      aggregateNames,
      aggregates,
      masks,
      obj["ignoreNullKeys"].asBool(),
      deserializeSingleSource(obj, context));
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
    std::vector<std::vector<FieldAccessTypedExprPtr>> groupingSets,
    std::vector<GroupIdNode::GroupingKeyInfo> groupingKeyInfos,
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
  VELOX_CHECK_GE(
      groupingSets_.size(),
      2,
      "GroupIdNode requires two or more grouping sets.");
}

void GroupIdNode::addDetails(std::stringstream& stream) const {
  for (auto i = 0; i < groupingSets_.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << "[";
    addFields(stream, groupingSets_[i]);
    stream << "]";
  }
}

folly::dynamic GroupIdNode::serialize() const {
  VELOX_NYI();
}

// static
PlanNodePtr GroupIdNode::create(
    const folly::dynamic& /* obj */,
    void* /* context */) {
  VELOX_NYI();
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
    if (i > 0) {
      stream << ", ";
    }
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
  VELOX_NYI();
}

// static
PlanNodePtr TableScanNode::create(
    const folly::dynamic& /* obj */,
    void* /* context */) {
  VELOX_NYI();
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
  VELOX_NYI();
}

// static
PlanNodePtr ExchangeNode::create(
    const folly::dynamic& /* obj */,
    void* /* context */) {
  VELOX_NYI();
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
    const RowTypePtr outputType)
    : PlanNode(id),
      joinType_(joinType),
      leftKeys_(leftKeys),
      rightKeys_(rightKeys),
      filter_(std::move(filter)),
      sources_({std::move(left), std::move(right)}),
      outputType_(outputType) {
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
    const auto& name = outputType->nameOf(numOutputColumms);
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

  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);

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

  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);

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

CrossJoinNode::CrossJoinNode(
    const PlanNodeId& id,
    PlanNodePtr left,
    PlanNodePtr right,
    RowTypePtr outputType)
    : PlanNode(id),
      sources_({std::move(left), std::move(right)}),
      outputType_(std::move(outputType)) {}

void CrossJoinNode::addDetails(std::stringstream& /* stream */) const {
  // Nothing to add.
}

folly::dynamic CrossJoinNode::serialize() const {
  VELOX_NYI();
}

// static
PlanNodePtr CrossJoinNode::create(
    const folly::dynamic& /* obj */,
    void* /* context */) {
  VELOX_NYI();
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
    PlanNodePtr source)
    : PlanNode(std::move(id)),
      partitionKeys_(std::move(partitionKeys)),
      sortingKeys_(std::move(sortingKeys)),
      sortingOrders_(std::move(sortingOrders)),
      windowFunctions_(std::move(windowFunctions)),
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
  for (const auto& order : array) {
    sortingOrders.push_back(SortOrder::deserialize(order));
  }
  return sortingOrders;
}
} // namespace

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

  return std::make_shared<WindowNode>(
      deserializePlanNodeId(obj),
      partitionKeys,
      sortingKeys,
      sortingOrders,
      windowNames,
      functions,
      source);
}

namespace {
void addSortingKeys(
    std::stringstream& stream,
    const std::vector<FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<SortOrder>& sortingOrders) {
  for (auto i = 0; i < sortingKeys.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << sortingKeys[i]->name() << " " << sortingOrders[i].toString();
  }
}
} // namespace

void LocalMergeNode::addDetails(std::stringstream& stream) const {
  addSortingKeys(stream, sortingKeys_, sortingOrders_);
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

void TableWriteNode::addDetails(std::stringstream& /* stream */) const {
  // TODO Add connector details.
}

folly::dynamic TableWriteNode::serialize() const {
  VELOX_NYI();
}

// static
PlanNodePtr TableWriteNode::create(
    const folly::dynamic& /* obj */,
    void* /* context */) {
  VELOX_NYI();
}

void MergeExchangeNode::addDetails(std::stringstream& stream) const {
  addSortingKeys(stream, sortingKeys_, sortingOrders_);
}

folly::dynamic MergeExchangeNode::serialize() const {
  VELOX_NYI();
}

// static
PlanNodePtr MergeExchangeNode::create(
    const folly::dynamic& /* obj */,
    void* /* context */) {
  VELOX_NYI();
}

void LocalPartitionNode::addDetails(std::stringstream& stream) const {
  stream << typeName(type_);
}

folly::dynamic LocalPartitionNode::serialize() const {
  VELOX_NYI();
}

// static
PlanNodePtr LocalPartitionNode::create(
    const folly::dynamic& /* obj */,
    void* /* context */) {
  VELOX_NYI();
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

void PartitionedOutputNode::addDetails(std::stringstream& stream) const {
  if (broadcast_) {
    stream << "BROADCAST";
  } else if (numPartitions_ == 1) {
    stream << "SINGLE";
  } else {
    stream << "HASH(";
    addKeys(stream, keys_);
    stream << ") " << numPartitions_;
  }

  if (replicateNullsAndAny_) {
    stream << " replicate nulls and any";
  }
}

folly::dynamic PartitionedOutputNode::serialize() const {
  VELOX_NYI();
}

// static
PlanNodePtr PartitionedOutputNode::create(
    const folly::dynamic& /* obj */,
    void* /* context */) {
  VELOX_NYI();
}

void TopNNode::addDetails(std::stringstream& stream) const {
  if (isPartial_) {
    stream << "PARTIAL ";
  }
  stream << count_ << " ";

  addSortingKeys(stream, sortingKeys_, sortingOrders_);
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
  addSortingKeys(stream, sortingKeys_, sortingOrders_);
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

void WindowNode::addDetails(std::stringstream& stream) const {
  stream << "partition by [";
  if (!partitionKeys_.empty()) {
    addFields(stream, partitionKeys_);
  }
  stream << "] ";

  stream << "order by [";
  addSortingKeys(stream, sortingKeys_, sortingOrders_);
  stream << "] ";

  auto numInputCols = sources_[0]->outputType()->size();
  auto numOutputCols = outputType_->size();
  for (auto i = numInputCols; i < numOutputCols; i++) {
    if (i >= numInputCols + 1) {
      stream << ", ";
    }
    stream << outputType_->names()[i] << " := ";
    addWindowFunction(stream, windowFunctions_[i - numInputCols]);
  }
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
  registry.Register("CrossJoinNode", CrossJoinNode::create);
  registry.Register("EnforceSingleRowNode", EnforceSingleRowNode::create);
  registry.Register("ExchangeNode", ExchangeNode::create);
  registry.Register("FilterNode", FilterNode::create);
  registry.Register("GroupIdNode", GroupIdNode::create);
  registry.Register("HashJoinNode", HashJoinNode::create);
  registry.Register("MergeExchangeNode", MergeExchangeNode::create);
  registry.Register("MergeJoinNode", MergeJoinNode::create);
  registry.Register("LimitNode", LimitNode::create);
  registry.Register("LocalMergeNode", LocalMergeNode::create);
  registry.Register("LocalPartitionNode", LocalPartitionNode::create);
  registry.Register("OrderByNode", OrderByNode::create);
  registry.Register("PartitionedOutputNode", PartitionedOutputNode::create);
  registry.Register("ProjectNode", ProjectNode::create);
  registry.Register("TableScanNode", TableScanNode::create);
  registry.Register("TableWriteNode", TableWriteNode::create);
  registry.Register("TopNNode", TopNNode::create);
  registry.Register("UnnestNode", UnnestNode::create);
  registry.Register("ValuesNode", ValuesNode::create);
  registry.Register("WindowNode", WindowNode::create);
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
