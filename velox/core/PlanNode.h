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
#pragma once

#include "velox/connectors/Connector.h"
#include "velox/core/Expressions.h"

namespace facebook::velox::core {

typedef std::string PlanNodeId;

/**
 * Generic representation of InsertTable
 */
struct InsertTableHandle {
 public:
  InsertTableHandle(
      const std::string& connectorId,
      const std::shared_ptr<connector::ConnectorInsertTableHandle>&
          connectorInsertTableHandle)
      : connectorId_(connectorId),
        connectorInsertTableHandle_(connectorInsertTableHandle) {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const std::shared_ptr<connector::ConnectorInsertTableHandle>&
  connectorInsertTableHandle() const {
    return connectorInsertTableHandle_;
  }

 private:
  // Connector ID
  const std::string connectorId_;

  // Write request to a DataSink of that connector type
  const std::shared_ptr<connector::ConnectorInsertTableHandle>
      connectorInsertTableHandle_;
};

class SortOrder {
 public:
  SortOrder(bool ascending, bool nullsFirst)
      : ascending_(ascending), nullsFirst_(nullsFirst) {}

  bool isAscending() const {
    return ascending_;
  }

  bool isNullsFirst() const {
    return nullsFirst_;
  }

 private:
  const bool ascending_;
  const bool nullsFirst_;
};

class PlanNode {
 public:
  explicit PlanNode(const PlanNodeId& id) : id_{id} {}

  virtual ~PlanNode() {}

  const PlanNodeId& id() const {
    return id_;
  }

  virtual const RowTypePtr& outputType() const = 0;

  virtual const std::vector<std::shared_ptr<const PlanNode>>& sources()
      const = 0;

  std::string toString(bool detailed = false, bool recursive = false) const {
    std::stringstream stream;
    toString(stream, detailed, recursive, 0);
    return stream.str();
  }

  /// The name of the plan node, used in toString.
  virtual std::string_view name() const = 0;

 private:
  /// The details of the plan node in textual format.
  virtual void addDetails(std::stringstream& stream) const {}

  // Format when detailed and recursive are enabled is:
  //  -> name[details]
  //      -> child1Name [details]
  //         ...
  //      -> child2Name [details]
  //         ...
  void toString(
      std::stringstream& stream,
      bool detailed = false,
      bool recursive = false,
      size_t indentation = 0) const {
    auto addIndentation = [&]() {
      auto counter = indentation;
      while (counter) {
        stream << " ";
        counter--;
      }
    };

    addIndentation();
    stream << "->" << name();
    if (detailed) {
      stream << "[";
      addDetails(stream);
      stream << "]";
    }
    stream << "\n";
    if (recursive) {
      for (auto& source : sources()) {
        source->toString(stream, detailed, true, indentation + 2);
      }
    }
  }

  const std::string id_;
};

class ValuesNode : public PlanNode {
 public:
  ValuesNode(
      const PlanNodeId& id,
      std::vector<RowVectorPtr>&& values,
      bool parallelizable = false)
      : PlanNode(id),
        values_(std::move(values)),
        outputType_(
            std::dynamic_pointer_cast<const RowType>(values_[0]->type())),
        parallelizable_(parallelizable) {
    VELOX_CHECK(!values_.empty());
  }

  ValuesNode(
      const PlanNodeId& id,
      const std::vector<RowVectorPtr>& values,
      bool parallelizable = false)
      : PlanNode(id),
        values_(values),
        outputType_(
            std::dynamic_pointer_cast<const RowType>(values_[0]->type())),
        parallelizable_(parallelizable) {
    VELOX_CHECK(!values_.empty());
  }

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override;

  const std::vector<RowVectorPtr>& values() const {
    return values_;
  }

  // for testing only
  bool isParallelizable() const {
    return parallelizable_;
  }

  std::string_view name() const override {
    return "values";
  }

 private:
  const std::vector<RowVectorPtr> values_;
  const RowTypePtr outputType_;
  const bool parallelizable_;
};

class FilterNode : public PlanNode {
 public:
  FilterNode(
      const PlanNodeId& id,
      std::shared_ptr<const ITypedExpr> filter,
      std::shared_ptr<const PlanNode> source)
      : PlanNode(id), sources_{source}, filter_(filter) {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  const std::shared_ptr<const ITypedExpr>& filter() const {
    return filter_;
  }

  std::string_view name() const override {
    return "filter";
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "expression: " << filter_->toString();
  }

  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  const std::shared_ptr<const ITypedExpr> filter_;
};

class ProjectNode : public PlanNode {
 public:
  ProjectNode(
      const PlanNodeId& id,
      std::vector<std::string>&& names,
      std::vector<std::shared_ptr<const ITypedExpr>>&& projections,
      std::shared_ptr<const PlanNode> source)
      : PlanNode(id),
        sources_{source},
        names_(std::move(names)),
        projections_(std::move(projections)),
        outputType_(makeOutputType(names_, projections_)) {}

  ProjectNode(
      const PlanNodeId& id,
      const std::vector<std::string>& names,
      const std::vector<std::shared_ptr<const ITypedExpr>>& projections,
      std::shared_ptr<const PlanNode> source)
      : PlanNode(id),
        sources_{source},
        names_(names),
        projections_(projections),
        outputType_(makeOutputType(names_, projections_)) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  const std::vector<std::string>& names() const {
    return names_;
  }

  const std::vector<std::shared_ptr<const ITypedExpr>>& projections() const {
    return projections_;
  }

  std::string_view name() const override {
    return "project";
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "expressions: ";
    for (auto i = 0; i < projections_.size(); i++) {
      auto& projection = projections_[i];
      stream << "(" << names_[i] << ":" << projection->type()->toString()
             << ", " << projection->toString() << "), ";
    }
  }

  static RowTypePtr makeOutputType(
      const std::vector<std::string>& names,
      const std::vector<std::shared_ptr<const ITypedExpr>>& projections) {
    std::vector<std::shared_ptr<const Type>> types;
    for (auto& projection : projections) {
      types.push_back(projection->type());
    }

    auto namesCopy = names;
    return std::make_shared<RowType>(std::move(namesCopy), std::move(types));
  }

  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  const std::vector<std::string> names_;
  const std::vector<std::shared_ptr<const ITypedExpr>> projections_;
  const RowTypePtr outputType_;
};

class TableScanNode : public PlanNode {
 public:
  TableScanNode(
      const PlanNodeId& id,
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& assignments)
      : PlanNode(id),
        outputType_(outputType),
        tableHandle_(tableHandle),
        assignments_(assignments) {}

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override;

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle() const {
    return tableHandle_;
  }

  const std::
      unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>&
      assignments() const {
    return assignments_;
  }

  std::string_view name() const override {
    return "table scan";
  }

 private:
  const RowTypePtr outputType_;
  const std::shared_ptr<connector::ConnectorTableHandle> tableHandle_;
  const std::
      unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
          assignments_;
};

class TableWriteNode : public PlanNode {
 public:
  TableWriteNode(
      const PlanNodeId& id,
      const RowTypePtr& columns,
      const std::vector<std::string>& columnNames,
      const std::shared_ptr<InsertTableHandle>& insertTableHandle,
      const RowTypePtr& outputType,
      const std::shared_ptr<const PlanNode>& source)
      : PlanNode(id),
        sources_{source},
        columns_{columns},
        columnNames_{columnNames},
        insertTableHandle_(insertTableHandle),
        outputType_(outputType) {
    VELOX_CHECK_EQ(columns->size(), columnNames.size());
    for (const auto& column : columns->names()) {
      VELOX_CHECK(source->outputType()->containsChild(column));
    }
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  // The subset of columns in the output of the source node, potentially in
  // different order, to write to the table.
  const RowTypePtr& columns() const {
    return columns_;
  }

  // Column names to use when writing the table. This vector is aligned with
  // 'columns' vector.
  const std::vector<std::string>& columnNames() const {
    return columnNames_;
  }

  const std::shared_ptr<InsertTableHandle>& insertTableHandle() const {
    return insertTableHandle_;
  }

  std::string_view name() const override {
    return "table write";
  }

 private:
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  const RowTypePtr columns_;
  const std::vector<std::string> columnNames_;
  const std::shared_ptr<InsertTableHandle> insertTableHandle_;
  const RowTypePtr outputType_;
};

class AggregationNode : public PlanNode {
 public:
  enum class Step {
    // raw input in - partial result out
    kPartial,
    // partial result in - final result out
    kFinal,
    // partial result in - partial result out
    kIntermediate,
    // raw input in - final result out
    kSingle
  };

  /**
   * @param ignoreNullKeys True if rows with at least one null key should be
   * ignored. Used when group by is a source of a join build side and grouping
   * keys are join keys.
   */
  AggregationNode(
      const PlanNodeId& id,
      Step step,
      const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
          groupingKeys,
      const std::vector<std::string>& aggregateNames,
      const std::vector<std::shared_ptr<const CallTypedExpr>>& aggregates,
      const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& aggrMasks,
      bool ignoreNullKeys,
      std::shared_ptr<const PlanNode> source);

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  Step step() const {
    return step_;
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& groupingKeys()
      const {
    return groupingKeys_;
  }

  const std::vector<std::string>& aggregateNames() const {
    return aggregateNames_;
  }

  const std::vector<std::shared_ptr<const CallTypedExpr>>& aggregates() const {
    return aggregates_;
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& aggrMasks()
      const {
    return aggrMasks_;
  }

  bool ignoreNullKeys() const {
    return ignoreNullKeys_;
  }

  std::string_view name() const override {
    return "aggregation";
  }

 private:
  static std::shared_ptr<RowType> getOutputType(
      const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
          groupingKeys,
      const std::vector<std::string>& aggregateNames,
      const std::vector<std::shared_ptr<const CallTypedExpr>>& aggregates) {
    VELOX_CHECK_EQ(
        aggregateNames.size(),
        aggregates.size(),
        "Number of aggregate names must be equal to number of aggregates");

    std::vector<std::string> names;
    std::vector<std::shared_ptr<const Type>> types;

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

  const Step step_;
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>> groupingKeys_;
  const std::vector<std::string> aggregateNames_;
  const std::vector<std::shared_ptr<const CallTypedExpr>> aggregates_;
  // Keeps mask/'no mask' for every aggregation. Mask, if given, is a reference
  // to a boolean projection column, used to mask out rows for the aggregation.
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>> aggrMasks_;
  const bool ignoreNullKeys_;
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  const RowTypePtr outputType_;
};

inline std::ostream& operator<<(
    std::ostream& out,
    const AggregationNode::Step& step) {
  switch (step) {
    case AggregationNode::Step::kFinal:
      return out << "FINAL";
    case AggregationNode::Step::kIntermediate:
      return out << "INTERMEDIATE";
    case AggregationNode::Step::kPartial:
      return out << "PARTIAL";
    case AggregationNode::Step::kSingle:
      return out << "SINGLE";
  }
  VELOX_UNREACHABLE();
}

inline std::string mapAggregationStepToName(const AggregationNode::Step& step) {
  std::stringstream ss;
  ss << step;
  return ss.str();
}

class ExchangeNode : public PlanNode {
 public:
  ExchangeNode(const PlanNodeId& id, RowTypePtr type)
      : PlanNode(id), outputType_(type) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override;

  std::string_view name() const override {
    return "exchange";
  }

 private:
  RowTypePtr outputType_;
};

class MergeExchangeNode : public ExchangeNode {
 public:
  explicit MergeExchangeNode(
      const PlanNodeId& id,
      const RowTypePtr& type,
      const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
          sortingKeys,
      const std::vector<SortOrder>& sortingOrders)
      : ExchangeNode(id, type),
        sortingKeys_(sortingKeys),
        sortingOrders_(sortingOrders) {}

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& sortingKeys()
      const {
    return sortingKeys_;
  }

  const std::vector<SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  std::string_view name() const override {
    return "merge exchange";
  }

 private:
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys_;
  const std::vector<SortOrder> sortingOrders_;
};

class LocalMergeNode : public PlanNode {
 public:
  LocalMergeNode(
      const PlanNodeId& id,
      std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys,
      std::vector<SortOrder> sortingOrders,
      std::shared_ptr<const PlanNode> source)
      : PlanNode(id),
        sources_{{std::move(source)}},
        sortingKeys_{std::move(sortingKeys)},
        sortingOrders_{std::move(sortingOrders)} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& sortingKeys()
      const {
    return sortingKeys_;
  }

  const std::vector<SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  std::string_view name() const override {
    return "local merge";
  }

 private:
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys_;
  const std::vector<SortOrder> sortingOrders_;
};

/// Hash partitions data using specified keys. The number of partitions is
/// determined by the parallelism of the upstream pipeline. Can be used to
/// gather data from multiple sources. The order of columns in the output may be
/// different from input.
class LocalPartitionNode : public PlanNode {
 public:
  LocalPartitionNode(
      const PlanNodeId& id,
      std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> keys,
      RowTypePtr outputType,
      std::vector<std::shared_ptr<const PlanNode>> sources)
      : PlanNode(id),
        sources_{std::move(sources)},
        keys_{std::move(keys)},
        outputType_{std::move(outputType)} {
    VELOX_CHECK_GT(
        sources_.size(),
        0,
        "Local repartitioning node requires at least one source");
  }

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  const RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& keys() const {
    return keys_;
  }

  std::string_view name() const override {
    return "local repartitioning";
  }

 private:
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>> keys_;
  const RowTypePtr outputType_;
};

class PartitionedOutputNode : public PlanNode {
 public:
  explicit PartitionedOutputNode(
      const PlanNodeId& id,
      const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
          keys,
      int numPartitions,
      bool broadcast,
      bool replicateNullsAndAny,
      const RowTypePtr outputType,
      std::shared_ptr<const PlanNode> source)
      : PlanNode(id),
        sources_{{std::move(source)}},
        keys_(keys),
        numPartitions_(numPartitions),
        broadcast_(broadcast),
        replicateNullsAndAny_(replicateNullsAndAny),
        outputType_(outputType) {
    VELOX_CHECK(numPartitions > 0, "numPartitions must be greater than zero");
    if (numPartitions == 1) {
      VELOX_CHECK(
          keys_.empty(),
          "Non-empty partitioning keys require more than one partition");
    }
    if (broadcast) {
      VELOX_CHECK(
          keys_.empty(),
          "Broadcast partitioning doesn't allow for partitioning keys");
    }
  }

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  const RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& keys() const {
    return keys_;
  }

  int numPartitions() const {
    return numPartitions_;
  }

  bool isBroadcast() const {
    return broadcast_;
  }

  /// Returns true if an arbitrary row and all rows with null keys must be
  /// replicated to all destinations. This is used to ensure correct results for
  /// anti-join which requires all nodes to know whether combined build side is
  /// empty and whether it has any entry with null join key.
  bool isReplicateNullsAndAny() const {
    return replicateNullsAndAny_;
  }

  std::string_view name() const override {
    return "repartitioning";
  }

 private:
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>> keys_;
  const int numPartitions_;
  const bool broadcast_;
  const bool replicateNullsAndAny_;
  const RowTypePtr outputType_;
};

enum class JoinType { kInner, kLeft, kRight, kFull, kSemi, kAnti };

// Represents inner/outer/semi/antijoin hash
// joins. Translates to an exec::HashBuild and exec::HashProbe. A
// separate pipeline is produced for the build side when generating
// exec::Operators.
class HashJoinNode : public PlanNode {
 public:
  HashJoinNode(
      const PlanNodeId& id,
      JoinType joinType,
      const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& leftKeys,
      const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& rightKeys,
      std::shared_ptr<const ITypedExpr> filter,
      std::shared_ptr<const PlanNode> left,
      std::shared_ptr<const PlanNode> right,
      const RowTypePtr outputType)
      : PlanNode(id),
        joinType_(joinType),
        leftKeys_(leftKeys),
        rightKeys_(rightKeys),
        filter_(std::move(filter)),
        sources_({std::move(left), std::move(right)}),
        outputType_(outputType) {
    VELOX_CHECK(!leftKeys_.empty());
    VELOX_CHECK_EQ(leftKeys_.size(), rightKeys_.size());
    auto leftType = sources_[0]->outputType();
    for (auto key : leftKeys_) {
      VELOX_CHECK(leftType->containsChild(key->name()));
    }
    auto rightType = sources_[1]->outputType();
    for (auto key : rightKeys_) {
      VELOX_CHECK(rightType->containsChild(key->name()));
    }
    for (auto i = 0; i < outputType_->size(); ++i) {
      auto name = outputType_->nameOf(i);
      VELOX_CHECK(
          leftType->containsChild(name) || rightType->containsChild(name));
    }
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  JoinType joinType() const {
    return joinType_;
  }

  bool isInnerJoin() const {
    return joinType_ == JoinType::kInner;
  }

  bool isLeftJoin() const {
    return joinType_ == JoinType::kLeft;
  }

  bool isRightJoin() const {
    return joinType_ == JoinType::kRight;
  }

  bool isFullJoin() const {
    return joinType_ == JoinType::kFull;
  }

  bool isSemiJoin() const {
    return joinType_ == JoinType::kSemi;
  }

  bool isAntiJoin() const {
    return joinType_ == JoinType::kAnti;
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& leftKeys()
      const {
    return leftKeys_;
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& rightKeys()
      const {
    return rightKeys_;
  }

  const std::shared_ptr<const ITypedExpr>& filter() const {
    return filter_;
  }

  std::string_view name() const override {
    return "hash join";
  }

 private:
  const JoinType joinType_;
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>> leftKeys_;
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>> rightKeys_;
  // Optional join filter, nullptr if absent. This is applied to
  // join hits and if this is false, the hit turns into a miss, which
  // has a special meaning for outer joins. For inner joins, this is
  // equivalent to a Filter above the join.
  const std::shared_ptr<const ITypedExpr> filter_;
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  const RowTypePtr outputType_;
};

// Represents the 'SortBy' node in the plan.
class OrderByNode : public PlanNode {
 public:
  OrderByNode(
      const PlanNodeId& id,
      const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
          sortingKeys,
      const std::vector<SortOrder>& sortingOrders,
      bool isPartial,
      const std::shared_ptr<const PlanNode>& source)
      : PlanNode(id),
        sortingKeys_(sortingKeys),
        sortingOrders_(sortingOrders),
        isPartial_(isPartial),
        sources_{source} {
    VELOX_CHECK(!sortingKeys.empty(), "OrderBy must specify sorting keys");
    VELOX_CHECK_EQ(
        sortingKeys.size(),
        sortingOrders.size(),
        "Number of sorting keys and sorting orders in OrderBy must be the same");
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& sortingKeys()
      const {
    return sortingKeys_;
  }

  const std::vector<SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  // True if this node only sorts a portion of the final result. If it is
  // true, a local merge or merge exchange is required to merge the sorted
  // runs.
  bool isPartial() const {
    return isPartial_;
  }

  std::string_view name() const override {
    return "orderby";
  }

 private:
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys_;
  const std::vector<SortOrder> sortingOrders_;
  const bool isPartial_;
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
};

class TopNNode : public PlanNode {
 public:
  TopNNode(
      const PlanNodeId& id,
      const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
          sortingKeys,
      const std::vector<SortOrder>& sortingOrders,
      int32_t count,
      bool isPartial,
      const std::shared_ptr<const PlanNode>& source)
      : PlanNode(id),
        sortingKeys_(sortingKeys),
        sortingOrders_(sortingOrders),
        count_(count),
        isPartial_(isPartial),
        sources_{source} {
    VELOX_CHECK(!sortingKeys.empty(), "TopN must specify sorting keys");
    VELOX_CHECK(
        sortingKeys.size() == sortingOrders.size(),
        "Number of sorting keys and sorting orders in TopN must be the same");
    VELOX_CHECK(
        count > 0,
        "TopN must specify greater than zero number of rows to keep");
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>& sortingKeys()
      const {
    return sortingKeys_;
  }

  const std::vector<SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  int32_t count() const {
    return count_;
  }

  bool isPartial() const {
    return isPartial_;
  }

  std::string_view name() const override {
    return "topN";
  }

 private:
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys_;
  const std::vector<SortOrder> sortingOrders_;
  const int32_t count_;
  const bool isPartial_;
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
};

class LimitNode : public PlanNode {
 public:
  // @param isPartial Boolean indicating whether Limit node generates partial
  // results on local workers or finalizes the partial results from `PARTIAL`
  // nodes.
  LimitNode(
      const PlanNodeId& id,
      int32_t count,
      bool isPartial,
      const std::shared_ptr<const PlanNode>& source)
      : PlanNode(id), count_(count), isPartial_(isPartial), sources_{source} {
    VELOX_CHECK(
        count > 0,
        "Limit must specify greater than zero number of rows to keep");
  }

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  int32_t count() const {
    return count_;
  }

  bool isPartial() const {
    return isPartial_;
  }

  std::string_view name() const override {
    return "limit";
  }

 private:
  const int32_t count_;
  const bool isPartial_;
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
};

/// Expands arrays and maps into separate columns. Arrays are expanded into a
/// single column, and maps are expanded into two columns (key, value). Can be
/// used to expand multiple columns. In this case will produce as many rows as
/// the highest cardinality array or map (the other columns are padded with
/// nulls). Optionally can produce an ordinality column that specifies the row
/// number starting with 1.
class UnnestNode : public PlanNode {
 public:
  /// @param replicateVariables Inputs that are projected as is
  /// @param unnestVariables Inputs that are unnested. Must be of type ARRAY or
  /// MAP.
  /// @param unnestNames Names to use for unnested outputs: one name for each
  /// array (element); two names for each map (key and value). The output names
  /// must appear in the same order as unnestVariables.
  /// @param ordinalityName Optional name for the ordinality columns. If not
  /// present, ordinality column is not produced.
  UnnestNode(
      const PlanNodeId& id,
      std::vector<std::shared_ptr<const FieldAccessTypedExpr>>
          replicateVariables,
      std::vector<std::shared_ptr<const FieldAccessTypedExpr>> unnestVariables,
      const std::vector<std::string>& unnestNames,
      const std::optional<std::string>& ordinalityName,
      const std::shared_ptr<const PlanNode>& source);

  /// The order of columns in the output is: replicated columns (in the order
  /// specified), unnested columns (in the order specified, for maps: key comes
  /// before value), optional ordinality column.
  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
  replicateVariables() const {
    return replicateVariables_;
  }

  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>&
  unnestVariables() const {
    return unnestVariables_;
  }

  bool withOrdinality() const {
    return withOrdinality_;
  }

  std::string_view name() const override {
    return "unnest";
  }

 private:
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>
      replicateVariables_;
  const std::vector<std::shared_ptr<const FieldAccessTypedExpr>>
      unnestVariables_;
  const bool withOrdinality_;
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  RowTypePtr outputType_;
};

} // namespace facebook::velox::core
