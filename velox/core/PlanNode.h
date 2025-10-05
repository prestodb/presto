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

#include <fmt/format.h>

#include <utility>

#include "velox/common/Enums.h"
#include "velox/connectors/Connector.h"
#include "velox/core/Expressions.h"
#include "velox/core/QueryConfig.h"
#include "velox/vector/VectorStream.h"

struct ArrowArrayStream;

namespace facebook::velox::core {

class PlanNodeVisitor;
class PlanNodeVisitorContext;

using PlanNodeId = std::string;

/// Generic representation of InsertTable
struct InsertTableHandle {
 public:
  InsertTableHandle(
      const std::string& connectorId,
      const connector::ConnectorInsertTableHandlePtr&
          connectorInsertTableHandle)
      : connectorId_(connectorId),
        connectorInsertTableHandle_(connectorInsertTableHandle) {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const connector::ConnectorInsertTableHandlePtr& connectorInsertTableHandle()
      const {
    return connectorInsertTableHandle_;
  }

 private:
  // Connector ID
  const std::string connectorId_;

  // Write request to a DataSink of that connector type
  const connector::ConnectorInsertTableHandlePtr connectorInsertTableHandle_;
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

  bool operator==(const SortOrder& other) const = default;

  std::string toString() const {
    return fmt::format(
        "{} NULLS {}",
        (ascending_ ? "ASC" : "DESC"),
        (nullsFirst_ ? "FIRST" : "LAST"));
  }

  folly::dynamic serialize() const;

  static SortOrder deserialize(const folly::dynamic& obj);

 private:
  bool ascending_;
  bool nullsFirst_;
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    const SortOrder& order) {
  os << order.toString();
  return os;
}

extern const SortOrder kAscNullsFirst;
extern const SortOrder kAscNullsLast;
extern const SortOrder kDescNullsFirst;
extern const SortOrder kDescNullsLast;

struct PlanSummaryOptions {
  /// Options that apply specifically to PROJECT nodes.
  struct ProjectOptions {
    /// For a given PROJECT node, maximum number of non-identity and
    /// non-constant projection expressions to include in the summary. By
    /// default, no expression is included.
    size_t maxProjections = 0;

    /// For a given PROJECT node, maximum number of dereference (access of a
    /// struct field) expressions to include in the summary. By default, no
    /// expression is included.
    size_t maxDereferences = 0;

    /// For a given PROJECT node, maximum number of constant expressions to
    /// include in the summary. By default, no expression is included.
    size_t maxConstants = 0;
  };

  ProjectOptions project = {};

  /// For a given node, maximum number of output fields to include in the
  /// summary. Each field has a name and a type. The amount of type information
  /// is controlled by 'maxChildTypes' option. Use 0 to include only the number
  /// of output fields.
  size_t maxOutputFields = 5;

  /// For a given output type, maximum number of child types to include in the
  /// summary. By default, only top-level type is included: BIGINT, ARRAY, MAP,
  /// ROW. Set to 2 to include types of array elements, map keys and values as
  /// well as up to 2 fields of a struct: ARRAY(REAL), MAP(INTEGER, ARRAY),
  /// ROW(VARCHAR, ARRAY,...).
  size_t maxChildTypes = 0;

  /// Controls the maximum length of a string that is included in the plan
  /// summary.
  size_t maxLength = 50;

  /// Options that apply specifically to AGGREGATION nodes.
  struct AggregateOptions {
    /// For a given AGGREGATION node, maximum number of aggregate expressions
    /// to include in the summary. By default, no aggregate expression is
    /// included.
    size_t maxAggregations = 0;
  };

  AggregateOptions aggregate = {};
};

class PlanNode;
using PlanNodePtr = std::shared_ptr<const PlanNode>;

class PlanNode : public ISerializable {
 public:
  explicit PlanNode(PlanNodeId id) : id_{std::move(id)} {}

  virtual ~PlanNode() {}

  const PlanNodeId& id() const {
    return id_;
  }

  folly::dynamic serialize() const override;

  static void registerSerDe();

  virtual const RowTypePtr& outputType() const = 0;

//  void updateOutputNameAndType(int i, std::string& newName, TypePtr newType)
//      const {
//    auto rowType = std::const_pointer_cast<RowType>(outputType());
//    rowType->updateChildAt(i, newName, newType);
//  }
//
//  // TODO: We are still marking const for the join key upcast elimination
//  // optimization, but the PlanNode need to provide mutable interface if we
//  // allow similar optimizations happen in other places.
//  virtual void updateNewTypes(
//      const std::map<std::string, std::pair<std::string, TypePtr>>& inputTypes);

  virtual const std::vector<std::shared_ptr<const PlanNode>>& sources()
      const = 0;

  virtual PlanNodePtr copyWithNewSources(
      std::vector<std::shared_ptr<const PlanNode>> newSources) const;

  /// Accepts a visitor to visit this plan node.
  /// Implementations of this class should implement it as
  ///   visitor.visit(*this, context);
  /// This has to be done in the descendant class in order to call the right
  /// overload of visit.
  /// We provide a default implementation in PlanNode so that custom extensions
  /// can either choose to implement it themselves or fall into the general
  /// bucket of PlanNodes which they will end up in anyway for PlanNodeVisitors
  /// that do not explicitly implement support for that PlanNode extension.
  virtual void accept(
      const PlanNodeVisitor& visitor,
      PlanNodeVisitorContext& context) const;

  /// Returns true if this is a leaf plan node and corresponding operator
  /// requires an ExchangeClient to retrieve data. For instance, TableScanNode
  /// is a leaf node that doesn't require an ExchangeClient. But ExchangeNode is
  /// a leaf node that requires an ExchangeClient.
  virtual bool requiresExchangeClient() const {
    return false;
  }

  /// Returns true if this is a leaf plan node and corresponding operator
  /// requires splits to make progress. ValueNode is a leaf node that doesn't
  /// require splits, but TableScanNode and ExchangeNode are leaf nodes that
  /// require splits.
  virtual bool requiresSplits() const {
    return false;
  }

  /// Returns true if this plan node operator is spillable and 'queryConfig' has
  /// enabled it.
  virtual bool canSpill(const QueryConfig& queryConfig) const {
    return false;
  }

  /// Returns true if this plan node operator supports task barrier processing.
  /// To support barrier processing, the operator must be able to drain its
  /// buffered output when it receives the drain signal at split boundary. Not
  /// all plan nodes support barrier processing. For example, Hash Join doesn't.
  virtual bool supportsBarrier() const {
    return false;
  }

  /// Returns a set of leaf plan node IDs.
  std::unordered_set<core::PlanNodeId> leafPlanNodeIds() const;

  /// Lambda to add context for a given plan node. Receives plan node ID,
  /// indentation and std::ostream where to append the context. Start each line
  /// of context with 'indentation' and end with a new-line character.
  using AddContextFunc = std::function<void(
      const PlanNodeId& planNodeId,
      const std::string& indentation,
      std::ostream& stream)>;

  /// Returns human-friendly representation of the plan. By default, returns the
  /// plan node name. Includes plan node details such as join keys and aggregate
  /// function names if 'detailed' is true. Returns the whole sub-tree if
  /// 'recursive' is true. Includes additional context for each plan node if
  /// 'addContext' is not null.
  ///
  /// @param addContext Optional lambda to add context for a given plan node.
  std::string toString(
      bool detailed = false,
      bool recursive = false,
      const AddContextFunc& addContext = nullptr) const {
    std::stringstream stream;
    toString(stream, detailed, recursive, 0, addContext);
    return stream.str();
  }

  /// @param addContext Optional lambda to add context for a given plan node.
  std::string toSummaryString(
      PlanSummaryOptions options = {},
      const AddContextFunc& addContext = nullptr) const {
    std::stringstream stream;
    toSummaryString(options, stream, 0, addContext);
    return stream.str();
  }

  std::string toSkeletonString() const {
    std::stringstream stream;
    toSkeletonString(stream, 0);
    return stream.str();
  }

  /// The name of the plan node, used in toString.
  virtual std::string_view name() const = 0;

  /// Recursively checks the node tree for a first node that satisfy a given
  /// condition. Returns pointer to the node if found, nullptr if not.
  static const PlanNode* findFirstNode(
      const PlanNode* node,
      const std::function<bool(const PlanNode* node)>& predicate) {
    if (predicate(node)) {
      return node;
    }

    // Recursively go further through the sources.
    for (const auto& source : node->sources()) {
      const auto* ret = PlanNode::findFirstNode(source.get(), predicate);
      if (ret != nullptr) {
        return ret;
      }
    }
    return nullptr;
  }

 private:
  // The details of the plan node in textual format.
  virtual void addDetails(std::stringstream& stream) const = 0;

  // Format when detailed and recursive are enabled is:
  //  -> name[details]
  //      -> child1Name [details]
  //         ...
  //      -> child2Name [details]
  //         ...
  void toString(
      std::stringstream& stream,
      bool detailed,
      bool recursive,
      size_t indentationSize,
      const AddContextFunc& addContext) const;

  // The default implementation calls 'addDetails' and truncates the result.
  virtual void addSummaryDetails(
      const std::string& indentation,
      const PlanSummaryOptions& options,
      std::stringstream& stream) const;

  void toSummaryString(
      const PlanSummaryOptions& options,
      std::stringstream& stream,
      size_t indentationSize,
      const AddContextFunc& addContext) const;

  // Even shorter summary of the plan. Hides all Project nodes. Shows only
  // number of output columns, but no names or types. Doesn't show any details
  // of the nodes, except for table scan.
  void toSkeletonString(std::stringstream& stream, size_t indentationSize)
      const;

  const PlanNodeId id_;
};

// using PlanNodePtr = std::shared_ptr<const PlanNode>;

class ValuesNode : public PlanNode {
 public:
  ValuesNode(
      const PlanNodeId& id,
      std::vector<RowVectorPtr> values,
      bool parallelizable = kDefaultParallelizable,
      size_t repeatTimes = kDefaultRepeatTimes)
      : PlanNode(id),
        values_(std::move(values)),
        outputType_(values_.empty() ? ROW({}) : values_[0]->rowType()),
        parallelizable_(parallelizable),
        repeatTimes_(repeatTimes) {}

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const ValuesNode& other) {
      id_ = other.id();
      values_ = other.values();
      parallelizable_ = other.testingIsParallelizable();
      repeatTimes_ = other.repeatTimes();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& values(std::vector<RowVectorPtr> values) {
      values_ = std::move(values);
      return *this;
    }

    Builder& testingParallelizable(const bool parallelizable) {
      parallelizable_ = parallelizable;
      return *this;
    }

    Builder& repeatTimes(const size_t repeatTimes) {
      repeatTimes_ = repeatTimes;
      return *this;
    }

    std::shared_ptr<ValuesNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "ValuesNode id is not set");
      VELOX_USER_CHECK(values_.has_value(), "ValuesNode values is not set");

      return std::make_shared<ValuesNode>(
          id_.value(), values_.value(), parallelizable_, repeatTimes_);
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::vector<RowVectorPtr>> values_;
    bool parallelizable_ = kDefaultParallelizable;
    size_t repeatTimes_ = kDefaultRepeatTimes;
  };

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<PlanNodePtr>& sources() const override;

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const std::vector<RowVectorPtr>& values() const {
    return values_;
  }

  // For testing only.
  bool testingIsParallelizable() const {
    return parallelizable_;
  }

  // Controls how many times each input buffer will be produced as input.
  // For example, if `values_` contains 3 rowVectors {v1, v2, v3}
  // and repeatTimes = 2, the following input will be produced:
  //   v1, v2, v3, v1, v2, v3
  size_t repeatTimes() const {
    return repeatTimes_;
  }

  std::string_view name() const override {
    return "Values";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  static constexpr bool kDefaultParallelizable = false;
  static constexpr size_t kDefaultRepeatTimes = 1;

  void addDetails(std::stringstream& stream) const override;

  const std::vector<RowVectorPtr> values_;
  const RowTypePtr outputType_;
  const bool parallelizable_;
  const size_t repeatTimes_;
};

using ValuesNodePtr = std::shared_ptr<const ValuesNode>;

class ArrowStreamNode : public PlanNode {
 public:
  ArrowStreamNode(
      const PlanNodeId& id,
      RowTypePtr outputType,
      std::shared_ptr<ArrowArrayStream> arrowStream)
      : PlanNode(id),
        outputType_(std::move(outputType)),
        arrowStream_(std::move(arrowStream)) {
    VELOX_USER_CHECK_NOT_NULL(arrowStream_);
  }

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const ArrowStreamNode& other) {
      id_ = other.id();
      outputType_ = other.outputType();
      arrowStream_ = other.arrowStream();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    Builder& arrowStream(std::shared_ptr<ArrowArrayStream> arrowStream) {
      arrowStream_ = std::move(arrowStream);
      return *this;
    }

    std::shared_ptr<ArrowStreamNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "ArrowStreamNode id is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "ArrowStreamNode outputType is not set");
      VELOX_USER_CHECK(
          arrowStream_.has_value(), "ArrowStreamNode arrowStream is not set");

      return std::make_shared<ArrowStreamNode>(
          id_.value(), outputType_.value(), arrowStream_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<RowTypePtr> outputType_;
    std::optional<std::shared_ptr<ArrowArrayStream>> arrowStream_;
  };

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<PlanNodePtr>& sources() const override;

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const std::shared_ptr<ArrowArrayStream>& arrowStream() const {
    return arrowStream_;
  }

  std::string_view name() const override {
    return "ArrowStream";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("ArrowStream plan node is not serializable");
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const RowTypePtr outputType_;
  std::shared_ptr<ArrowArrayStream> arrowStream_;
};

using ArrowStreamNodePtr = std::shared_ptr<const ArrowStreamNode>;

class TraceScanNode final : public PlanNode {
 public:
  TraceScanNode(
      const PlanNodeId& id,
      const std::string& traceDir,
      uint32_t pipelineId,
      std::vector<uint32_t> driverIds,
      const RowTypePtr& outputType)
      : PlanNode(id),
        traceDir_(traceDir),
        pipelineId_(pipelineId),
        driverIds_(std::move(driverIds)),
        outputType_(outputType) {}

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const TraceScanNode& other) {
      id_ = other.id();
      traceDir_ = other.traceDir();
      pipelineId_ = other.pipelineId();
      driverIds_ = other.driverIds();
      outputType_ = other.outputType();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& traceDir(const std::string& traceDir) {
      traceDir_ = traceDir;
      return *this;
    }

    Builder& pipelineId(const uint32_t pipelineId) {
      pipelineId_ = pipelineId;
      return *this;
    }

    Builder& driverIds(std::vector<uint32_t> driverIds) {
      driverIds_ = std::move(driverIds);
      return *this;
    }

    Builder& outputType(const RowTypePtr& outputType) {
      outputType_ = outputType;
      return *this;
    }

    std::shared_ptr<TraceScanNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "TraceScanNode id is not set");
      VELOX_USER_CHECK(
          traceDir_.has_value(), "TraceScanNode traceDir is not set");
      VELOX_USER_CHECK(
          pipelineId_.has_value(), "TraceScanNode pipelineId is not set");
      VELOX_USER_CHECK(
          driverIds_.has_value(), "TraceScanNode driverIds is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "TraceScanNode outputType is not set");

      return std::make_shared<TraceScanNode>(
          id_.value(),
          traceDir_.value(),
          pipelineId_.value(),
          driverIds_.value(),
          outputType_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::string> traceDir_;
    std::optional<uint32_t> pipelineId_;
    std::optional<std::vector<uint32_t>> driverIds_;
    std::optional<RowTypePtr> outputType_;
  };

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<PlanNodePtr>& sources() const override;

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  std::string_view name() const override {
    return "QueryReplayScan";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("TraceScanNode is not serializable");
    return nullptr;
  }

  std::string traceDir() const;

  uint32_t pipelineId() const {
    return pipelineId_;
  }

  std::vector<uint32_t> driverIds() const {
    return driverIds_;
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  // Directory of traced data, which is $traceRoot/$taskId/$nodeId.
  const std::string traceDir_;
  const uint32_t pipelineId_;
  const std::vector<uint32_t> driverIds_;
  const RowTypePtr outputType_;
};

using TraceScanNodePtr = std::shared_ptr<const TraceScanNode>;

class FilterNode : public PlanNode {
 public:
  FilterNode(const PlanNodeId& id, TypedExprPtr filter, PlanNodePtr source)
      : PlanNode(id), sources_{std::move(source)}, filter_(std::move(filter)) {
    VELOX_USER_CHECK(
        filter_->type()->isBoolean(),
        "Filter expression must be of type BOOLEAN. Got {}.",
        filter_->type()->toString());
  }

  bool supportsBarrier() const override {
    return true;
  }

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const FilterNode& other) {
      id_ = other.id();
      filter_ = other.filter();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& filter(TypedExprPtr filter) {
      filter_ = std::move(filter);
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<FilterNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "FilterNode id is not set");
      VELOX_USER_CHECK(filter_.has_value(), "FilterNode filter is not set");
      VELOX_USER_CHECK(source_.has_value(), "FilterNode source is not set");

      return std::make_shared<FilterNode>(
          id_.value(), filter_.value(), source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<TypedExprPtr> filter_;
    std::optional<PlanNodePtr> source_;
  };

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const TypedExprPtr& filter() const {
    return filter_;
  }

  std::string_view name() const override {
    return "Filter";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "expression: " << filter_->toString();
  }

  void addSummaryDetails(
      const std::string& indentation,
      const PlanSummaryOptions& options,
      std::stringstream& stream) const override;

  const std::vector<PlanNodePtr> sources_;
  const TypedExprPtr filter_;
};

using FilterNodePtr = std::shared_ptr<const FilterNode>;

class AbstractProjectNode : public PlanNode {
 public:
  AbstractProjectNode(
      const PlanNodeId& id,
      std::vector<std::string>&& names,
      std::vector<TypedExprPtr>&& projections,
      PlanNodePtr source)
      : PlanNode(id),
        sources_{source},
        names_(std::move(names)),
        projections_(std::move(projections)),
        outputType_(makeOutputType(names_, projections_)) {}

  AbstractProjectNode(
      const PlanNodeId& id,
      const std::vector<std::string>& names,
      const std::vector<TypedExprPtr>& projections,
      PlanNodePtr source)
      : PlanNode(id),
        sources_{source},
        names_(names),
        projections_(projections),
        outputType_(makeOutputType(names_, projections_)) {}

  template <typename DerivedPlanNode, typename DerivedBuilder>
  class Builder {
   public:
    Builder() = default;

    explicit Builder(const DerivedPlanNode& other) {
      id_ = other.id();
      names_ = other.names();
      projections_ = other.projections();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    virtual ~Builder() = default;

    DerivedBuilder& id(PlanNodeId id) {
      id_ = std::move(id);
      return static_cast<DerivedBuilder&>(*this);
    }

    DerivedBuilder& names(std::vector<std::string> names) {
      names_ = std::move(names);
      return static_cast<DerivedBuilder&>(*this);
    }

    DerivedBuilder& projections(std::vector<TypedExprPtr> projections) {
      projections_ = std::move(projections);
      return static_cast<DerivedBuilder&>(*this);
    }

    DerivedBuilder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return static_cast<DerivedBuilder&>(*this);
    }

   protected:
    std::optional<PlanNodeId> id_;
    std::optional<std::vector<std::string>> names_;
    std::optional<std::vector<TypedExprPtr>> projections_;
    std::optional<PlanNodePtr> source_;
  };

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

//  void updateNewTypes(
//      const std::map<std::string, std::pair<std::string, TypePtr>>& newTypes)
//      override;

//  PlanNodePtr copyWithNewSources(
//      std::vector<PlanNodePtr> newSources) const override;

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  const std::vector<std::string>& names() const {
    return names_;
  }

  const std::vector<TypedExprPtr>& projections() const {
    return projections_;
  }
//
//  void setProjection(int i, TypedExprPtr expr) {
//    projections_[i] = expr;
//  }
//
//  void setName(int i, const std::string& projectionName) {
//    names_[i] = projectionName;
//  }

  // This function is virtual to allow customized projections to inherit from
  // this class without re-implementing the other functions.
  virtual std::string_view name() const override {
    return "Project";
  }

 protected:
  void addDetails(std::stringstream& stream) const override;

  /// Append a summary of the plan node to 'stream'. Make sure to append full
  /// lines that start with 'identation' and end with std::endl. It is append
  /// one or multiple lines or not append anything. Make sure to truncate any
  /// output that can be arbitrary long. The default implementation appends
  /// truncated output of 'addDetails'.
  void addSummaryDetails(
      const std::string& indentation,
      const PlanSummaryOptions& options,
      std::stringstream& stream) const override;

  static RowTypePtr makeOutputType(
      const std::vector<std::string>& names,
      const std::vector<TypedExprPtr>& projections);

  const std::vector<PlanNodePtr> sources_;
  const std::vector<std::string> names_;
  const std::vector<TypedExprPtr> projections_;
  const RowTypePtr outputType_;
};

class ProjectNode : public AbstractProjectNode {
 public:
  ProjectNode(
      const PlanNodeId& id,
      std::vector<std::string>&& names,
      std::vector<TypedExprPtr>&& projections,
      PlanNodePtr source)
      : AbstractProjectNode(
            id,
            std::move(names),
            std::move(projections),
            source) {}

  ProjectNode(
      const PlanNodeId& id,
      const std::vector<std::string>& names,
      const std::vector<TypedExprPtr>& projections,
      PlanNodePtr source)
      : AbstractProjectNode(id, names, projections, source) {}

  bool supportsBarrier() const override {
    return true;
  }

  class Builder : public AbstractProjectNode::Builder<ProjectNode, Builder> {
   public:
    Builder() : AbstractProjectNode::Builder<ProjectNode, Builder>() {}

    explicit Builder(const ProjectNode& other)
        : AbstractProjectNode::Builder<ProjectNode, Builder>(other) {}

    std::shared_ptr<ProjectNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "ProjectNode id is not set");
      VELOX_USER_CHECK(names_.has_value(), "ProjectNode names is not set");
      VELOX_USER_CHECK(
          projections_.has_value(), "ProjectNode projections is not set");
      VELOX_USER_CHECK(source_.has_value(), "ProjectNode source is not set");

      return std::make_shared<ProjectNode>(
          id_.value(), names_.value(), projections_.value(), source_.value());
    }
  };

  folly::dynamic serialize() const override;

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;
};

using ProjectNodePtr = std::shared_ptr<const ProjectNode>;

/// Variant of ProjectNode that computes projections in
/// parallel. The exprs are given in groups, so that all exprs in
/// one group run together and all groups run in parallel. If lazies
/// are loaded, each lazy must be loaded by exactly one group. If
/// there are identity projections in the groups, possible lazies
/// are loaded as part of processing the group. One can additionally
/// specify 'noLoadIdentities' which are identity projected through
/// without loading. This last set must be disjoint from all columns
/// accessed by the exprs. The output type has 'names' first and
/// then 'noLoadIdentities'. The ith name corresponds to the ith
/// expr when exprs is flattened. Inherits core::ProjectNode in order to reuse
/// the summary functions.
class ParallelProjectNode : public core::AbstractProjectNode {
 public:
  ParallelProjectNode(
      const core::PlanNodeId& id,
      std::vector<std::string> names,
      std::vector<std::vector<core::TypedExprPtr>> exprGroups,
      std::vector<std::string> noLoadIdentities,
      core::PlanNodePtr input);

  class Builder
      : public AbstractProjectNode::Builder<ParallelProjectNode, Builder> {
   public:
    Builder() : AbstractProjectNode::Builder<ParallelProjectNode, Builder>() {}

    explicit Builder(const ParallelProjectNode& other)
        : AbstractProjectNode::Builder<ParallelProjectNode, Builder>(other) {}

    Builder& exprNames(std::vector<std::string> exprNames) {
      exprNames_ = std::move(exprNames);
      return static_cast<Builder&>(*this);
    }

    Builder& exprGroups(
        std::vector<std::vector<core::TypedExprPtr>> exprGroups) {
      exprGroups_ = std::move(exprGroups);
      return static_cast<Builder&>(*this);
    }

    Builder& noLoadIdentities(std::vector<std::string> noLoadIdentities) {
      noLoadIdentities_ = std::move(noLoadIdentities);
      return static_cast<Builder&>(*this);
    }

    std::shared_ptr<ProjectNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "ProjectNode id is not set");
      VELOX_USER_CHECK(names_.has_value(), "ProjectNode names is not set");
      VELOX_USER_CHECK(
          projections_.has_value(), "ProjectNode projections is not set");
      VELOX_USER_CHECK(source_.has_value(), "ProjectNode source is not set");

      return std::make_shared<ProjectNode>(
          id_.value(), names_.value(), projections_.value(), source_.value());
    }

   private:
    std::vector<std::string> exprNames_;
    std::vector<std::vector<core::TypedExprPtr>> exprGroups_;
    std::vector<std::string> noLoadIdentities_;
  };

  std::string_view name() const override {
    return "ParallelProject";
  }

  const std::vector<std::string>& exprNames() const {
    return exprNames_;
  }

  const std::vector<std::vector<core::TypedExprPtr>>& exprGroups() const {
    return exprGroups_;
  }

  const std::vector<std::string> noLoadIdentities() const {
    return noLoadIdentities_;
  }

  folly::dynamic serialize() const override;

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<std::string> exprNames_;
  const std::vector<std::vector<core::TypedExprPtr>> exprGroups_;
  const std::vector<std::string> noLoadIdentities_;
};

/// Variant of project node that contains only field accesses and dereferences,
/// and does not materialize the input columns.  Used to split subfields of
/// struct columns for later parallel processing.
class LazyDereferenceNode : public core::ProjectNode {
 public:
  LazyDereferenceNode(
      const PlanNodeId& id,
      std::vector<std::string> names,
      std::vector<TypedExprPtr> projections,
      PlanNodePtr source)
      : ProjectNode(
            id,
            std::move(names),
            std::move(projections),
            std::move(source)) {}

  std::string_view name() const override {
    return "LazyDereference";
  }

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;
};

using ParallelProjectNodePtr = std::shared_ptr<const ParallelProjectNode>;

class TableScanNode : public PlanNode {
 public:
  TableScanNode(
      const PlanNodeId& id,
      RowTypePtr outputType,
      const connector::ConnectorTableHandlePtr& tableHandle,
      const connector::ColumnHandleMap& assignments)
      : PlanNode(id),
        outputType_(std::move(outputType)),
        tableHandle_(tableHandle),
        assignments_(assignments) {}

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const TableScanNode& other) {
      id_ = other.id();
      outputType_ = other.outputType();
      tableHandle_ = other.tableHandle();
      assignments_ = other.assignments();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    Builder& tableHandle(connector::ConnectorTableHandlePtr tableHandle) {
      tableHandle_ = std::move(tableHandle);
      return *this;
    }

    Builder& assignments(connector::ColumnHandleMap assignments) {
      assignments_ = std::move(assignments);
      return *this;
    }

    std::shared_ptr<TableScanNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "TableScanNode id is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "TableScanNode outputType is not set");
      VELOX_USER_CHECK(
          tableHandle_.has_value(), "TableScanNode tableHandle is not set");
      VELOX_USER_CHECK(
          assignments_.has_value(), "TableScanNode assignments is not set");

      return std::make_shared<TableScanNode>(
          id_.value(),
          outputType_.value(),
          tableHandle_.value(),
          assignments_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<RowTypePtr> outputType_;
    std::optional<connector::ConnectorTableHandlePtr> tableHandle_;
    std::optional<connector::ColumnHandleMap> assignments_;
  };

  bool supportsBarrier() const override {
    return true;
  }

  const std::vector<PlanNodePtr>& sources() const override;

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  bool requiresSplits() const override {
    return true;
  }

  const connector::ConnectorTableHandlePtr& tableHandle() const {
    return tableHandle_;
  }

  const connector::ColumnHandleMap& assignments() const {
    return assignments_;
  }

  std::string_view name() const override {
    return "TableScan";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

//  PlanNodePtr copyWithNewSources(
//      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  void addSummaryDetails(
      const std::string& indentation,
      const PlanSummaryOptions& options,
      std::stringstream& stream) const override;

  const RowTypePtr outputType_;
  const connector::ConnectorTableHandlePtr tableHandle_;
  const connector::ColumnHandleMap assignments_;
};

using TableScanNodePtr = std::shared_ptr<const TableScanNode>;

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

  VELOX_DECLARE_EMBEDDED_ENUM_NAME(Step);

  /// Aggregate function call.
  struct Aggregate {
    /// Function name and input column names.
    CallTypedExprPtr call;

    /// Raw input types used to properly identify aggregate function. These
    /// might be different from the input types specified in 'call' when
    /// aggregation step is kIntermediate or kFinal.
    std::vector<TypePtr> rawInputTypes;

    /// Optional name of input column to use as a mask. Column type must be
    /// BOOLEAN.
    FieldAccessTypedExprPtr mask{};

    /// Optional list of input columns to sort by before applying aggregate
    /// function.
    std::vector<FieldAccessTypedExprPtr> sortingKeys{};

    /// A list of sorting orders that goes together with 'sortingKeys'.
    std::vector<SortOrder> sortingOrders{};

    /// Boolean indicating whether inputs must be de-duplicated before
    /// aggregating.
    bool distinct{false};

    folly::dynamic serialize() const;

    static Aggregate deserialize(const folly::dynamic& obj, void* context);
  };

  AggregationNode(
      const PlanNodeId& id,
      Step step,
      const std::vector<FieldAccessTypedExprPtr>& groupingKeys,
      const std::vector<FieldAccessTypedExprPtr>& preGroupedKeys,
      const std::vector<std::string>& aggregateNames,
      const std::vector<Aggregate>& aggregates,
      bool ignoreNullKeys,
      PlanNodePtr source);

  /// @param globalGroupingSets Group IDs of the global grouping sets produced
  /// by the preceding GroupId node
  /// @param groupId Group ID key produced by the preceding GroupId node. Must
  /// be set if globalGroupingSets is not empty. Must not be set otherwise. Must
  /// be one of the groupingKeys.

  /// GlobalGroupingSets and groupId trigger special handling when the input
  /// data set is empty (no rows). In that case, aggregation generates a single
  /// row with the default global aggregate value per global grouping set.
  AggregationNode(
      const PlanNodeId& id,
      Step step,
      const std::vector<FieldAccessTypedExprPtr>& groupingKeys,
      const std::vector<FieldAccessTypedExprPtr>& preGroupedKeys,
      const std::vector<std::string>& aggregateNames,
      const std::vector<Aggregate>& aggregates,
      const std::vector<vector_size_t>& globalGroupingSets,
      const std::optional<FieldAccessTypedExprPtr>& groupId,
      bool ignoreNullKeys,
      PlanNodePtr source);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const AggregationNode& other) {
      id_ = other.id();
      step_ = other.step();
      groupingKeys_ = other.groupingKeys();
      preGroupedKeys_ = other.preGroupedKeys();
      aggregateNames_ = other.aggregateNames();
      aggregates_ = other.aggregates();
      globalGroupingSets_ = other.globalGroupingSets();
      groupId_ = other.groupId();
      ignoreNullKeys_ = other.ignoreNullKeys();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& step(Step step) {
      step_ = step;
      return *this;
    }

    Builder& groupingKeys(std::vector<FieldAccessTypedExprPtr> groupingKeys) {
      groupingKeys_ = std::move(groupingKeys);
      return *this;
    }

    Builder& preGroupedKeys(
        std::vector<FieldAccessTypedExprPtr> preGroupedKeys) {
      preGroupedKeys_ = std::move(preGroupedKeys);
      return *this;
    }

    Builder& aggregateNames(std::vector<std::string> aggregateNames) {
      aggregateNames_ = std::move(aggregateNames);
      return *this;
    }

    Builder& aggregates(std::vector<Aggregate> aggregates) {
      aggregates_ = std::move(aggregates);
      return *this;
    }

    Builder& globalGroupingSets(std::vector<vector_size_t> globalGroupingSets) {
      globalGroupingSets_ = std::move(globalGroupingSets);
      return *this;
    }

    Builder& groupId(std::optional<FieldAccessTypedExprPtr> groupId) {
      groupId_ = std::move(groupId);
      return *this;
    }

    Builder& ignoreNullKeys(bool ignoreNullKeys) {
      ignoreNullKeys_ = ignoreNullKeys;
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<AggregationNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "AggregationNode id is not set");
      VELOX_USER_CHECK(step_.has_value(), "AggregationNode step is not set");
      VELOX_USER_CHECK(
          groupingKeys_.has_value(), "AggregationNode groupingKeys is not set");
      VELOX_USER_CHECK(
          preGroupedKeys_.has_value(),
          "AggregationNode preGroupedKeys is not set");
      VELOX_USER_CHECK(
          aggregateNames_.has_value(),
          "AggregationNode aggregateNames is not set");
      VELOX_USER_CHECK(
          aggregates_.has_value(), "AggregationNode aggregates is not set");
      VELOX_USER_CHECK(
          ignoreNullKeys_.has_value(),
          "AggregationNode ignoreNullKeys is not set");
      VELOX_USER_CHECK(
          source_.has_value(), "AggregationNode source is not set");

      return std::make_shared<AggregationNode>(
          id_.value(),
          step_.value(),
          groupingKeys_.value(),
          preGroupedKeys_.value(),
          aggregateNames_.value(),
          aggregates_.value(),
          globalGroupingSets_,
          groupId_,
          ignoreNullKeys_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<Step> step_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> groupingKeys_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> preGroupedKeys_;
    std::optional<std::vector<std::string>> aggregateNames_;
    std::optional<std::vector<Aggregate>> aggregates_;
    std::vector<vector_size_t> globalGroupingSets_ = kDefaultGlobalGroupingSets;
    std::optional<FieldAccessTypedExprPtr> groupId_ = kDefaultGroupId;
    std::optional<bool> ignoreNullKeys_;
    std::optional<PlanNodePtr> source_;
  };

  bool supportsBarrier() const override {
    return isPreGrouped();
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  Step step() const {
    return step_;
  }

  const std::vector<FieldAccessTypedExprPtr>& groupingKeys() const {
    return groupingKeys_;
  }

  const std::vector<FieldAccessTypedExprPtr>& preGroupedKeys() const {
    return preGroupedKeys_;
  }

  bool isPreGrouped() const {
    return !preGroupedKeys_.empty() &&
        std::equal(
            preGroupedKeys_.begin(),
            preGroupedKeys_.end(),
            groupingKeys_.begin(),
            groupingKeys_.end(),
            [](const FieldAccessTypedExprPtr& x,
               const FieldAccessTypedExprPtr& y) -> bool {
              return (*x == *y);
            });
  }

  const std::vector<std::string>& aggregateNames() const {
    return aggregateNames_;
  }

  const std::vector<Aggregate>& aggregates() const {
    return aggregates_;
  }

  bool ignoreNullKeys() const {
    return ignoreNullKeys_;
  }

  const std::vector<vector_size_t>& globalGroupingSets() const {
    return globalGroupingSets_;
  }

  std::optional<FieldAccessTypedExprPtr> groupId() const {
    return groupId_;
  }

  std::string_view name() const override {
    return "Aggregation";
  }

  bool canSpill(const QueryConfig& queryConfig) const override;

  bool isFinal() const {
    return step_ == Step::kFinal;
  }

  bool isSingle() const {
    return step_ == Step::kSingle;
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);
//  void updateNewTypes(
//      const std::map<std::string, std::pair<std::string, TypePtr>>& newTypes)
//      override;

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  static const std::vector<vector_size_t> kDefaultGlobalGroupingSets;
  static const std::optional<FieldAccessTypedExprPtr> kDefaultGroupId;

  void addDetails(std::stringstream& stream) const override;

  void addSummaryDetails(
      const std::string& indentation,
      const PlanSummaryOptions& options,
      std::stringstream& stream) const override;

  const Step step_;
  const std::vector<FieldAccessTypedExprPtr> groupingKeys_;
  const std::vector<FieldAccessTypedExprPtr> preGroupedKeys_;
  const std::vector<std::string> aggregateNames_;
  const std::vector<Aggregate> aggregates_;
  const bool ignoreNullKeys_;

  std::optional<FieldAccessTypedExprPtr> groupId_;
  std::vector<vector_size_t> globalGroupingSets_;

  const std::vector<PlanNodePtr> sources_;
  const RowTypePtr outputType_;
};

using AggregationNodePtr = std::shared_ptr<const AggregationNode>;

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

/// Specify the column stats collection by aggregation. This is used by table
/// writer plan nodes.
struct ColumnStatsSpec : public ISerializable {
  /// Grouping keys of the aggregation. It is set to partitioning keys for the
  /// partitioned table. It is empty for unpartitioned table.
  std::vector<FieldAccessTypedExprPtr> groupingKeys;

  /// Step of the aggregation. Specifies the stage of multi-step aggregation
  /// processing for column statistics collection:
  /// - kSingle: used by TableWrite to complete aggregation in one step when no
  ///   multi-stage processing needed.
  /// - kPartial: used by TableWrite for the first stage of multi-step
  ///   aggregation, produces intermediate results that need further processing
  ///   (used in distributed scenarios)
  /// - kIntermediate: used by TableWriteMerge in middle stage that processes
  ///   partial results and produces more refined intermediate results for
  ///   further aggregation
  /// - kFinal: used by TableWriteMerge Final stage that processes intermediate
  ///   results to produce the complete aggregated statistics.
  AggregationNode::Step aggregationStep{AggregationNode::Step::kSingle};

  /// Names of the aggregations.
  std::vector<std::string> aggregateNames;

  /// Aggregations.
  std::vector<AggregationNode::Aggregate> aggregates;

  ColumnStatsSpec(
      std::vector<FieldAccessTypedExprPtr> _groupingKeys,
      AggregationNode::Step _aggregationStep,
      std::vector<std::string> _aggregateNames,
      std::vector<AggregationNode::Aggregate> _aggregates)
      : groupingKeys(std::move(_groupingKeys)),
        aggregationStep(_aggregationStep),
        aggregateNames(std::move(_aggregateNames)),
        aggregates(std::move(_aggregates)) {
    VELOX_CHECK(!aggregates.empty());
    VELOX_CHECK_EQ(aggregates.size(), aggregateNames.size());
  }

  folly::dynamic serialize() const override;

  static ColumnStatsSpec create(const folly::dynamic& obj, void* context);
};

class TableWriteNode : public PlanNode {
 public:
  TableWriteNode(
      const PlanNodeId& id,
      const RowTypePtr& columns,
      const std::vector<std::string>& columnNames,
      std::optional<ColumnStatsSpec> columnStatsSpec,
      std::shared_ptr<const InsertTableHandle> insertTableHandle,
      bool hasPartitioningScheme,
      RowTypePtr outputType,
      connector::CommitStrategy commitStrategy,
      const PlanNodePtr& source)
      : PlanNode(id),
        sources_{source},
        columns_{columns},
        columnNames_{columnNames},
        columnStatsSpec_(std::move(columnStatsSpec)),
        insertTableHandle_(std::move(insertTableHandle)),
        hasPartitioningScheme_(hasPartitioningScheme),
        outputType_(std::move(outputType)),
        commitStrategy_(commitStrategy) {
    VELOX_USER_CHECK_EQ(columns_->size(), columnNames_.size());
    for (const auto& column : columns_->names()) {
      VELOX_USER_CHECK(
          source->outputType()->containsChild(column),
          "Column {} not found in TableWriter input: {}",
          column,
          source->outputType()->toString());
    }
  }

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const TableWriteNode& other) {
      id_ = other.id();
      columns_ = other.columns();
      columnNames_ = other.columnNames();
      columnStatsSpec_ = other.columnStatsSpec();
      insertTableHandle_ = other.insertTableHandle();
      hasPartitioningScheme_ = other.hasPartitioningScheme();
      outputType_ = other.outputType();
      commitStrategy_ = other.commitStrategy();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& columns(RowTypePtr columns) {
      columns_ = std::move(columns);
      return *this;
    }

    Builder& columnNames(std::vector<std::string> columnNames) {
      columnNames_ = std::move(columnNames);
      return *this;
    }

    Builder& columnStatsSpec(std::optional<ColumnStatsSpec> columnStatsSpec) {
      columnStatsSpec_ = std::move(columnStatsSpec);
      return *this;
    }

    Builder& insertTableHandle(
        std::shared_ptr<InsertTableHandle> insertTableHandle) {
      insertTableHandle_ = std::move(insertTableHandle);
      return *this;
    }

    Builder& hasPartitioningScheme(bool hasPartitioningScheme) {
      hasPartitioningScheme_ = hasPartitioningScheme;
      return *this;
    }

    Builder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    Builder& commitStrategy(connector::CommitStrategy commitStrategy) {
      commitStrategy_ = commitStrategy;
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<TableWriteNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "TableWriteNode id is not set");
      VELOX_USER_CHECK(
          columns_.has_value(), "TableWriteNode columns is not set");
      VELOX_USER_CHECK(
          columnNames_.has_value(), "TableWriteNode columnNames is not set");
      VELOX_USER_CHECK(
          insertTableHandle_.has_value(),
          "TableWriteNode insertTableHandle is not set");
      VELOX_USER_CHECK(
          hasPartitioningScheme_.has_value(),
          "TableWriteNode hasPartitioningScheme is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "TableWriteNode outputType is not set");
      VELOX_USER_CHECK(
          commitStrategy_.has_value(),
          "TableWriteNode commitStrategy is not set");
      VELOX_USER_CHECK(source_.has_value(), "TableWriteNode source is not set");

      return std::make_shared<TableWriteNode>(
          id_.value(),
          columns_.value(),
          columnNames_.value(),
          columnStatsSpec_,
          insertTableHandle_.value(),
          hasPartitioningScheme_.value(),
          outputType_.value(),
          commitStrategy_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<RowTypePtr> columns_;
    std::optional<std::vector<std::string>> columnNames_;
    std::optional<ColumnStatsSpec> columnStatsSpec_;
    std::optional<std::shared_ptr<const InsertTableHandle>> insertTableHandle_;
    std::optional<bool> hasPartitioningScheme_;
    std::optional<RowTypePtr> outputType_;
    std::optional<connector::CommitStrategy> commitStrategy_;
    std::optional<PlanNodePtr> source_;
  };

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  /// The subset of columns in the output of the source node, potentially in
  /// different order, to write to the table.
  const RowTypePtr& columns() const {
    return columns_;
  }

  /// Column names to use when writing the table. This vector is aligned
  /// with 'columns' vector.
  const std::vector<std::string>& columnNames() const {
    return columnNames_;
  }

  const std::shared_ptr<const InsertTableHandle>& insertTableHandle() const {
    return insertTableHandle_;
  }

  /// Indicates if this table write plan node has specified partitioning
  /// scheme for remote and local shuffles. If true, the task creates a
  /// number of table write operators based on the query config
  /// 'task_partitioned_writer_count', otherwise based on
  /// x'task_writer_count'.
  bool hasPartitioningScheme() const {
    return hasPartitioningScheme_;
  }

  connector::CommitStrategy commitStrategy() const {
    return commitStrategy_;
  }

  /// Returns true of this table write plan node has configured column
  /// statistics collection.
  bool hasColumnStatsSpec() const {
    return columnStatsSpec_.has_value();
  }

  /// Optional spec for column statistics collection.
  const std::optional<ColumnStatsSpec>& columnStatsSpec() const {
    return columnStatsSpec_;
  }

  bool canSpill(const QueryConfig& queryConfig) const override {
    return queryConfig.writerSpillEnabled();
  }

  std::string_view name() const override {
    return "TableWrite";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<PlanNodePtr> sources_;
  const RowTypePtr columns_;
  const std::vector<std::string> columnNames_;
  const std::optional<ColumnStatsSpec> columnStatsSpec_;
  const std::shared_ptr<const InsertTableHandle> insertTableHandle_;
  const bool hasPartitioningScheme_;
  const RowTypePtr outputType_;
  const connector::CommitStrategy commitStrategy_;
};

using TableWriteNodePtr = std::shared_ptr<const TableWriteNode>;

class TableWriteMergeNode : public PlanNode {
 public:
  /// 'outputType' specifies the type to store the metadata of table write
  /// output which contains the following columns: 'numWrittenRows',
  /// 'fragment' and 'tableCommitContext'.
  TableWriteMergeNode(
      const PlanNodeId& id,
      RowTypePtr outputType,
      std::optional<ColumnStatsSpec> columnStatsSpec,
      PlanNodePtr source)
      : PlanNode(id),
        columnStatsSpec_(std::move(columnStatsSpec)),
        sources_{std::move(source)},
        outputType_(std::move(outputType)) {
    if (hasColumnStatsSpec()) {
      VELOX_USER_CHECK(
          columnStatsSpec_->aggregationStep ==
                  core::AggregationNode::Step::kFinal ||
              columnStatsSpec_->aggregationStep ==
                  core::AggregationNode::Step::kIntermediate,
          "TableWriteMergeNode requires aggregation step to be intermediate or final");
    }
  }

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const TableWriteMergeNode& other) {
      id_ = other.id();
      outputType_ = other.outputType();
      columnStatsSpec_ = other.columnStatsSpec();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    Builder& columnStatsSpec(std::optional<ColumnStatsSpec> columnStatsSpec) {
      columnStatsSpec_ = std::move(columnStatsSpec);
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<TableWriteMergeNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "TableWriteMergeNode id is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "TableWriteMergeNode outputType is not set");
      VELOX_USER_CHECK(
          source_.has_value(), "TableWriteMergeNode source is not set");

      return std::make_shared<TableWriteMergeNode>(
          id_.value(), outputType_.value(), columnStatsSpec_, source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<RowTypePtr> outputType_;
    std::optional<ColumnStatsSpec> columnStatsSpec_;
    std::optional<PlanNodePtr> source_;
  };

  /// Returns true of this table write merge plan node has configured column
  /// statistics collection.
  bool hasColumnStatsSpec() const {
    return columnStatsSpec_.has_value();
  }

  /// Optional spec for column statistics collection.
  const std::optional<ColumnStatsSpec>& columnStatsSpec() const {
    return columnStatsSpec_;
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  std::string_view name() const override {
    return "TableWriteMerge";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::optional<ColumnStatsSpec> columnStatsSpec_;
  const std::vector<PlanNodePtr> sources_;
  const RowTypePtr outputType_;
};

using TableWriteMergeNodePtr = std::shared_ptr<const TableWriteMergeNode>;

/// For each input row, generates N rows with M columns according to
/// specified 'projections'. 'projections' is an N x M matrix of expressions:
/// a vector of N rows each having M columns. Each expression is either a
/// column reference or a constant. Both null and non-null constants are
/// allowed. 'names' is a list of M new column names. The semantic of this
/// operator matches Spark.
class ExpandNode : public PlanNode {
 public:
  ExpandNode(
      PlanNodeId id,
      std::vector<std::vector<TypedExprPtr>> projections,
      std::vector<std::string> names,
      PlanNodePtr source);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const ExpandNode& other) {
      id_ = other.id();
      projections_ = other.projections();
      names_ = other.names();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& projections(std::vector<std::vector<TypedExprPtr>> projections) {
      projections_ = std::move(projections);
      return *this;
    }

    Builder& names(std::vector<std::string> names) {
      names_ = std::move(names);
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<ExpandNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "ExpandNode id is not set");
      VELOX_USER_CHECK(
          projections_.has_value(), "ExpandNode projections is not set");
      VELOX_USER_CHECK(names_.has_value(), "ExpandNode names is not set");
      VELOX_USER_CHECK(source_.has_value(), "ExpandNode source is not set");

      return std::make_shared<ExpandNode>(
          id_.value(), projections_.value(), names_.value(), source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::vector<std::vector<TypedExprPtr>>> projections_;
    std::optional<std::vector<std::string>> names_;
    std::optional<PlanNodePtr> source_;
  };

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const std::vector<std::vector<TypedExprPtr>>& projections() const {
    return projections_;
  }

  const std::vector<std::string>& names() const {
    return outputType_->names();
  }

  std::string_view name() const override {
    return "Expand";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<PlanNodePtr> sources_;
  const RowTypePtr outputType_;
  const std::vector<std::vector<TypedExprPtr>> projections_;
};

using ExpandNodePtr = std::shared_ptr<const ExpandNode>;

/// Plan node used to implement aggregations over grouping sets. Duplicates
/// the aggregation input for each set of grouping keys. The output contains
/// one column for each grouping key, followed by aggregation inputs, followed
/// by a column containing grouping set ID. For a given grouping set, a subset
/// of the grouping key columns present in the set are populated with values.
/// The rest of the grouping key columns are filled in with nulls.
class GroupIdNode : public PlanNode {
 public:
  struct GroupingKeyInfo {
    // The name to use in the output.
    std::string output;
    // The input field.
    FieldAccessTypedExprPtr input;

    folly::dynamic serialize() const;
  };

  /// @param id Plan node ID.
  /// @param groupingSets A list of grouping key sets. Grouping keys within
  /// the set must be unique, but grouping keys across sets may repeat. Note:
  /// groupingSets are specified using output column names.
  /// @param groupingKeyInfos The names and order of the grouping keys in the
  /// output.
  /// @param aggregationInputs Columns that contain inputs to the aggregate
  /// functions.
  /// @param groupIdName Name of the column that will contain the grouping set
  /// ID (a zero based integer).
  /// @param source Input plan node.
  GroupIdNode(
      PlanNodeId id,
      std::vector<std::vector<std::string>> groupingSets,
      std::vector<GroupingKeyInfo> groupingKeyInfos,
      std::vector<FieldAccessTypedExprPtr> aggregationInputs,
      std::string groupIdName,
      PlanNodePtr source);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const GroupIdNode& other) {
      id_ = other.id();
      groupingSets_ = other.groupingSets();
      groupingKeyInfos_ = other.groupingKeyInfos();
      aggregationInputs_ = other.aggregationInputs();
      groupIdName_ = other.groupIdName();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& groupingSets(std::vector<std::vector<std::string>> groupingSets) {
      groupingSets_ = std::move(groupingSets);
      return *this;
    }

    Builder& groupingKeyInfos(std::vector<GroupingKeyInfo> groupingKeyInfos) {
      groupingKeyInfos_ = std::move(groupingKeyInfos);
      return *this;
    }

    Builder& aggregationInputs(
        std::vector<FieldAccessTypedExprPtr> aggregationInputs) {
      aggregationInputs_ = std::move(aggregationInputs);
      return *this;
    }

    Builder& groupIdName(std::string groupIdName) {
      groupIdName_ = std::move(groupIdName);
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<GroupIdNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "GroupIdNode id is not set");
      VELOX_USER_CHECK(
          groupingSets_.has_value(), "GroupIdNode groupingSets is not set");
      VELOX_USER_CHECK(
          groupingKeyInfos_.has_value(),
          "GroupIdNode groupingKeyInfos is not set");
      VELOX_USER_CHECK(
          aggregationInputs_.has_value(),
          "GroupIdNode aggregationInputs is not set");
      VELOX_USER_CHECK(
          groupIdName_.has_value(), "GroupIdNode groupIdName is not set");
      VELOX_USER_CHECK(source_.has_value(), "GroupIdNode source is not set");

      return std::make_shared<GroupIdNode>(
          id_.value(),
          groupingSets_.value(),
          groupingKeyInfos_.value(),
          aggregationInputs_.value(),
          groupIdName_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::vector<std::vector<std::string>>> groupingSets_;
    std::optional<std::vector<GroupingKeyInfo>> groupingKeyInfos_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> aggregationInputs_;
    std::optional<std::string> groupIdName_;
    std::optional<PlanNodePtr> source_;
  };

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const std::vector<std::vector<std::string>>& groupingSets() const {
    return groupingSets_;
  }

  const std::vector<GroupingKeyInfo>& groupingKeyInfos() const {
    return groupingKeyInfos_;
  }

  const std::vector<FieldAccessTypedExprPtr>& aggregationInputs() const {
    return aggregationInputs_;
  }

  const std::string& groupIdName() const {
    return groupIdName_;
  }

  int32_t numGroupingKeys() const {
    return outputType_->size() - aggregationInputs_.size() - 1;
  }

  std::string_view name() const override {
    return "GroupId";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<PlanNodePtr> sources_;
  const RowTypePtr outputType_;

  // Specifies groupingSets with output column names.
  // This allows for the case when a single input column could map
  // to multiple output columns which are used in separate grouping sets.
  const std::vector<std::vector<std::string>> groupingSets_;

  const std::vector<GroupingKeyInfo> groupingKeyInfos_;
  const std::vector<FieldAccessTypedExprPtr> aggregationInputs_;
  const std::string groupIdName_;
};

using GroupIdNodePtr = std::shared_ptr<const GroupIdNode>;

class ExchangeNode : public PlanNode {
 public:
  ExchangeNode(
      const PlanNodeId& id,
      RowTypePtr type,
      VectorSerde::Kind serdeKind)
      : PlanNode(id), outputType_(type), serdeKind_(serdeKind) {}

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const ExchangeNode& other) {
      id_ = other.id();
      outputType_ = other.outputType();
      serdeKind_ = other.serdeKind();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    Builder& serdeKind(VectorSerde::Kind serdeKind) {
      serdeKind_ = serdeKind;
      return *this;
    }

    std::shared_ptr<ExchangeNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "ExchangeNode id is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "ExchangeNode outputType is not set");
      VELOX_USER_CHECK(
          serdeKind_.has_value(), "ExchangeNode serdeKind is not set");

      return std::make_shared<ExchangeNode>(
          id_.value(), outputType_.value(), serdeKind_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<RowTypePtr> outputType_;
    std::optional<VectorSerde::Kind> serdeKind_;
  };

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<PlanNodePtr>& sources() const override;

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  bool requiresExchangeClient() const override {
    return true;
  }

  bool requiresSplits() const override {
    return true;
  }

  std::string_view name() const override {
    return "Exchange";
  }

  VectorSerde::Kind serdeKind() const {
    return serdeKind_;
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

//  PlanNodePtr copyWithNewSources(
//      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const RowTypePtr outputType_;
  const VectorSerde::Kind serdeKind_;
};

using ExchangeNodePtr = std::shared_ptr<const ExchangeNode>;

class MergeExchangeNode : public ExchangeNode {
 public:
  MergeExchangeNode(
      const PlanNodeId& id,
      const RowTypePtr& type,
      const std::vector<FieldAccessTypedExprPtr>& sortingKeys,
      const std::vector<SortOrder>& sortingOrders,
      VectorSerde::Kind serdeKind);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const MergeExchangeNode& other) {
      id_ = other.id();
      outputType_ = other.outputType();
      sortingKeys_ = other.sortingKeys();
      sortingOrders_ = other.sortingOrders();
      serdeKind_ = other.serdeKind();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    Builder& sortingKeys(std::vector<FieldAccessTypedExprPtr> sortingKeys) {
      sortingKeys_ = std::move(sortingKeys);
      return *this;
    }

    Builder& sortingOrders(std::vector<SortOrder> sortingOrders) {
      sortingOrders_ = std::move(sortingOrders);
      return *this;
    }

    Builder& serdeKind(VectorSerde::Kind serdeKind) {
      serdeKind_ = serdeKind;
      return *this;
    }

    std::shared_ptr<MergeExchangeNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "MergeExchangeNode id is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "MergeExchangeNode outputType is not set");
      VELOX_USER_CHECK(
          sortingKeys_.has_value(), "MergeExchangeNode sortingKeys is not set");
      VELOX_USER_CHECK(
          sortingOrders_.has_value(),
          "MergeExchangeNode sortingOrders is not set");
      VELOX_USER_CHECK(
          serdeKind_.has_value(), "MergeExchangeNode serdeKind is not set");

      return std::make_shared<MergeExchangeNode>(
          id_.value(),
          outputType_.value(),
          sortingKeys_.value(),
          sortingOrders_.value(),
          serdeKind_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<RowTypePtr> outputType_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> sortingKeys_;
    std::optional<std::vector<SortOrder>> sortingOrders_;
    std::optional<VectorSerde::Kind> serdeKind_;
  };

  const std::vector<FieldAccessTypedExprPtr>& sortingKeys() const {
    return sortingKeys_;
  }

  const std::vector<SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  std::string_view name() const override {
    return "MergeExchange";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<FieldAccessTypedExprPtr> sortingKeys_;
  const std::vector<SortOrder> sortingOrders_;
};

using MergeExchangeNodePtr = std::shared_ptr<const MergeExchangeNode>;

class LocalMergeNode : public PlanNode {
 public:
  LocalMergeNode(
      const PlanNodeId& id,
      std::vector<FieldAccessTypedExprPtr> sortingKeys,
      std::vector<SortOrder> sortingOrders,
      std::vector<PlanNodePtr> sources)
      : PlanNode(id),
        sources_{std::move(sources)},
        sortingKeys_{std::move(sortingKeys)},
        sortingOrders_{std::move(sortingOrders)} {}

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const LocalMergeNode& other) {
      id_ = other.id();
      sortingKeys_ = other.sortingKeys();
      sortingOrders_ = other.sortingOrders();
      sources_ = other.sources();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& sortingKeys(std::vector<FieldAccessTypedExprPtr> sortingKeys) {
      sortingKeys_ = std::move(sortingKeys);
      return *this;
    }

    Builder& sortingOrders(std::vector<SortOrder> sortingOrders) {
      sortingOrders_ = std::move(sortingOrders);
      return *this;
    }

    Builder& sources(std::vector<PlanNodePtr> sources) {
      sources_ = std::move(sources);
      return *this;
    }

    std::shared_ptr<LocalMergeNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "LocalMergeNode id is not set");
      VELOX_USER_CHECK(
          sortingKeys_.has_value(), "LocalMergeNode sortingKeys is not set");
      VELOX_USER_CHECK(
          sortingOrders_.has_value(),
          "LocalMergeNode sortingOrders is not set");
      VELOX_USER_CHECK(
          sources_.has_value(), "LocalMergeNode sources is not set");

      return std::make_shared<LocalMergeNode>(
          id_.value(),
          sortingKeys_.value(),
          sortingOrders_.value(),
          sources_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> sortingKeys_;
    std::optional<std::vector<SortOrder>> sortingOrders_;
    std::optional<std::vector<PlanNodePtr>> sources_;
  };

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const std::vector<FieldAccessTypedExprPtr>& sortingKeys() const {
    return sortingKeys_;
  }

  bool canSpill(const QueryConfig& queryConfig) const override {
    return !sortingKeys_.empty() && queryConfig.localMergeSpillEnabled();
  }

  const std::vector<SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  std::string_view name() const override {
    return "LocalMerge";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<PlanNodePtr> sources_;
  const std::vector<FieldAccessTypedExprPtr> sortingKeys_;
  const std::vector<SortOrder> sortingOrders_;
};

using LocalMergeNodePtr = std::shared_ptr<const LocalMergeNode>;

/// Calculates partition number for each row of the specified vector.
class PartitionFunction {
 public:
  virtual ~PartitionFunction() = default;

  /// @param input RowVector to split into partitions.
  /// @param [out] partitions Computed partition numbers for each row in
  /// 'input'.
  /// @return Returns partition number in case all rows of 'input' are
  /// assigned to the same partition. In this case 'partitions' vector is left
  /// unchanged. Used to optimize round-robin partitioning in local exchange.
  virtual std::optional<uint32_t> partition(
      const RowVector& input,
      std::vector<uint32_t>& partitions) = 0;
};

/// Factory class for creating PartitionFunction instances.
class PartitionFunctionSpec : public ISerializable {
 public:
  /// If 'localExchange' is true, the partition function is used for local
  /// exchange within a velox task.
  virtual std::unique_ptr<PartitionFunction> create(
      int numPartitions,
      bool localExchange = false) const = 0;

  virtual ~PartitionFunctionSpec() = default;

  virtual std::string toString() const = 0;
};

using PartitionFunctionSpecPtr = std::shared_ptr<const PartitionFunctionSpec>;

class GatherPartitionFunctionSpec : public PartitionFunctionSpec {
 public:
  std::unique_ptr<PartitionFunction> create(
      int /*numPartitions*/,
      bool /*localExchange*/) const override {
    VELOX_UNREACHABLE();
  }

  std::string toString() const override {
    return "gather";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "GatherPartitionFunctionSpec";
    return obj;
  }

  static PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& /* obj */,
      void* /* context */) {
    return std::make_shared<GatherPartitionFunctionSpec>();
  }
};

/// Partitions data using specified partition function. The number of
/// partitions is determined by the parallelism of the upstream pipeline. Can
/// be used to gather data from multiple sources.
class LocalPartitionNode : public PlanNode {
 public:
  enum class Type {
    // N-to-1 exchange.
    kGather,
    // N-to-M shuffle.
    kRepartition,
  };

  VELOX_DECLARE_EMBEDDED_ENUM_NAME(Type);

  /// If 'scaleWriter' is true, the local partition is used to scale the table
  /// writer prcessing.
  LocalPartitionNode(
      const PlanNodeId& id,
      Type type,
      bool scaleWriter,
      PartitionFunctionSpecPtr partitionFunctionSpec,
      std::vector<PlanNodePtr> sources)
      : PlanNode(id),
        type_{type},
        scaleWriter_(scaleWriter),
        sources_{std::move(sources)},
        partitionFunctionSpec_{std::move(partitionFunctionSpec)} {
    VELOX_USER_CHECK_GT(
        sources_.size(),
        0,
        "Local repartitioning node requires at least one source");

    VELOX_USER_CHECK_NOT_NULL(partitionFunctionSpec_);

    for (size_t i = 1; i < sources_.size(); ++i) {
      VELOX_USER_CHECK(
          *sources_[i]->outputType() == *sources_[0]->outputType(),
          "All sources of the LocalPartitionedNode must have the same output type: {} vs. {}.",
          sources_[i]->outputType()->toString(),
          sources_[0]->outputType()->toString());
    }
  }

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const LocalPartitionNode& other) {
      id_ = other.id();
      type_ = other.type();
      scaleWriter_ = other.scaleWriter();
      partitionFunctionSpec_ = other.partitionFunctionSpec_;
      sources_ = other.sources();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& type(LocalPartitionNode::Type type) {
      type_ = type;
      return *this;
    }

    Builder& scaleWriter(bool scaleWriter) {
      scaleWriter_ = scaleWriter;
      return *this;
    }

    Builder& partitionFunctionSpec(PartitionFunctionSpecPtr spec) {
      partitionFunctionSpec_ = std::move(spec);
      return *this;
    }

    Builder& sources(std::vector<PlanNodePtr> sources) {
      sources_ = std::move(sources);
      return *this;
    }

    std::shared_ptr<LocalPartitionNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "LocalPartitionNode id is not set");
      VELOX_USER_CHECK(type_.has_value(), "LocalPartitionNode type is not set");
      VELOX_USER_CHECK(
          scaleWriter_.has_value(),
          "LocalPartitionNode scaleWriter is not set");
      VELOX_USER_CHECK(
          partitionFunctionSpec_.has_value(),
          "LocalPartitionNode partitionFunctionSpec is not set");
      VELOX_USER_CHECK(
          sources_.has_value(), "LocalPartitionNode sources is not set");

      return std::make_shared<LocalPartitionNode>(
          id_.value(),
          type_.value(),
          scaleWriter_.value(),
          partitionFunctionSpec_.value(),
          sources_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<LocalPartitionNode::Type> type_;
    std::optional<bool> scaleWriter_;
    std::optional<PartitionFunctionSpecPtr> partitionFunctionSpec_;
    std::optional<std::vector<PlanNodePtr>> sources_;
  };

  static std::shared_ptr<LocalPartitionNode> gather(
      const PlanNodeId& id,
      std::vector<PlanNodePtr> sources) {
    return std::make_shared<LocalPartitionNode>(
        id,
        Type::kGather,
        /*scaleWriter=*/false,
        std::make_shared<GatherPartitionFunctionSpec>(),
        std::move(sources));
  }

  /// Returns true if this is for table writer scaling.
  bool scaleWriter() const {
    return scaleWriter_;
  }

  bool supportsBarrier() const override {
    return !scaleWriter_;
  }

  Type type() const {
    return type_;
  }

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const PartitionFunctionSpec& partitionFunctionSpec() const {
    return *partitionFunctionSpec_;
  }

  std::string_view name() const override {
    return "LocalPartition";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const Type type_;
  const bool scaleWriter_;
  const std::vector<PlanNodePtr> sources_;
  const PartitionFunctionSpecPtr partitionFunctionSpec_;
};

using LocalPartitionNodePtr = std::shared_ptr<const LocalPartitionNode>;

class PartitionedOutputNode : public PlanNode {
 public:
  enum class Kind {
    kPartitioned,
    kBroadcast,
    kArbitrary,
  };

  VELOX_DECLARE_EMBEDDED_ENUM_NAME(Kind)

  PartitionedOutputNode(
      const PlanNodeId& id,
      Kind kind,
      const std::vector<TypedExprPtr>& keys,
      int numPartitions,
      bool replicateNullsAndAny,
      PartitionFunctionSpecPtr partitionFunctionSpec,
      RowTypePtr outputType,
      VectorSerde::Kind serdeKind,
      PlanNodePtr source);

  static std::shared_ptr<PartitionedOutputNode> broadcast(
      const PlanNodeId& id,
      int numPartitions,
      RowTypePtr outputType,
      VectorSerde::Kind serdeKind,
      PlanNodePtr source);

  static std::shared_ptr<PartitionedOutputNode> arbitrary(
      const PlanNodeId& id,
      RowTypePtr outputType,
      VectorSerde::Kind serdeKind,
      PlanNodePtr source);

  static std::shared_ptr<PartitionedOutputNode> single(
      const PlanNodeId& id,
      RowTypePtr outputType,
      VectorSerde::Kind VectorSerde,
      PlanNodePtr source);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const PartitionedOutputNode& other) {
      id_ = other.id();
      kind_ = other.kind();
      keys_ = other.keys();
      numPartitions_ = other.numPartitions();
      replicateNullsAndAny_ = other.isReplicateNullsAndAny();
      partitionFunctionSpec_ = other.partitionFunctionSpecPtr();
      outputType_ = other.outputType();
      serdeKind_ = other.serdeKind();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& kind(PartitionedOutputNode::Kind kind) {
      kind_ = kind;
      return *this;
    }

    Builder& keys(std::vector<TypedExprPtr> keys) {
      keys_ = std::move(keys);
      return *this;
    }

    Builder& numPartitions(int numPartitions) {
      numPartitions_ = numPartitions;
      return *this;
    }

    Builder& replicateNullsAndAny(bool replicate) {
      replicateNullsAndAny_ = replicate;
      return *this;
    }

    Builder& partitionFunctionSpec(PartitionFunctionSpecPtr spec) {
      partitionFunctionSpec_ = std::move(spec);
      return *this;
    }

    Builder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    Builder& serdeKind(VectorSerde::Kind serdeKind) {
      serdeKind_ = serdeKind;
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<PartitionedOutputNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "PartitionedOutputNode id is not set");
      VELOX_USER_CHECK(
          kind_.has_value(), "PartitionedOutputNode kind is not set");
      VELOX_USER_CHECK(
          keys_.has_value(), "PartitionedOutputNode keys is not set");
      VELOX_USER_CHECK(
          numPartitions_.has_value(),
          "PartitionedOutputNode numPartitions is not set");
      VELOX_USER_CHECK(
          replicateNullsAndAny_.has_value(),
          "PartitionedOutputNode replicateNullsAndAny is not set");
      VELOX_USER_CHECK(
          partitionFunctionSpec_.has_value(),
          "PartitionedOutputNode partitionFunctionSpec is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(),
          "PartitionedOutputNode outputType is not set");
      VELOX_USER_CHECK(
          serdeKind_.has_value(), "PartitionedOutputNode serdeKind is not set");
      VELOX_USER_CHECK(
          source_.has_value(), "PartitionedOutputNode source is not set");

      return std::make_shared<PartitionedOutputNode>(
          id_.value(),
          kind_.value(),
          keys_.value(),
          numPartitions_.value(),
          replicateNullsAndAny_.value(),
          partitionFunctionSpec_.value(),
          outputType_.value(),
          serdeKind_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<PartitionedOutputNode::Kind> kind_;
    std::optional<std::vector<TypedExprPtr>> keys_;
    std::optional<int> numPartitions_;
    std::optional<bool> replicateNullsAndAny_;
    std::optional<PartitionFunctionSpecPtr> partitionFunctionSpec_;
    std::optional<RowTypePtr> outputType_;
    std::optional<VectorSerde::Kind> serdeKind_;
    std::optional<PlanNodePtr> source_;
  };

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  const std::vector<TypedExprPtr>& keys() const {
    return keys_;
  }

  int numPartitions() const {
    return numPartitions_;
  }

  bool isPartitioned() const {
    return kind_ == Kind::kPartitioned;
  }

  bool isBroadcast() const {
    return kind_ == Kind::kBroadcast;
  }

  bool isArbitrary() const {
    return kind_ == Kind::kArbitrary;
  }

  Kind kind() const {
    return kind_;
  }

  VectorSerde::Kind serdeKind() const {
    return serdeKind_;
  }

  /// Returns true if an arbitrary row and all rows with null keys must be
  /// replicated to all destinations. This is used to ensure correct results
  /// for anti-join which requires all nodes to know whether combined build
  /// side is empty and whether it has any entry with null join key.
  bool isReplicateNullsAndAny() const {
    return replicateNullsAndAny_;
  }

  const PartitionFunctionSpecPtr& partitionFunctionSpecPtr() const {
    return partitionFunctionSpec_;
  }

  const PartitionFunctionSpec& partitionFunctionSpec() const {
    VELOX_CHECK_NOT_NULL(partitionFunctionSpec_);
    return *partitionFunctionSpec_;
  }

  std::string_view name() const override {
    return "PartitionedOutput";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const Kind kind_;
  const std::vector<PlanNodePtr> sources_;
  const std::vector<TypedExprPtr> keys_;
  const int numPartitions_;
  const bool replicateNullsAndAny_;
  const PartitionFunctionSpecPtr partitionFunctionSpec_;
  const VectorSerde::Kind serdeKind_;
  const RowTypePtr outputType_;
};

using PartitionedOutputNodePtr = std::shared_ptr<const PartitionedOutputNode>;

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& out,
    const PartitionedOutputNode::Kind kind) {
  out << PartitionedOutputNode::toName(kind);
  return out;
}

enum class JoinType {
  // For each row on the left, find all matching rows on the right and return
  // all combinations.
  kInner = 0,

  // For each row on the left, find all matching rows on the right and return
  // all combinations. In addition, return all rows from the left that have no
  // match on the right with right-side columns filled with nulls.
  kLeft = 1,

  // Opposite of kLeft. For each row on the right, find all matching rows on
  // the left and return all combinations. In addition, return all rows from the
  // right that have no match on the left with left-side columns filled with
  // nulls.
  kRight = 2,

  // A "union" of kLeft and kRight. For each row on the left, find all
  // matching rows on the right and return all combinations. In addition, return
  // all rows from the left that have no match on the right with right-side
  // columns filled with nulls. Also, return all rows from the right that have
  // no match on the left with left-side columns filled with nulls.
  kFull = 3,

  // Return a subset of rows from the left side which have a match on the
  // right side. For this join type, cardinality of the output is less than or
  // equal to the cardinality of the left side.
  kLeftSemiFilter = 4,

  // Return each row from the left side with a boolean flag indicating whether
  // there exists a match on the right side. For this join type, cardinality
  // of the output equals the cardinality of the left side.
  //
  // The handling of the rows with nulls in the join key depends on the
  // 'nullAware' boolean specified separately.
  //
  // Null-aware join follows IN semantic. Regular join follows EXISTS semantic.
  kLeftSemiProject = 5,

  // Opposite of kLeftSemiFilter. Return a subset of rows from the right side
  // which have a match on the left side. For this join type, cardinality of
  // the output is less than or equal to the cardinality of the right side.
  kRightSemiFilter = 6,

  // Opposite of kLeftSemiProject. Return each row from the right side with a
  // boolean flag indicating whether there exists a match on the left side.
  // For this join type, cardinality of the output equals the cardinality of the
  // right side.
  //
  // The handling of the rows with nulls in the join key depends on the
  // 'nullAware' boolean specified separately.
  //
  // Null-aware join follows IN semantic. Regular join follows EXISTS semantic.
  kRightSemiProject = 7,

  // Return each row from the left side which has no match on the right side.
  // The handling of the rows with nulls in the join key depends on the
  // 'nullAware' boolean specified separately.
  //
  // Null-aware join follows NOT IN semantic:
  // (1) return empty result if the right side contains a record with a null in
  // the join key;
  // (2) return left-side row with null in the join key only when the right side
  // is empty.
  //
  // Regular anti join follows NOT EXISTS semantic:
  // (1) ignore right-side rows with nulls in the join keys;
  // (2) unconditionally return left side rows with nulls in the join keys.
  kAnti = 8,

  kNumJoinTypes = 9,
};

VELOX_DECLARE_ENUM_NAME(JoinType);

inline bool isInnerJoin(JoinType joinType) {
  return joinType == JoinType::kInner;
}

inline bool isLeftJoin(JoinType joinType) {
  return joinType == JoinType::kLeft;
}

inline bool isRightJoin(JoinType joinType) {
  return joinType == JoinType::kRight;
}

inline bool isFullJoin(JoinType joinType) {
  return joinType == JoinType::kFull;
}

inline bool isLeftSemiFilterJoin(JoinType joinType) {
  return joinType == JoinType::kLeftSemiFilter;
}

inline bool isLeftSemiProjectJoin(JoinType joinType) {
  return joinType == JoinType::kLeftSemiProject;
}

inline bool isRightSemiFilterJoin(JoinType joinType) {
  return joinType == JoinType::kRightSemiFilter;
}

inline bool isRightSemiProjectJoin(JoinType joinType) {
  return joinType == JoinType::kRightSemiProject;
}

inline bool isAntiJoin(JoinType joinType) {
  return joinType == JoinType::kAnti;
}

inline bool isNullAwareSupported(JoinType joinType) {
  return joinType == JoinType::kAnti ||
      joinType == JoinType::kLeftSemiProject ||
      joinType == JoinType::kRightSemiProject;
}

/// Abstract class representing inner/outer/semi/anti joins. Used as a base
/// class for specific join implementations, e.g. hash and merge joins.
class AbstractJoinNode : public PlanNode {
 public:
  AbstractJoinNode(
      const PlanNodeId& id,
      JoinType joinType,
      const std::vector<FieldAccessTypedExprPtr>& leftKeys,
      const std::vector<FieldAccessTypedExprPtr>& rightKeys,
      TypedExprPtr filter,
      PlanNodePtr left,
      PlanNodePtr right,
      RowTypePtr outputType);

  template <typename DerivedPlanNode, typename DerivedBuilder>
  class Builder {
   public:
    Builder() = default;

    explicit Builder(const AbstractJoinNode& other) {
      id_ = other.id();
      joinType_ = other.joinType();
      leftKeys_ = other.leftKeys();
      rightKeys_ = other.rightKeys();
      filter_ = other.filter();
      VELOX_CHECK_EQ(other.sources().size(), 2);
      left_ = other.sources()[0];
      right_ = other.sources()[1];
      outputType_ = other.outputType();
    }

    virtual ~Builder() = default;

    DerivedBuilder& id(PlanNodeId id) {
      id_ = std::move(id);
      return static_cast<DerivedBuilder&>(*this);
    }

    DerivedBuilder& joinType(JoinType joinType) {
      joinType_ = joinType;
      return static_cast<DerivedBuilder&>(*this);
    }

    DerivedBuilder& leftKeys(std::vector<FieldAccessTypedExprPtr> leftKeys) {
      leftKeys_ = std::move(leftKeys);
      return static_cast<DerivedBuilder&>(*this);
    }

    DerivedBuilder& rightKeys(std::vector<FieldAccessTypedExprPtr> rightKeys) {
      rightKeys_ = std::move(rightKeys);
      return static_cast<DerivedBuilder&>(*this);
    }

    DerivedBuilder& filter(TypedExprPtr filter) {
      filter_ = std::move(filter);
      return static_cast<DerivedBuilder&>(*this);
    }

    DerivedBuilder& left(PlanNodePtr left) {
      left_ = std::move(left);
      return static_cast<DerivedBuilder&>(*this);
    }

    DerivedBuilder& right(PlanNodePtr right) {
      right_ = std::move(right);
      return static_cast<DerivedBuilder&>(*this);
    }

    DerivedBuilder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return static_cast<DerivedBuilder&>(*this);
    }

   protected:
    std::optional<PlanNodeId> id_;
    std::optional<JoinType> joinType_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> leftKeys_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> rightKeys_;
    std::optional<TypedExprPtr> filter_;
    std::optional<PlanNodePtr> left_;
    std::optional<PlanNodePtr> right_;
    std::optional<RowTypePtr> outputType_;
  };

  const std::vector<PlanNodePtr>& sources() const override {
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

  bool isLeftSemiFilterJoin() const {
    return joinType_ == JoinType::kLeftSemiFilter;
  }

  bool isLeftSemiProjectJoin() const {
    return joinType_ == JoinType::kLeftSemiProject;
  }

  bool isRightSemiFilterJoin() const {
    return joinType_ == JoinType::kRightSemiFilter;
  }

  bool isRightSemiProjectJoin() const {
    return joinType_ == JoinType::kRightSemiProject;
  }

  bool isAntiJoin() const {
    return joinType_ == JoinType::kAnti;
  }

  bool isPreservingProbeOrder() const {
    return isInnerJoin() || isLeftJoin() || isAntiJoin();
  }

  const std::vector<FieldAccessTypedExprPtr>& leftKeys() const {
    return leftKeys_;
  }

  const std::vector<FieldAccessTypedExprPtr>& rightKeys() const {
    return rightKeys_;
  }

//  void updateNewTypes(
//      const std::map<std::string, std::pair<std::string, TypePtr>>&
//          newOutputTypes) override;

  //  void updateJoinKeys(std::map<std::string, TypePtr> updatedOutputTypes) {
  //    leftKeys_ = leftKeys;
  //    rightKeys_ = rightKeys;
  //  }

  const TypedExprPtr& filter() const {
    return filter_;
  }

 protected:
  void validate() const;
  void addDetails(std::stringstream& stream) const override;

  folly::dynamic serializeBase() const;

  const JoinType joinType_;
  const std::vector<FieldAccessTypedExprPtr> leftKeys_;
  const std::vector<FieldAccessTypedExprPtr> rightKeys_;
  // Optional join filter, nullptr if absent. This is applied to
  // join hits and if this is false, the hit turns into a miss, which
  // has a special meaning for outer joins. For inner joins, this is
  // equivalent to a Filter above the join.
  const TypedExprPtr filter_;
  const std::vector<PlanNodePtr> sources_;
  const RowTypePtr outputType_;
};

/// Represents inner/outer/semi/anti hash joins. Translates to an
/// exec::HashBuild and exec::HashProbe. A separate pipeline is produced for
/// the build side when generating exec::Operators.
///
/// 'nullAware' boolean applies to semi and anti joins. When true, the join
/// semantic is IN / NOT IN. When false, the join semantic is EXISTS / NOT
/// EXISTS.
class HashJoinNode : public AbstractJoinNode {
 public:
  HashJoinNode(
      const PlanNodeId& id,
      JoinType joinType,
      bool nullAware,
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
            std::move(outputType)),
        nullAware_{nullAware} {
    validate();

    if (nullAware) {
      VELOX_USER_CHECK(
          isNullAwareSupported(joinType),
          "Null-aware flag is supported only for semi project and anti joins");
      VELOX_USER_CHECK_EQ(
          1, leftKeys_.size(), "Null-aware joins allow only one join key");

      if (filter_) {
        VELOX_USER_CHECK(
            !isRightSemiProjectJoin(),
            "Null-aware right semi project join doesn't support extra filter");
      }
    }
  }

  class Builder : public AbstractJoinNode::Builder<HashJoinNode, Builder> {
   public:
    Builder() = default;

    explicit Builder(const HashJoinNode& other)
        : AbstractJoinNode::Builder<HashJoinNode, Builder>(other) {
      nullAware_ = other.isNullAware();
    }

    Builder& nullAware(bool value) {
      nullAware_ = value;
      return *this;
    }

    std::shared_ptr<HashJoinNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "HashJoinNode id is not set");
      VELOX_USER_CHECK(
          joinType_.has_value(), "HashJoinNode joinType is not set");
      VELOX_USER_CHECK(
          leftKeys_.has_value(), "HashJoinNode leftKeys is not set");
      VELOX_USER_CHECK(
          rightKeys_.has_value(), "HashJoinNode rightKeys is not set");
      VELOX_USER_CHECK(
          left_.has_value(), "HashJoinNode left source is not set");
      VELOX_USER_CHECK(
          right_.has_value(), "HashJoinNode right source is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "HashJoinNode outputType is not set");
      VELOX_USER_CHECK(
          nullAware_.has_value(), "HashJoinNode nullAware flag is not set");

      return std::make_shared<HashJoinNode>(
          id_.value(),
          joinType_.value(),
          nullAware_.value(),
          leftKeys_.value(),
          rightKeys_.value(),
          filter_.value_or(nullptr),
          left_.value(),
          right_.value(),
          outputType_.value());
    }

   private:
    std::optional<bool> nullAware_;
  };

  std::string_view name() const override {
    return "HashJoin";
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  bool canSpill(const QueryConfig& queryConfig) const override {
    // NOTE: as for now, we don't allow spilling for null-aware anti-join with
    // filter set. It requires to cross join the null-key probe rows with all
    // the build-side rows for filter evaluation which is not supported under
    // spilling.
    return !(isAntiJoin() && nullAware_ && filter() != nullptr) &&
        queryConfig.joinSpillEnabled();
  }

  bool isNullAware() const {
    return nullAware_;
  }

  bool updateKeys(std::map<std::string, TypePtr> updatedOutputTypes) {
    bool updated = false;
    for (auto& key : leftKeys_) {
      auto it = updatedOutputTypes.find(key->name());
      if (it != updatedOutputTypes.end()) {
        updated = true;
        printf(
            "Update left join key %s to type %s\n",
            key->name().c_str(),
            it->second->toString().c_str());
      }
    }
    return updated;
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const bool nullAware_;
};

using HashJoinNodePtr = std::shared_ptr<const HashJoinNode>;

/// Represents inner/outer/semi/anti merge joins. Translates to an
/// exec::MergeJoin operator. Assumes that both left and right input data is
/// sorted on the join keys. A separate pipeline that puts its output into
/// exec::MergeJoinSource is produced for the right side when generating
/// exec::Operators.
class MergeJoinNode : public AbstractJoinNode {
 public:
  MergeJoinNode(
      const PlanNodeId& id,
      JoinType joinType,
      const std::vector<FieldAccessTypedExprPtr>& leftKeys,
      const std::vector<FieldAccessTypedExprPtr>& rightKeys,
      TypedExprPtr filter,
      PlanNodePtr left,
      PlanNodePtr right,
      RowTypePtr outputType);

  class Builder : public AbstractJoinNode::Builder<MergeJoinNode, Builder> {
   public:
    Builder() = default;

    explicit Builder(const MergeJoinNode& other)
        : AbstractJoinNode::Builder<MergeJoinNode, Builder>(other) {}

    std::shared_ptr<MergeJoinNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "MergeJoinNode id is not set");
      VELOX_USER_CHECK(
          joinType_.has_value(), "MergeJoinNode joinType is not set");
      VELOX_USER_CHECK(
          leftKeys_.has_value(), "MergeJoinNode leftKeys is not set");
      VELOX_USER_CHECK(
          rightKeys_.has_value(), "MergeJoinNode rightKeys is not set");
      VELOX_USER_CHECK(
          left_.has_value(), "MergeJoinNode left source is not set");
      VELOX_USER_CHECK(
          right_.has_value(), "MergeJoinNode right source is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "MergeJoinNode outputType is not set");

      return std::make_shared<MergeJoinNode>(
          id_.value(),
          joinType_.value(),
          leftKeys_.value(),
          rightKeys_.value(),
          filter_.value_or(nullptr),
          left_.value(),
          right_.value(),
          outputType_.value());
    }
  };

  std::string_view name() const override {
    return "MergeJoin";
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  bool supportsBarrier() const override {
    return true;
  }

  /// Returns true if the merge join supports this join type, otherwise false.
  static bool isSupported(JoinType joinType);

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;
};

using MergeJoinNodePtr = std::shared_ptr<const MergeJoinNode>;

struct IndexLookupCondition : public ISerializable {
  /// References to an index table column.
  FieldAccessTypedExprPtr key;

  explicit IndexLookupCondition(FieldAccessTypedExprPtr _key)
      : key(std::move(_key)) {
    VELOX_CHECK_NOT_NULL(key);
  }

  /// Indicates if this object represents a filter condition or not. A filter
  /// condition only involves one table index column plus constant values. A
  /// join condition involves one table index column plus at least one probe
  /// input column.
  virtual bool isFilter() const = 0;

  folly::dynamic serialize() const override;

  virtual std::string toString() const = 0;
};

using IndexLookupConditionPtr = std::shared_ptr<IndexLookupCondition>;

/// Represents IN-LIST index lookup condition: contains('list', 'key'). 'list'
/// can be either a probe input column or a constant list with type of
/// ARRAY(typeof('key')).
struct InIndexLookupCondition : public IndexLookupCondition {
  /// References to either a probe input column or a constant list.
  TypedExprPtr list;

  InIndexLookupCondition(FieldAccessTypedExprPtr _key, TypedExprPtr _list)
      : IndexLookupCondition(std::move(_key)), list(std::move(_list)) {
    validate();
  }

  bool isFilter() const override;

  folly::dynamic serialize() const override;

  std::string toString() const override;

  static IndexLookupConditionPtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void validate() const;
};

using InIndexLookupConditionPtr = std::shared_ptr<InIndexLookupCondition>;

/// Represents BETWEEN index lookup condition: 'key' between 'lower' and
/// 'upper'. 'lower' and 'upper' have the same type of 'key'.
struct BetweenIndexLookupCondition : public IndexLookupCondition {
  /// The between bound either reference to a probe input column or a constant
  /// value.
  ///
  /// NOTE: the bounds are inclusive.
  TypedExprPtr lower;
  TypedExprPtr upper;

  BetweenIndexLookupCondition(
      FieldAccessTypedExprPtr _key,
      TypedExprPtr _lower,
      TypedExprPtr _upper)
      : IndexLookupCondition(std::move(_key)),
        lower(std::move(_lower)),
        upper(std::move(_upper)) {
    validate();
  }

  bool isFilter() const override;

  folly::dynamic serialize() const override;

  std::string toString() const override;

  static IndexLookupConditionPtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void validate() const;
};

using BetweenIndexLookupConditionPtr =
    std::shared_ptr<BetweenIndexLookupCondition>;

/// Represents EQUAL index lookup condition: 'key' = 'value'. 'value' must be a
/// constant value with the same type as 'key'.
struct EqualIndexLookupCondition : public IndexLookupCondition {
  /// The value to compare against.
  TypedExprPtr value;

  EqualIndexLookupCondition(FieldAccessTypedExprPtr _key, TypedExprPtr _value)
      : IndexLookupCondition(std::move(_key)), value(std::move(_value)) {
    validate();
  }

  bool isFilter() const override;

  folly::dynamic serialize() const override;

  std::string toString() const override;

  static IndexLookupConditionPtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void validate() const;
};

using EqualIndexLookupConditionPtr = std::shared_ptr<EqualIndexLookupCondition>;

/// Represents index lookup join. Translates to an exec::IndexLookupJoin
/// operator. Assumes the right input is a table scan source node that provides
/// indexed table lookup for the left input with the specified join keys and
/// conditions. The join keys must be a prefix of the index columns of the
/// lookup table. Each join condition must use columns from both sides. For the
/// right side, it can only use one index column. Each index column can either
/// be a join key or a join condition once. The table scan node of the right
/// input is translated to a connector::IndexSource within
/// exec::IndexLookupJoin. Only INNER and LEFT joins are supported.
///
/// Take the following query for example, 't' is left table, 'u' is the right
/// table with indexed columns. 'sid' is the join keys. 'u.event_type in
/// t.event_list' is the join condition.
///
/// SELECT t.sid, t.day_ts, u.event_type
/// FROM t LEFT JOIN u
/// ON t.sid = u.sid
///  AND contains(t.event_list, u.event_type)
///  AND t.ds BETWEEN '2024-01-01' AND '2024-01-07'
///
/// Here,
/// - 'joinType' is JoinType::kLeft
/// - 'left' describes scan of t with a filter on 'ds':t.ds BETWEEN '2024-01-01'
///    AND '2024-01-07'
/// - 'right' describes indexed table 'u' with index keys sid, event_type(and
///    maybe some more)
/// - 'leftKeys' is a list of one key 't.sid'
/// - 'rightKeys' is a list of one key 'u.sid'
/// - 'joinConditions' specifies one condition: contains(t.event_list,
///   u.event_type)
/// - 'outputType' contains 3 columns : t.sid, t.day_ts, u.event_type
///
class IndexLookupJoinNode : public AbstractJoinNode {
 public:
  /// @param joinType Specifies the lookup join type. Only INNER and LEFT joins
  /// are supported.
  /// @param leftKeys Left side join keys used for index lookup.
  /// @param rightKeys Right side join keys that form the index prefix.
  /// @param joinConditions Additional conditions for index lookup that can't
  /// be converted into simple equality join conditions. These conditions use
  /// columns from both left and right  and exactly one index column from
  /// the right side.sides
  /// @param filter Additional filter to apply on join results. This supports
  /// filters that can't be converted into join conditions.
  /// @param hasMarker if true, the output type includes a boolean
  /// column at the end to indicate if a join output row has a match or not.
  /// This only applies for left join.
  IndexLookupJoinNode(
      const PlanNodeId& id,
      JoinType joinType,
      const std::vector<FieldAccessTypedExprPtr>& leftKeys,
      const std::vector<FieldAccessTypedExprPtr>& rightKeys,
      const std::vector<IndexLookupConditionPtr>& joinConditions,
      TypedExprPtr filter,
      bool hasMarker,
      PlanNodePtr left,
      TableScanNodePtr right,
      RowTypePtr outputType);

  class Builder
      : public AbstractJoinNode::Builder<IndexLookupJoinNode, Builder> {
   public:
    Builder() = default;

    explicit Builder(const IndexLookupJoinNode& other)
        : AbstractJoinNode::Builder<IndexLookupJoinNode, Builder>(other) {
      joinConditions_ = other.joinConditions();
      filter_ = other.filter();
      hasMarker_ = other.hasMarker();
    }

    /// Set lookup conditions for index lookup that can't be converted into
    /// simple equality join conditions.
    Builder& joinConditions(
        std::vector<IndexLookupConditionPtr> joinConditions) {
      joinConditions_ = std::move(joinConditions);
      return *this;
    }

    /// Set additional filter to apply on join results.
    Builder& filter(TypedExprPtr filter) {
      filter_ = std::move(filter);
      return *this;
    }

    /// Set whether to include a marker column for left joins.
    Builder& hasMarker(bool hasMarker) {
      hasMarker_ = hasMarker;
      return *this;
    }

    std::shared_ptr<IndexLookupJoinNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "IndexLookupJoinNode id is not set");
      VELOX_USER_CHECK(
          joinType_.has_value(), "IndexLookupJoinNode joinType is not set");
      VELOX_USER_CHECK(
          leftKeys_.has_value(), "IndexLookupJoinNode leftKeys is not set");
      VELOX_USER_CHECK(
          rightKeys_.has_value(), "IndexLookupJoinNode rightKeys is not set");
      VELOX_USER_CHECK(
          left_.has_value(), "IndexLookupJoinNode left source is not set");
      VELOX_USER_CHECK(
          right_.has_value(), "IndexLookupJoinNode right source is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "IndexLookupJoinNode outputType is not set");

      return std::make_shared<IndexLookupJoinNode>(
          id_.value(),
          joinType_.value(),
          leftKeys_.value(),
          rightKeys_.value(),
          joinConditions_,
          filter_.value_or(nullptr),
          hasMarker_,
          left_.value(),
          std::dynamic_pointer_cast<const TableScanNode>(right_.value()),
          outputType_.value());
    }

   private:
    std::vector<IndexLookupConditionPtr> joinConditions_;
    bool hasMarker_;
  };

  bool supportsBarrier() const override {
    return true;
  }

  const TableScanNodePtr& lookupSource() const {
    return lookupSourceNode_;
  }

  /// Returns the join conditions for index lookup that can't be converted into
  /// simple equality join conditions.
  const std::vector<IndexLookupConditionPtr>& joinConditions() const {
    return joinConditions_;
  }

  std::string_view name() const override {
    return "IndexLookupJoin";
  }

  /// Returns whether this node includes a marker column for left joins.
  bool hasMarker() const {
    return hasMarker_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

  /// Returns true if the lookup join supports this join type, otherwise false.
  static bool isSupported(JoinType joinType);

 private:
  void addDetails(std::stringstream& stream) const override;

  /// The table scan node that provides the lookup source for index operations.
  const TableScanNodePtr lookupSourceNode_;

  /// Join conditions that can't be converted into simple equality join
  /// conditions. These conditions involve columns from both left and right
  /// sides and exactly one index column from the right side.
  const std::vector<IndexLookupConditionPtr> joinConditions_;

  /// Whether to include a marker column for left joins to indicate matches.
  const bool hasMarker_;
};

using IndexLookupJoinNodePtr = std::shared_ptr<const IndexLookupJoinNode>;

/// Returns true if 'planNode' is index lookup join node.
bool isIndexLookupJoin(const core::PlanNode* planNode);

/// Represents inner/outer nested loop joins. Translates to an
/// exec::NestedLoopJoinProbe and exec::NestedLoopJoinBuild. A separate
/// pipeline is produced for the build side when generating exec::Operators.
///
/// Nested loop join (NLJ) supports both equal and non-equal joins.
/// Expressions specified in joinCondition are evaluated on every combination
/// of left/right tuple, to emit result. Results are emitted following the
/// same input order of probe rows for inner and left joins, for each thread
/// of execution.
///
/// To create Cartesian product of the left/right's output, use the
/// constructor without `joinType` and `joinCondition` parameter.
class NestedLoopJoinNode : public PlanNode {
 public:
  NestedLoopJoinNode(
      const PlanNodeId& id,
      JoinType joinType,
      TypedExprPtr joinCondition,
      PlanNodePtr left,
      PlanNodePtr right,
      RowTypePtr outputType);

  NestedLoopJoinNode(
      const PlanNodeId& id,
      PlanNodePtr left,
      PlanNodePtr right,
      RowTypePtr outputType);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const NestedLoopJoinNode& other) {
      id_ = other.id();
      joinType_ = other.joinType();
      joinCondition_ = other.joinCondition();
      VELOX_CHECK_EQ(other.sources().size(), 2);
      left_ = other.sources()[0];
      right_ = other.sources()[1];
      outputType_ = other.outputType();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& joinType(JoinType joinType) {
      joinType_ = joinType;
      return *this;
    }

    Builder& joinCondition(TypedExprPtr joinCondition) {
      joinCondition_ = std::move(joinCondition);
      return *this;
    }

    Builder& left(PlanNodePtr left) {
      left_ = std::move(left);
      return *this;
    }

    Builder& right(PlanNodePtr right) {
      right_ = std::move(right);
      return *this;
    }

    Builder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    std::shared_ptr<NestedLoopJoinNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "NestedLoopJoinNode id is not set");
      VELOX_USER_CHECK(
          left_.has_value(), "NestedLoopJoinNode left source is not set");
      VELOX_USER_CHECK(
          right_.has_value(), "NestedLoopJoinNode right source is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "NestedLoopJoinNode outputType is not set");

      return std::make_shared<NestedLoopJoinNode>(
          id_.value(),
          joinType_,
          joinCondition_,
          left_.value(),
          right_.value(),
          outputType_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    JoinType joinType_ = kDefaultJoinType;
    TypedExprPtr joinCondition_ = kDefaultJoinCondition;
    std::optional<PlanNodePtr> left_;
    std::optional<PlanNodePtr> right_;
    std::optional<RowTypePtr> outputType_;
  };

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  std::string_view name() const override {
    return "NestedLoopJoin";
  }

  const TypedExprPtr& joinCondition() const {
    return joinCondition_;
  }

  JoinType joinType() const {
    return joinType_;
  }

  folly::dynamic serialize() const override;

  /// If nested loop join supports this join type.
  static bool isSupported(JoinType joinType);

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  static const JoinType kDefaultJoinType;
  static const TypedExprPtr kDefaultJoinCondition;

  void addDetails(std::stringstream& stream) const override;

  const JoinType joinType_;
  const TypedExprPtr joinCondition_;
  const std::vector<PlanNodePtr> sources_;
  const RowTypePtr outputType_;
};

using NestedLoopJoinNodePtr = std::shared_ptr<const NestedLoopJoinNode>;

// Represents the 'SortBy' node in the plan.
class OrderByNode : public PlanNode {
 public:
  OrderByNode(
      const PlanNodeId& id,
      const std::vector<FieldAccessTypedExprPtr>& sortingKeys,
      const std::vector<SortOrder>& sortingOrders,
      bool isPartial,
      const PlanNodePtr& source)
      : PlanNode(id),
        sortingKeys_(sortingKeys),
        sortingOrders_(sortingOrders),
        isPartial_(isPartial),
        sources_{source} {
    VELOX_USER_CHECK(!sortingKeys.empty(), "OrderBy must specify sorting keys");
    VELOX_USER_CHECK_EQ(
        sortingKeys.size(),
        sortingOrders.size(),
        "Number of sorting keys and sorting orders in OrderBy must be the same");
    // Reject duplicate sorting keys.
    std::unordered_set<std::string> uniqueKeys;
    for (const auto& sortKey : sortingKeys) {
      VELOX_USER_CHECK_NOT_NULL(sortKey, "Sorting key cannot be null");
      VELOX_USER_CHECK(
          uniqueKeys.insert(sortKey->name()).second,
          "Duplicate sorting keys are not allowed: {}",
          sortKey->name());
    }
  }

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const OrderByNode& other) {
      id_ = other.id();
      sortingKeys_ = other.sortingKeys();
      sortingOrders_ = other.sortingOrders();
      isPartial_ = other.isPartial();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& sortingKeys(std::vector<FieldAccessTypedExprPtr> sortingKeys) {
      sortingKeys_ = std::move(sortingKeys);
      return *this;
    }

    Builder& sortingOrders(std::vector<SortOrder> sortingOrders) {
      sortingOrders_ = std::move(sortingOrders);
      return *this;
    }

    Builder& isPartial(bool isPartial) {
      isPartial_ = isPartial;
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<OrderByNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "OrderByNode id is not set");
      VELOX_USER_CHECK(
          sortingKeys_.has_value(), "OrderByNode sortingKeys is not set");
      VELOX_USER_CHECK(
          sortingOrders_.has_value(), "OrderByNode sortingOrders is not set");
      VELOX_USER_CHECK(
          isPartial_.has_value(), "OrderByNode isPartial is not set");
      VELOX_USER_CHECK(source_.has_value(), "OrderByNode source is not set");

      return std::make_shared<OrderByNode>(
          id_.value(),
          sortingKeys_.value(),
          sortingOrders_.value(),
          isPartial_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> sortingKeys_;
    std::optional<std::vector<SortOrder>> sortingOrders_;
    std::optional<bool> isPartial_;
    std::optional<PlanNodePtr> source_;
  };

  const std::vector<FieldAccessTypedExprPtr>& sortingKeys() const {
    return sortingKeys_;
  }

  const std::vector<SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  bool canSpill(const QueryConfig& queryConfig) const override {
    return queryConfig.orderBySpillEnabled();
  }

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  // True if this node only sorts a portion of the final result. If it is
  // true, a local merge or merge exchange is required to merge the sorted
  // runs.
  bool isPartial() const {
    return isPartial_;
  }

  std::string_view name() const override {
    return "OrderBy";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<FieldAccessTypedExprPtr> sortingKeys_;
  const std::vector<SortOrder> sortingOrders_;
  const bool isPartial_;
  const std::vector<PlanNodePtr> sources_;
};

using OrderByNodePtr = std::shared_ptr<const OrderByNode>;

/// Represents a spatial join between two geometries. Translates to an
/// exec::SpatialJoinProbe and exec::SpatialJoinBuild. A separate
/// pipeline is produced for the build side when generating exec::Operators.
///
/// Spatial join supports "local" spatial predicates, i.e. predicates that
/// require defined proximity.  Examples include ST_Intersects or any of
/// the DE-9IM predicates except for ST_Disjoint.  It also supports
/// `ST_Distance(g1, g2) <= d`.
///
/// The local join index is a collection of bounding boxes for a quick
/// check (either "no" or "maybe"), and the actual predicate must be
/// checked for each candidate.
///
/// Currently only INNER joins are supported, but LEFT joins are planned.
class SpatialJoinNode : public PlanNode {
 public:
  SpatialJoinNode(
      const PlanNodeId& id,
      JoinType joinType,
      TypedExprPtr joinCondition,
      PlanNodePtr left,
      PlanNodePtr right,
      RowTypePtr outputType);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const SpatialJoinNode& other) {
      id_ = other.id();
      joinType_ = other.joinType();
      joinCondition_ = other.joinCondition();
      VELOX_CHECK_EQ(other.sources().size(), 2);
      left_ = other.sources()[0];
      right_ = other.sources()[1];
      outputType_ = other.outputType();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& joinType(JoinType joinType) {
      joinType_ = joinType;
      return *this;
    }

    Builder& joinCondition(TypedExprPtr joinCondition) {
      joinCondition_ = std::move(joinCondition);
      return *this;
    }

    Builder& left(PlanNodePtr left) {
      left_ = std::move(left);
      return *this;
    }

    Builder& right(PlanNodePtr right) {
      right_ = std::move(right);
      return *this;
    }

    Builder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    std::shared_ptr<SpatialJoinNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "SpatialJoinNode id is not set");
      VELOX_USER_CHECK(
          left_.has_value(), "SpatialJoinNode left source is not set");
      VELOX_USER_CHECK(
          right_.has_value(), "SpatialJoinNode right source is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "SpatialJoinNode outputType is not set");

      return std::make_shared<SpatialJoinNode>(
          id_.value(),
          joinType_,
          joinCondition_,
          left_.value(),
          right_.value(),
          outputType_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    JoinType joinType_ = kDefaultJoinType;
    TypedExprPtr joinCondition_;
    std::optional<PlanNodePtr> left_;
    std::optional<PlanNodePtr> right_;
    std::optional<RowTypePtr> outputType_;
  };

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  std::string_view name() const override {
    return "SpatialJoin";
  }

  const TypedExprPtr& joinCondition() const {
    return joinCondition_;
  }

  JoinType joinType() const {
    return joinType_;
  }

  folly::dynamic serialize() const override;

  /// If spatial join supports this join type.
  static bool isSupported(JoinType joinType);

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  constexpr static JoinType kDefaultJoinType = JoinType::kInner;

  void addDetails(std::stringstream& stream) const override;

  const JoinType joinType_;
  const TypedExprPtr joinCondition_;
  const std::vector<PlanNodePtr> sources_;
  const RowTypePtr outputType_;
};

using SpatialJoinNodePtr = std::shared_ptr<const SpatialJoinNode>;

class TopNNode : public PlanNode {
 public:
  TopNNode(
      const PlanNodeId& id,
      const std::vector<FieldAccessTypedExprPtr>& sortingKeys,
      const std::vector<SortOrder>& sortingOrders,
      int32_t count,
      bool isPartial,
      const PlanNodePtr& source);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const TopNNode& other) {
      id_ = other.id();
      sortingKeys_ = other.sortingKeys();
      sortingOrders_ = other.sortingOrders();
      count_ = other.count();
      isPartial_ = other.isPartial();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& sortingKeys(std::vector<FieldAccessTypedExprPtr> sortingKeys) {
      sortingKeys_ = std::move(sortingKeys);
      return *this;
    }

    Builder& sortingOrders(std::vector<SortOrder> sortingOrders) {
      sortingOrders_ = std::move(sortingOrders);
      return *this;
    }

    Builder& count(int32_t count) {
      count_ = count;
      return *this;
    }

    Builder& isPartial(bool isPartial) {
      isPartial_ = isPartial;
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<TopNNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "TopNNode id is not set");
      VELOX_USER_CHECK(
          sortingKeys_.has_value(), "TopNNode sortingKeys is not set");
      VELOX_USER_CHECK(
          sortingOrders_.has_value(), "TopNNode sortingOrders is not set");
      VELOX_USER_CHECK(count_.has_value(), "TopNNode count is not set");
      VELOX_USER_CHECK(isPartial_.has_value(), "TopNNode isPartial is not set");
      VELOX_USER_CHECK(source_.has_value(), "TopNNode source is not set");

      return std::make_shared<TopNNode>(
          id_.value(),
          sortingKeys_.value(),
          sortingOrders_.value(),
          count_.value(),
          isPartial_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> sortingKeys_;
    std::optional<std::vector<SortOrder>> sortingOrders_;
    std::optional<int32_t> count_;
    std::optional<bool> isPartial_;
    std::optional<PlanNodePtr> source_;
  };

  const std::vector<FieldAccessTypedExprPtr>& sortingKeys() const {
    return sortingKeys_;
  }

  const std::vector<SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  int32_t count() const {
    return count_;
  }

  bool isPartial() const {
    return isPartial_;
  }

  std::string_view name() const override {
    return "TopN";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<FieldAccessTypedExprPtr> sortingKeys_;
  const std::vector<SortOrder> sortingOrders_;
  const int32_t count_;
  const bool isPartial_;
  const std::vector<PlanNodePtr> sources_;
};

using TopNNodePtr = std::shared_ptr<const TopNNode>;

class LimitNode : public PlanNode {
 public:
  // @param isPartial Boolean indicating whether Limit node generates partial
  // results on local workers or finalizes the partial results from `PARTIAL`
  // nodes.
  LimitNode(
      const PlanNodeId& id,
      int64_t offset,
      int64_t count,
      bool isPartial,
      const PlanNodePtr& source)
      : PlanNode(id),
        offset_(offset),
        count_(count),
        isPartial_(isPartial),
        sources_{source} {
    VELOX_CHECK_GT(
        count,
        0,
        "Limit must specify greater than zero number of rows to keep");
  }

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const LimitNode& other) {
      id_ = other.id();
      offset_ = other.offset();
      count_ = other.count();
      isPartial_ = other.isPartial();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& offset(int64_t offset) {
      offset_ = offset;
      return *this;
    }

    Builder& count(int64_t count) {
      count_ = count;
      return *this;
    }

    Builder& isPartial(bool isPartial) {
      isPartial_ = isPartial;
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<LimitNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "LimitNode id is not set");
      VELOX_USER_CHECK(offset_.has_value(), "LimitNode offset is not set");
      VELOX_USER_CHECK(count_.has_value(), "LimitNode count is not set");
      VELOX_USER_CHECK(
          isPartial_.has_value(), "LimitNode isPartial is not set");
      VELOX_USER_CHECK(source_.has_value(), "LimitNode source is not set");

      return std::make_shared<LimitNode>(
          id_.value(),
          offset_.value(),
          count_.value(),
          isPartial_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<int64_t> offset_;
    std::optional<int64_t> count_;
    std::optional<bool> isPartial_;
    std::optional<PlanNodePtr> source_;
  };

  bool supportsBarrier() const override {
    return true;
  }

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  int64_t offset() const {
    return offset_;
  }

  int64_t count() const {
    return count_;
  }

  bool isPartial() const {
    return isPartial_;
  }

  std::string_view name() const override {
    return "Limit";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const int64_t offset_;
  const int64_t count_;
  const bool isPartial_;
  const std::vector<PlanNodePtr> sources_;
};

using LimitNodePtr = std::shared_ptr<const LimitNode>;

/// Expands arrays and maps into separate columns. Arrays are expanded into a
/// single column, and maps are expanded into two columns (key, value). Can be
/// used to expand multiple columns. In this case will produce as many rows as
/// the highest cardinality array or map (the other columns are padded with
/// nulls). Optionally can produce an ordinality column that specifies the row
/// number starting with 1.
class UnnestNode : public PlanNode {
 public:
  /// @param replicateVariables Inputs that are projected as is
  /// @param unnestVariables Inputs that are unnested. Must be of type ARRAY
  /// or MAP.
  /// @param unnestNames Names to use for unnested outputs: one name for each
  /// array (element); two names for each map (key and value). The output
  /// names must appear in the same order as unnestVariables.
  /// @param ordinalityName Optional name for the ordinality columns. If not
  /// present, ordinality column is not produced.
  /// @param markerName Optional name for column which indicates whether an
  /// output row has non-empty unnested value. If not present, marker column is
  /// not provided and the unnest operator also skips producing output rows
  /// with empty unnest value.
  UnnestNode(
      const PlanNodeId& id,
      std::vector<FieldAccessTypedExprPtr> replicateVariables,
      std::vector<FieldAccessTypedExprPtr> unnestVariables,
      std::vector<std::string> unnestNames,
      std::optional<std::string> ordinalityName,
      std::optional<std::string> markerName,
      const PlanNodePtr& source);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const UnnestNode& other) {
      id_ = other.id();
      replicateVariables_ = other.replicateVariables();
      unnestVariables_ = other.unnestVariables();
      unnestNames_ = other.unnestNames_;
      ordinalityName_ = other.ordinalityName_;
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& replicateVariables(
        std::vector<FieldAccessTypedExprPtr> replicateVariables) {
      replicateVariables_ = std::move(replicateVariables);
      return *this;
    }

    Builder& unnestVariables(
        std::vector<FieldAccessTypedExprPtr> unnestVariables) {
      unnestVariables_ = std::move(unnestVariables);
      return *this;
    }

    Builder& unnestNames(std::vector<std::string> unnestNames) {
      unnestNames_ = std::move(unnestNames);
      return *this;
    }

    Builder& ordinalityName(std::optional<std::string> ordinalityName) {
      ordinalityName_ = std::move(ordinalityName);
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    Builder& markerName(std::optional<std::string> markerName) {
      markerName_ = std::move(markerName);
      return *this;
    }

    std::shared_ptr<UnnestNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "UnnestNode id is not set");
      VELOX_USER_CHECK(
          replicateVariables_.has_value(),
          "UnnestNode replicateVariables is not set");
      VELOX_USER_CHECK(
          unnestVariables_.has_value(),
          "UnnestNode unnestVariables is not set");
      VELOX_USER_CHECK(
          unnestNames_.has_value(), "UnnestNode unnestNames is not set");
      VELOX_USER_CHECK(source_.has_value(), "UnnestNode source is not set");

      return std::make_shared<UnnestNode>(
          id_.value(),
          replicateVariables_.value(),
          unnestVariables_.value(),
          unnestNames_.value(),
          ordinalityName_,
          markerName_,
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> replicateVariables_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> unnestVariables_;
    std::optional<std::vector<std::string>> unnestNames_;
    std::optional<std::string> ordinalityName_;
    std::optional<std::string> markerName_;
    std::optional<PlanNodePtr> source_;
  };

  bool supportsBarrier() const override {
    return true;
  }

  /// The order of columns in the output is: replicated columns (in the order
  /// specified), unnested columns (in the order specified, for maps: key
  /// comes before value), optional ordinality column.
  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const std::vector<FieldAccessTypedExprPtr>& replicateVariables() const {
    return replicateVariables_;
  }

  const std::vector<FieldAccessTypedExprPtr>& unnestVariables() const {
    return unnestVariables_;
  }

  const std::vector<std::string>& unnestNames() const {
    return unnestNames_;
  }

  const std::optional<std::string>& ordinalityName() const {
    return ordinalityName_;
  }

  bool hasOrdinality() const {
    return ordinalityName_.has_value();
  }

  const std::optional<std::string>& markerName() const {
    return markerName_;
  }

  bool hasMarker() const {
    return markerName_.has_value();
  }

  std::string_view name() const override {
    return "Unnest";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<FieldAccessTypedExprPtr> replicateVariables_;
  const std::vector<FieldAccessTypedExprPtr> unnestVariables_;
  const std::vector<std::string> unnestNames_;
  const std::optional<std::string> ordinalityName_;
  const std::optional<std::string> markerName_;
  const std::vector<PlanNodePtr> sources_;
  RowTypePtr outputType_;
};

using UnnestNodePtr = std::shared_ptr<const UnnestNode>;

/// Checks that input contains at most one row. Return that row as is. If
/// input is empty, returns a single row with all values set to null. If input
/// contains more than one row raises an exception.
///
/// This plan node is used in query plans that use non-correlated sub-queries.
class EnforceSingleRowNode : public PlanNode {
 public:
  EnforceSingleRowNode(const PlanNodeId& id, PlanNodePtr source)
      : PlanNode(id), sources_{std::move(source)} {}

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const EnforceSingleRowNode& other) {
      id_ = other.id();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<EnforceSingleRowNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "EnforceSingleRowNode id is not set");
      VELOX_USER_CHECK(
          source_.has_value(), "EnforceSingleRowNode source is not set");

      return std::make_shared<EnforceSingleRowNode>(
          id_.value(), source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<PlanNodePtr> source_;
  };

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  std::string_view name() const override {
    return "EnforceSingleRow";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<PlanNodePtr> sources_;
};

using EnforceSingleRowNodePtr = std::shared_ptr<const EnforceSingleRowNode>;

/// Adds a new column named `idName` at the end of the input columns
/// with unique int64_t value per input row.
///
/// 64-bit unique id is built in following way:
///  - first 24 bits - task unique id
///  - next 40 bits - operator counter value
///
/// The task unique id is added to ensure the generated id is unique
/// across all the nodes executing the same query stage in a distributed
/// query execution.
class AssignUniqueIdNode : public PlanNode {
 public:
  AssignUniqueIdNode(
      const PlanNodeId& id,
      const std::string& idName,
      const int32_t taskUniqueId,
      PlanNodePtr source);

  bool supportsBarrier() const override {
    return true;
  }

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const AssignUniqueIdNode& other) {
      id_ = other.id();
      idName_ = other.outputType()->names().back();
      taskUniqueId_ = other.taskUniqueId();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& idName(std::string idName) {
      idName_ = std::move(idName);
      return *this;
    }

    Builder& taskUniqueId(int32_t taskUniqueId) {
      taskUniqueId_ = taskUniqueId;
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<AssignUniqueIdNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "AssignUniqueIdNode id not set");
      VELOX_USER_CHECK(
          idName_.has_value(), "AssignUniqueIdNode idName not set");
      VELOX_USER_CHECK(
          taskUniqueId_.has_value(), "AssignUniqueIdNode taskUniqueId not set");
      VELOX_USER_CHECK(
          source_.has_value(), "AssignUniqueIdNode source is not set");

      return std::make_shared<AssignUniqueIdNode>(
          id_.value(), idName_.value(), taskUniqueId_.value(), source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::string> idName_;
    std::optional<int32_t> taskUniqueId_;
    std::optional<PlanNodePtr> source_;
  };

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  std::string_view name() const override {
    return "AssignUniqueId";
  }

  int32_t taskUniqueId() const {
    return taskUniqueId_;
  }

  const std::shared_ptr<std::atomic_int64_t>& uniqueIdCounter() const {
    return uniqueIdCounter_;
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const int32_t taskUniqueId_;
  const std::vector<PlanNodePtr> sources_;
  RowTypePtr outputType_;
  std::shared_ptr<std::atomic_int64_t> uniqueIdCounter_;
};

using AssignUniqueIdNodePtr = std::shared_ptr<const AssignUniqueIdNode>;

/// PlanNode used for evaluating Sql window functions.
/// All window functions evaluated in the operator have the same
/// window spec (partition keys + order columns).
/// If no partition keys are specified, then all input rows
/// are considered to be in a single partition.
/// If no order by columns are specified, then the input rows order
/// is non-deterministic.
/// Each window function also has a frame which specifies the sliding
/// window over which it is computed. The frame
/// could be RANGE (based on peers which are all rows with the same
/// ORDER BY value) or ROWS (position based).
/// The frame bound types are CURRENT_ROW, (expression or UNBOUNDED)
/// ROWS_PRECEDING and (expression or UNBOUNDED) ROWS_FOLLOWING.
/// The WindowNode has one passthrough output column for each input
/// column followed by the results of the window functions.
class WindowNode : public PlanNode {
 public:
  enum class WindowType { kRange, kRows };

  VELOX_DECLARE_EMBEDDED_ENUM_NAME(WindowType)

  enum class BoundType {
    kUnboundedPreceding,
    kPreceding,
    kCurrentRow,
    kFollowing,
    kUnboundedFollowing
  };

  VELOX_DECLARE_EMBEDDED_ENUM_NAME(BoundType)

  /// Window frames can be ROW or RANGE type.
  /// Frame bounds can be CURRENT ROW, UNBOUNDED PRECEDING(FOLLOWING)
  /// and k PRECEDING(FOLLOWING). K could be a constant or column.
  ///
  /// k has to be of integer or bigint type.
  struct Frame {
    WindowType type;
    BoundType startType;
    TypedExprPtr startValue;
    BoundType endType;
    TypedExprPtr endValue;

    folly::dynamic serialize() const;

    static Frame deserialize(const folly::dynamic& obj);
  };

  struct Function {
    CallTypedExprPtr functionCall;
    Frame frame;
    bool ignoreNulls;

    folly::dynamic serialize() const;

    static Function deserialize(const folly::dynamic& obj);
  };

  /// @param windowColumnNames specifies the output column
  /// names for each window function column. So
  /// windowColumnNames.length() = windowFunctions.length().
  WindowNode(
      PlanNodeId id,
      std::vector<FieldAccessTypedExprPtr> partitionKeys,
      std::vector<FieldAccessTypedExprPtr> sortingKeys,
      std::vector<SortOrder> sortingOrders,
      std::vector<std::string> windowColumnNames,
      std::vector<Function> windowFunctions,
      bool inputsSorted,
      PlanNodePtr source);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const WindowNode& other) {
      id_ = other.id();
      partitionKeys_ = other.partitionKeys();
      sortingKeys_ = other.sortingKeys();
      sortingOrders_ = other.sortingOrders();
      windowColumnNames_ = other.windowColumnNames_;
      windowFunctions_ = other.windowFunctions();
      inputsSorted_ = other.inputsSorted();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& partitionKeys(std::vector<FieldAccessTypedExprPtr> partitionKeys) {
      partitionKeys_ = std::move(partitionKeys);
      return *this;
    }

    Builder& sortingKeys(std::vector<FieldAccessTypedExprPtr> sortingKeys) {
      sortingKeys_ = std::move(sortingKeys);
      return *this;
    }

    Builder& sortingOrders(std::vector<SortOrder> sortingOrders) {
      sortingOrders_ = std::move(sortingOrders);
      return *this;
    }

    Builder& windowColumnNames(std::vector<std::string> windowColumNames) {
      windowColumnNames_ = std::move(windowColumNames);
      return *this;
    }

    Builder& windowFunctions(
        std::vector<WindowNode::Function> windowFunctions) {
      windowFunctions_ = std::move(windowFunctions);
      return *this;
    }

    Builder& inputsSorted(bool inputsSorted) {
      inputsSorted_ = inputsSorted;
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<WindowNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "WindowNode id is not set");
      VELOX_USER_CHECK(
          partitionKeys_.has_value(), "WindowNode partitionKeys is not set");
      VELOX_USER_CHECK(
          sortingKeys_.has_value(), "WindowNode sortingKeys is not set");
      VELOX_USER_CHECK(
          sortingOrders_.has_value(), "WindowNode sortingOrders is not set");
      VELOX_USER_CHECK(
          windowColumnNames_.has_value(),
          "WindowNode windowColumnNames is not set");
      VELOX_USER_CHECK(
          windowFunctions_.has_value(),
          "WindowNode windowFunctions is not set");
      VELOX_USER_CHECK(
          inputsSorted_.has_value(), "WindowNode inputsSorted is not set");
      VELOX_USER_CHECK(source_.has_value(), "WindowNode source is not set");

      return std::make_shared<WindowNode>(
          id_.value(),
          partitionKeys_.value(),
          sortingKeys_.value(),
          sortingOrders_.value(),
          windowColumnNames_.value(),
          windowFunctions_.value(),
          inputsSorted_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> partitionKeys_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> sortingKeys_;
    std::optional<std::vector<SortOrder>> sortingOrders_;
    std::optional<std::vector<std::string>> windowColumnNames_;
    std::optional<std::vector<WindowNode::Function>> windowFunctions_;
    std::optional<bool> inputsSorted_;
    std::optional<PlanNodePtr> source_;
  };

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  /// The outputType is the concatenation of the input columns
  /// with the output columns of each window function.
  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  bool canSpill(const QueryConfig& queryConfig) const override {
    // No partitioning keys means the whole input is one big partition. In
    // this case, spilling is not helpful because we need to have a full
    // partition in memory to produce results.
    return !partitionKeys_.empty() && !inputsSorted_ &&
        queryConfig.windowSpillEnabled();
  }

  const RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  const std::vector<FieldAccessTypedExprPtr>& partitionKeys() const {
    return partitionKeys_;
  }

  const std::vector<FieldAccessTypedExprPtr>& sortingKeys() const {
    return sortingKeys_;
  }

  const std::vector<SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  const std::vector<Function>& windowFunctions() const {
    return windowFunctions_;
  }

  bool inputsSorted() const {
    return inputsSorted_;
  }

  std::string_view name() const override {
    return "Window";
  }

  const std::vector<std::string>& windowColumnNames() const {
    return windowColumnNames_;
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<FieldAccessTypedExprPtr> partitionKeys_;

  const std::vector<FieldAccessTypedExprPtr> sortingKeys_;
  const std::vector<SortOrder> sortingOrders_;

  const std::vector<std::string> windowColumnNames_;
  const std::vector<Function> windowFunctions_;

  const bool inputsSorted_;

  const std::vector<PlanNodePtr> sources_;

  const RowTypePtr outputType_;
};

using WindowNodePtr = std::shared_ptr<const WindowNode>;

/// Optimized version of a WindowNode for a single row_number function with an
/// optional limit and no sorting.
/// The output of this node contains all input columns followed by an optional
/// 'rowNumberColumnName' BIGINT column.
class RowNumberNode : public PlanNode {
 public:
  /// @param partitionKeys Partitioning keys. May be empty.
  /// @param rowNumberColumnName Optional name of the column containing row
  /// numbers. If not specified, the output doesn't include 'row number'
  /// column. This is used when computing partial results.
  /// @param limit Optional per-partition limit. If specified, the number of
  /// rows produced by this node will not exceed this value for any given
  /// partition. Extra rows will be dropped.
  RowNumberNode(
      PlanNodeId id,
      std::vector<FieldAccessTypedExprPtr> partitionKeys,
      const std::optional<std::string>& rowNumberColumnName,
      std::optional<int32_t> limit,
      PlanNodePtr source);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const RowNumberNode& other) {
      id_ = other.id();
      partitionKeys_ = other.partitionKeys();
      rowNumberColumnName_ = other.generateRowNumber()
          ? std::make_optional(other.outputType()->names().back())
          : std::nullopt;
      limit_ = other.limit();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& partitionKeys(std::vector<FieldAccessTypedExprPtr> partitionKeys) {
      partitionKeys_ = std::move(partitionKeys);
      return *this;
    }

    Builder& rowNumberColumnName(
        std::optional<std::string> rowNumberColumnName) {
      rowNumberColumnName_ = std::move(rowNumberColumnName);
      return *this;
    }

    Builder& limit(std::optional<int32_t> limit) {
      limit_ = limit;
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<RowNumberNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "RowNumberNode id is not set");
      VELOX_USER_CHECK(
          partitionKeys_.has_value(), "RowNumberNode partitionKeys is not set");
      VELOX_USER_CHECK(
          rowNumberColumnName_.has_value(),
          "RowNumberNode rowNumberColumnName is not set");
      VELOX_USER_CHECK(limit_.has_value(), "RowNumberNode limit is not set");
      VELOX_USER_CHECK(source_.has_value(), "RowNumberNode source is not set");

      return std::make_shared<RowNumberNode>(
          id_.value(),
          partitionKeys_.value(),
          rowNumberColumnName_.value(),
          limit_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> partitionKeys_;
    std::optional<std::optional<std::string>> rowNumberColumnName_;
    std::optional<std::optional<int32_t>> limit_;
    std::optional<PlanNodePtr> source_;
  };

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  bool canSpill(const QueryConfig& queryConfig) const override {
    return !partitionKeys_.empty() && queryConfig.rowNumberSpillEnabled();
  }

  const std::vector<FieldAccessTypedExprPtr>& partitionKeys() const {
    return partitionKeys_;
  }

  std::optional<int32_t> limit() const {
    return limit_;
  }

  bool generateRowNumber() const {
    return outputType_->size() > sources_[0]->outputType()->size();
  }

  std::string_view name() const override {
    return "RowNumber";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<FieldAccessTypedExprPtr> partitionKeys_;

  const std::optional<int32_t> limit_;

  const std::vector<PlanNodePtr> sources_;

  const RowTypePtr outputType_;
};

using RowNumberNodePtr = std::shared_ptr<const RowNumberNode>;

/// The MarkDistinct operator marks unique rows based on distinctKeys.
/// The result is put in a new markerName column alongside the original input.
/// @param markerName Name of the output mask channel.
/// @param distinctKeys Names of grouping keys.
/// column.
class MarkDistinctNode : public PlanNode {
 public:
  MarkDistinctNode(
      PlanNodeId id,
      std::string markerName,
      std::vector<FieldAccessTypedExprPtr> distinctKeys,
      PlanNodePtr source);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const MarkDistinctNode& other) {
      id_ = other.id();
      markerName_ = other.markerName();
      distinctKeys_ = other.distinctKeys();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& markerName(std::string markerName) {
      markerName_ = std::move(markerName);
      return *this;
    }

    Builder& distinctKeys(std::vector<FieldAccessTypedExprPtr> distinctKeys) {
      distinctKeys_ = std::move(distinctKeys);
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<MarkDistinctNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "MarkDistinctNode id is not set");
      VELOX_USER_CHECK(
          markerName_.has_value(), "MarkDistinctNode markerName is not set");
      VELOX_USER_CHECK(
          distinctKeys_.has_value(),
          "MarkDistinctNode distinctKeys is not set");
      VELOX_USER_CHECK(
          source_.has_value(), "MarkDistinctNode source is not set");

      return std::make_shared<MarkDistinctNode>(
          id_.value(),
          markerName_.value(),
          distinctKeys_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<std::string> markerName_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> distinctKeys_;
    std::optional<PlanNodePtr> source_;
  };

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  /// The outputType is the concatenation of the input columns and mask
  /// column.
  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  std::string_view name() const override {
    return "MarkDistinct";
  }

  const std::string& markerName() const {
    return markerName_;
  }

  const std::vector<FieldAccessTypedExprPtr>& distinctKeys() const {
    return distinctKeys_;
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::string markerName_;

  const std::vector<FieldAccessTypedExprPtr> distinctKeys_;

  const std::vector<PlanNodePtr> sources_;

  const RowTypePtr outputType_;
};

using MarkDistinctNodePtr = std::shared_ptr<const MarkDistinctNode>;

/// Optimized version of a WindowNode for a single row_number, rank or
/// dense_rank function with a limit over sorted partitions. The output of this
/// node contains all input columns followed by an optional
/// 'rowNumberColumnName' BIGINT column.
/// TODO: This node will be renamed to TopNRank or TopNRowNode once all the
/// support for handling rank and dense_rank is committed to Velox.
class TopNRowNumberNode : public PlanNode {
 public:
  enum class RankFunction {
    kRowNumber,
    kRank,
    kDenseRank,
  };

  static const char* rankFunctionName(RankFunction function);

  static RankFunction rankFunctionFromName(std::string_view name);

  /// @param rankFunction RanksFunction (row_number, rank, dense_rank) for TopN.
  /// @param partitionKeys Partitioning keys. May be empty.
  /// @param sortingKeys Sorting keys. May not be empty and may not intersect
  /// with 'partitionKeys'.
  /// @param sortingOrders Sorting orders, one per sorting key.
  /// @param rowNumberColumnName Optional name of the column containing row
  /// numbers or rank or dense_rank. If not specified, the output doesn't
  /// include 'row_number' column. This is used when computing partial results.
  /// @param limit Per-partition limit. The number of
  /// rows produced by this node will not exceed this value for any given
  /// partition. Extra rows will be dropped.
  TopNRowNumberNode(
      PlanNodeId id,
      RankFunction function,
      std::vector<FieldAccessTypedExprPtr> partitionKeys,
      std::vector<FieldAccessTypedExprPtr> sortingKeys,
      std::vector<SortOrder> sortingOrders,
      const std::optional<std::string>& rowNumberColumnName,
      int32_t limit,
      PlanNodePtr source);

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const TopNRowNumberNode& other) {
      id_ = other.id();
      partitionKeys_ = other.partitionKeys();
      sortingKeys_ = other.sortingKeys();
      sortingOrders_ = other.sortingOrders();
      rowNumberColumnName_ = other.generateRowNumber()
          ? std::make_optional(other.outputType()->names().back())
          : std::nullopt;
      limit_ = other.limit();
      VELOX_CHECK_EQ(other.sources().size(), 1);
      source_ = other.sources()[0];
      function_ = other.rankFunction();
    }

    Builder& id(PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& function(RankFunction function) {
      function_ = function;
      return *this;
    }

    Builder& partitionKeys(std::vector<FieldAccessTypedExprPtr> partitionKeys) {
      partitionKeys_ = std::move(partitionKeys);
      return *this;
    }

    Builder& sortingKeys(std::vector<FieldAccessTypedExprPtr> sortingKeys) {
      sortingKeys_ = std::move(sortingKeys);
      return *this;
    }

    Builder& sortingOrders(std::vector<SortOrder> sortingOrders) {
      sortingOrders_ = std::move(sortingOrders);
      return *this;
    }

    Builder& rowNumberColumnName(
        std::optional<std::string> rowNumberColumNName) {
      rowNumberColumnName_ = std::move(rowNumberColumNName);
      return *this;
    }

    Builder& limit(int32_t limit) {
      limit_ = limit;
      return *this;
    }

    Builder& source(PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<TopNRowNumberNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "TopNRowNumberNode id is not set");
      VELOX_USER_CHECK(
          partitionKeys_.has_value(),
          "TopNRowNumberNode partitionKeys is not set");
      VELOX_USER_CHECK(
          sortingKeys_.has_value(), "TopNRowNumberNode sortingKeys is not set");
      VELOX_USER_CHECK(
          sortingOrders_.has_value(),
          "TopNRowNumberNode sortingOrders is not set");
      VELOX_USER_CHECK(
          rowNumberColumnName_.has_value(),
          "TopNRowNumberNode rowNumberColumnName is not set");
      VELOX_USER_CHECK(
          limit_.has_value(), "TopNRowNumberNode limit is not set");
      VELOX_USER_CHECK(
          source_.has_value(), "TopNRowNumberNode source is not set");

      return std::make_shared<TopNRowNumberNode>(
          id_.value(),
          function_.has_value() ? function_.value() : RankFunction::kRowNumber,
          partitionKeys_.value(),
          sortingKeys_.value(),
          sortingOrders_.value(),
          rowNumberColumnName_.value(),
          limit_.value(),
          source_.value());
    }

   private:
    std::optional<PlanNodeId> id_;
    std::optional<RankFunction> function_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> partitionKeys_;
    std::optional<std::vector<FieldAccessTypedExprPtr>> sortingKeys_;
    std::optional<std::vector<SortOrder>> sortingOrders_;
    std::optional<std::optional<std::string>> rowNumberColumnName_;
    std::optional<int32_t> limit_;
    std::optional<PlanNodePtr> source_;
  };

  const std::vector<PlanNodePtr>& sources() const override {
    return sources_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  bool canSpill(const QueryConfig& queryConfig) const override {
    return !partitionKeys_.empty() && queryConfig.topNRowNumberSpillEnabled();
  }

  const RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  const std::vector<FieldAccessTypedExprPtr>& partitionKeys() const {
    return partitionKeys_;
  }

  const std::vector<FieldAccessTypedExprPtr>& sortingKeys() const {
    return sortingKeys_;
  }

  const std::vector<SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  int32_t limit() const {
    return limit_;
  }

  RankFunction rankFunction() const {
    return function_;
  }

  bool generateRowNumber() const {
    return outputType_->size() > sources_[0]->outputType()->size();
  }

  std::string_view name() const override {
    return "TopNRowNumber";
  }

  folly::dynamic serialize() const override;

  static PlanNodePtr create(const folly::dynamic& obj, void* context);

  PlanNodePtr copyWithNewSources(
      std::vector<PlanNodePtr> newSources) const override;

 private:
  void addDetails(std::stringstream& stream) const override;

  const RankFunction function_;

  const std::vector<FieldAccessTypedExprPtr> partitionKeys_;

  const std::vector<FieldAccessTypedExprPtr> sortingKeys_;
  const std::vector<SortOrder> sortingOrders_;

  const int32_t limit_;

  const std::vector<PlanNodePtr> sources_;

  const RowTypePtr outputType_;
};

using TopNRowNumberNodePtr = std::shared_ptr<const TopNRowNumberNode>;

class PlanNodeVisitorContext {
 public:
  virtual ~PlanNodeVisitorContext() = default;
};

class PlanNodeVisitor {
 public:
  virtual ~PlanNodeVisitor() = default;

  virtual void visit(const AggregationNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const ArrowStreamNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(
      const AssignUniqueIdNode& node,
      PlanNodeVisitorContext& ctx) const = 0;

  virtual void visit(
      const EnforceSingleRowNode& node,
      PlanNodeVisitorContext& ctx) const = 0;

  virtual void visit(const ExchangeNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const ExpandNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const FilterNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const GroupIdNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const HashJoinNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(
      const IndexLookupJoinNode& node,
      PlanNodeVisitorContext& ctx) const = 0;

  virtual void visit(const LimitNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const LocalMergeNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(
      const LocalPartitionNode& node,
      PlanNodeVisitorContext& ctx) const = 0;

  virtual void visit(const MarkDistinctNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const MergeExchangeNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const MergeJoinNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(
      const NestedLoopJoinNode& node,
      PlanNodeVisitorContext& ctx) const = 0;

  virtual void visit(const SpatialJoinNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const OrderByNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(
      const PartitionedOutputNode& node,
      PlanNodeVisitorContext& ctx) const = 0;

  virtual void visit(const ProjectNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(
      const ParallelProjectNode& node,
      PlanNodeVisitorContext& ctx) const = 0;

  virtual void visit(const RowNumberNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const TableScanNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const TableWriteNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(
      const TableWriteMergeNode& node,
      PlanNodeVisitorContext& ctx) const = 0;

  virtual void visit(const TopNNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const TopNRowNumberNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const TraceScanNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const UnnestNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const ValuesNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  virtual void visit(const WindowNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

  /// Used to visit custom PlanNodes that extend the set provided by Velox.
  virtual void visit(const PlanNode& node, PlanNodeVisitorContext& ctx)
      const = 0;

 protected:
  void visitSources(const PlanNode& node, PlanNodeVisitorContext& ctx) const {
    for (auto& source : node.sources()) {
      source->accept(*this, ctx);
    }
  }
};

} // namespace facebook::velox::core

template <>
struct fmt::formatter<facebook::velox::core::PartitionedOutputNode::Kind>
    : formatter<std::string_view> {
  auto format(
      facebook::velox::core::PartitionedOutputNode::Kind s,
      format_context& ctx) const {
    return formatter<std::string_view>::format(
        facebook::velox::core::PartitionedOutputNode::toName(s), ctx);
  }
};

template <>
struct fmt::formatter<facebook::velox::core::JoinType> : formatter<int> {
  auto format(facebook::velox::core::JoinType s, format_context& ctx) const {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};

template <>
struct fmt::formatter<facebook::velox::core::TopNRowNumberNode::RankFunction>
    : formatter<std::string> {
  auto format(
      facebook::velox::core::TopNRowNumberNode::RankFunction f,
      format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::velox::core::TopNRowNumberNode::rankFunctionName(f), ctx);
  }
};

template <>
struct fmt::formatter<facebook::velox::core::AggregationNode::Step>
    : formatter<std::string_view> {
  auto format(
      facebook::velox::core::AggregationNode::Step s,
      format_context& ctx) const {
    return formatter<std::string_view>::format(
        facebook::velox::core::AggregationNode::toName(s), ctx);
  }
};
